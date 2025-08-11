from django.core.management.base import BaseCommand
from django.core.cache import cache
from django.db import transaction
from website.models import (
    scenarios, 
    SMART_Forecast_Model, 
    CalcualtedReplenishmentModel,
    MasterDataPlantModel, 
    MasterDataProductModel,
    MasterDataOrderBook,
    MasterDataHistoryOfProductionModel,
    MasterDataEpicorSupplierMasterDataModel,
    MasterDataManuallyAssignProductionRequirement,
    MasterDataInventory,
    MasterDataSafetyStocks,
    MasterDataFreightModel,
    MasterDataCastToDespatchModel,
    MasterdataIncoTermsModel,
    MasterDataEpicorMethodOfManufacturingModel
)
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback

class Command(BaseCommand):
    help = "Fast replenishment calculation using pandas optimization"

    def add_arguments(self, parser):
        parser.add_argument('version', type=str, help='The scenario version to process')
        parser.add_argument('--product', type=str, help='Optional: Process only this specific product')

    def extract_location_from_forecast(self, location_series):
        """Extract location code from forecast location series using vectorized operations"""
        # Use pandas string operations for much faster processing
        return (location_series.str.split('-').str[1]
                .fillna(location_series.str.split('_').str[1])
                .fillna(location_series))

    def handle(self, *args, **kwargs):
        version = kwargs['version']
        product_filter = kwargs.get('product')
        
        if product_filter:
            self.stdout.write(f"Processing only product: {product_filter}")
        
        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario '{version}' not found"))
            return

        # Prevent concurrent execution
        cache_key = f"replenishment_fast_{version}_{product_filter or 'all'}"
        if cache.get(cache_key):
            self.stdout.write(self.style.ERROR("Another replenishment calculation is already running"))
            return
        
        cache.set(cache_key, True, timeout=3600)

        try:
            self.stdout.write(f"Starting FAST replenishment calculation for scenario: {version}")
            
            # Step 0: Clean up existing records
            with transaction.atomic():
                if product_filter:
                    existing_count = CalcualtedReplenishmentModel.objects.filter(
                        version=scenario, 
                        Product__Product=product_filter
                    ).count()
                    if existing_count > 0:
                        CalcualtedReplenishmentModel.objects.filter(
                            version=scenario, 
                            Product__Product=product_filter
                        ).delete()
                        self.stdout.write(f"Deleted {existing_count:,} existing records for product '{product_filter}'")
                else:
                    existing_count = CalcualtedReplenishmentModel.objects.filter(version=scenario).count()
                    if existing_count > 0:
                        CalcualtedReplenishmentModel.objects.filter(version=scenario).delete()
                        self.stdout.write(f"Deleted {existing_count:,} existing records")
            
            # Step 1: Load all data into pandas DataFrames
            self.load_data_to_pandas(scenario, product_filter)
            
            # Step 2: Process forecast with pandas operations
            self.process_forecast_pandas()
            
            # Step 3: Calculate replenishment using vectorized operations
            self.calculate_replenishment_pandas(scenario)
            
            self.stdout.write(self.style.SUCCESS("Fast replenishment calculation completed successfully"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error in fast replenishment calculation: {str(e)}"))
            self.stdout.write(self.style.ERROR(traceback.format_exc()))
        finally:
            cache.delete(cache_key)

    def load_data_to_pandas(self, scenario, product_filter=None):
        """Load all data into pandas DataFrames for fast processing"""
        self.stdout.write("Loading data into pandas DataFrames...")
        
        # Load casting products
        casting_keywords = ['cast', 'pour', 'mould', 'coulée', 'foundry', 'shakeout', 'core making', 'cool time', 'pouring']
        casting_products = set()
        
        for keyword in casting_keywords:
            products = MasterDataEpicorMethodOfManufacturingModel.objects.filter(
                OperationDesc__icontains=keyword
            ).values_list('ProductKey', flat=True)
            casting_products.update(products)
        
        if not casting_products:
            casting_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        
        # Load SMART forecast
        forecast_query = SMART_Forecast_Model.objects.filter(version=scenario)
        if product_filter:
            forecast_query = forecast_query.filter(Product=product_filter)
        
        forecast_values = forecast_query.filter(Product__in=casting_products).values(
            'Product', 'Location', 'Period_AU', 'Qty', 'Customer_code', 'Forecast_Region'
        )
        
        self.df_forecast = pd.DataFrame(list(forecast_values))
        if self.df_forecast.empty:
            self.stdout.write("No forecast data found")
            return
        
        self.stdout.write(f"Loaded {len(self.df_forecast):,} forecast records")
        
        # Extract locations using vectorized operations
        self.df_forecast['extracted_location'] = self.extract_location_from_forecast(self.df_forecast['Location'])
        
        # Load master data as DataFrames
        self.df_manual = pd.DataFrame(list(
            MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario)
            .values('Product__Product', 'ShippingDate', 'Site__SiteName')
        )).rename(columns={'Product__Product': 'Product', 'Site__SiteName': 'Site'})
        
        self.df_orderbook = pd.DataFrame(list(
            MasterDataOrderBook.objects.filter(version=scenario)
            .values('productkey', 'site')
        )).rename(columns={'productkey': 'Product', 'site': 'Site'})
        
        self.df_history = pd.DataFrame(list(
            MasterDataHistoryOfProductionModel.objects.filter(version=scenario)
            .values('Product', 'Foundry')
        )).rename(columns={'Foundry': 'Site'})
        
        self.df_supplier = pd.DataFrame(list(
            MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario, SourceType='Make')
            .values('PartNum', 'Plant')
        )).rename(columns={'PartNum': 'Product', 'Plant': 'Site'})
        
        # Load freight data
        self.df_freight = pd.DataFrame(list(
            MasterDataFreightModel.objects.filter(version=scenario)
            .values('ForecastRegion__Forecast_region', 'ManufacturingSite__SiteName', 
                   'PlantToDomesticPortDays', 'OceanFreightDays', 'PortToCustomerDays')
        ))
        if not self.df_freight.empty:
            self.df_freight['total_freight_days'] = (
                self.df_freight['PlantToDomesticPortDays'] + 
                self.df_freight['OceanFreightDays'] + 
                self.df_freight['PortToCustomerDays']
            )
        
        # Load cast to despatch
        self.df_cast_despatch = pd.DataFrame(list(
            MasterDataCastToDespatchModel.objects.filter(version=scenario)
            .values('Foundry__SiteName', 'CastToDespatchDays')
        )).rename(columns={'Foundry__SiteName': 'Site'})
        
        # Load incoterms
        self.df_incoterms = pd.DataFrame(list(
            MasterdataIncoTermsModel.objects.filter(version=scenario)
            .values('CustomerCode', 'Incoterm__IncoTerm')
        )).rename(columns={'Incoterm__IncoTerm': 'IncoTerm'})
        
        # Load safety stocks
        self.df_safety = pd.DataFrame(list(
            MasterDataSafetyStocks.objects.filter(version=scenario)
            .values('Plant', 'PartNum', 'MinimumQty', 'SafetyQty')
        ))
        if not self.df_safety.empty:
            self.df_safety['MinimumQty'] = self.df_safety['MinimumQty'].fillna(0).astype(float)
            self.df_safety['SafetyQty'] = self.df_safety['SafetyQty'].fillna(0).astype(float)
            self.df_safety['total_safety'] = self.df_safety['MinimumQty'] + self.df_safety['SafetyQty']
        
        # Load inventory
        self.df_inventory = pd.DataFrame(list(
            MasterDataInventory.objects.filter(version=scenario)
            .values('product', 'site_id', 'onhandstock_qty', 'intransitstock_qty', 'date_of_snapshot')
        ))
        if not self.df_inventory.empty:
            self.df_inventory['total_inventory'] = (
                self.df_inventory['onhandstock_qty'] + self.df_inventory['intransitstock_qty']
            )
        
        self.stdout.write("Data loading completed")

    def process_forecast_pandas(self):
        """Process forecast using pandas vectorized operations"""
        self.stdout.write("Processing forecast with pandas...")
        
        if self.df_forecast.empty:
            self.df_processed = pd.DataFrame()
            return
        
        # Deduplicate by summing quantities
        self.df_forecast_dedup = self.df_forecast.groupby([
            'Product', 'extracted_location', 'Period_AU'
        ]).agg({
            'Qty': 'sum',
            'Customer_code': 'first',  # Take first occurrence
            'Forecast_Region': 'first',
            'Location': 'first'
        }).reset_index()
        
        duplicate_count = len(self.df_forecast) - len(self.df_forecast_dedup)
        self.stdout.write(f"Deduplication: {len(self.df_forecast):,} -> {len(self.df_forecast_dedup):,} records")
        self.stdout.write(f"Removed {duplicate_count:,} duplicates")
        
        # Assign sites using pandas merge operations (much faster than loops)
        df_work = self.df_forecast_dedup.copy()
        
        # Priority 1: Manual assignments
        if not self.df_manual.empty:
            df_work = df_work.merge(
                self.df_manual[['Product', 'ShippingDate', 'Site']],
                left_on=['Product', 'Period_AU'], 
                right_on=['Product', 'ShippingDate'],
                how='left', suffixes=('', '_manual')
            )
            df_work['assigned_site'] = df_work['Site']
        else:
            df_work['assigned_site'] = None
        
        # Priority 2: Order book (only for unassigned)
        if not self.df_orderbook.empty:
            unassigned = df_work['assigned_site'].isna()
            df_work.loc[unassigned] = df_work.loc[unassigned].merge(
                self.df_orderbook[['Product', 'Site']],
                on='Product', how='left', suffixes=('', '_ob')
            )['Site'].fillna(df_work.loc[unassigned, 'assigned_site'])
        
        # Priority 3: Production history (only for unassigned)
        if not self.df_history.empty:
            unassigned = df_work['assigned_site'].isna()
            temp_merge = df_work.loc[unassigned].merge(
                self.df_history[['Product', 'Site']].drop_duplicates('Product'),
                on='Product', how='left', suffixes=('', '_hist')
            )
            df_work.loc[unassigned, 'assigned_site'] = temp_merge['Site'].fillna(df_work.loc[unassigned, 'assigned_site'])
        
        # Priority 4: Supplier mapping (only for unassigned)
        if not self.df_supplier.empty:
            unassigned = df_work['assigned_site'].isna()
            temp_merge = df_work.loc[unassigned].merge(
                self.df_supplier[['Product', 'Site']].drop_duplicates('Product'),
                on='Product', how='left', suffixes=('', '_sup')
            )
            df_work.loc[unassigned, 'assigned_site'] = temp_merge['Site'].fillna(df_work.loc[unassigned, 'assigned_site'])
        
        # Filter out unassigned
        self.df_processed = df_work.dropna(subset=['assigned_site']).copy()
        
        # Calculate shipping dates using vectorized operations
        self.calculate_shipping_dates_pandas()
        
        self.stdout.write(f"Processed {len(self.df_processed):,} records with site assignments")

    def calculate_shipping_dates_pandas(self):
        """Calculate shipping dates using pandas vectorized operations"""
        if self.df_processed.empty:
            return
        
        # Merge incoterms
        if not self.df_incoterms.empty:
            self.df_processed = self.df_processed.merge(
                self.df_incoterms, left_on='Customer_code', right_on='CustomerCode', how='left'
            )
        self.df_processed['IncoTerm'] = self.df_processed.get('IncoTerm', 'EXW').fillna('EXW')
        
        # Merge freight data
        if not self.df_freight.empty:
            self.df_processed = self.df_processed.merge(
                self.df_freight[['ForecastRegion__Forecast_region', 'ManufacturingSite__SiteName', 'total_freight_days']],
                left_on=['Forecast_Region', 'assigned_site'],
                right_on=['ForecastRegion__Forecast_region', 'ManufacturingSite__SiteName'],
                how='left'
            )
        self.df_processed['total_freight_days'] = self.df_processed.get('total_freight_days', 0).fillna(0)
        
        # Merge cast to despatch
        if not self.df_cast_despatch.empty:
            self.df_processed = self.df_processed.merge(
                self.df_cast_despatch, left_on='assigned_site', right_on='Site', how='left', suffixes=('', '_cast')
            )
        self.df_processed['CastToDespatchDays'] = self.df_processed.get('CastToDespatchDays', 0).fillna(0)
        
        # Calculate lead time based on incoterms (vectorized)
        is_exw_type = self.df_processed['IncoTerm'].isin(['EXW', 'FCA', 'FOB'])
        self.df_processed['total_lead_days'] = np.where(
            is_exw_type,
            self.df_processed['CastToDespatchDays'],
            self.df_processed['CastToDespatchDays'] + self.df_processed['total_freight_days']
        )
        
        # Calculate shipping dates (vectorized)
        self.df_processed['shipping_date'] = pd.to_datetime(self.df_processed['Period_AU']) - pd.to_timedelta(
            self.df_processed['total_lead_days'], unit='days'
        )
        
        # Adjust for snapshot dates if needed
        if not self.df_inventory.empty:
            inv_snapshot = self.df_inventory.groupby(['product', 'site_id']).agg({
                'date_of_snapshot': 'first'
            }).reset_index()
            
            self.df_processed = self.df_processed.merge(
                inv_snapshot, 
                left_on=['Product', 'extracted_location'], 
                right_on=['product', 'site_id'], 
                how='left'
            )
            
            # Ensure shipping date is not before snapshot date
            has_snapshot = self.df_processed['date_of_snapshot'].notna()
            self.df_processed.loc[has_snapshot, 'shipping_date'] = np.maximum(
                self.df_processed.loc[has_snapshot, 'shipping_date'],
                pd.to_datetime(self.df_processed.loc[has_snapshot, 'date_of_snapshot'])
            )
        
        # Group by month
        self.df_processed['month_key'] = self.df_processed['shipping_date'].dt.to_period('M')

    def calculate_replenishment_pandas(self, scenario):
        """Calculate replenishment using pandas vectorized operations"""
        self.stdout.write("Calculating replenishment with pandas...")
        
        if self.df_processed.empty:
            self.stdout.write("No processed data available for replenishment calculation")
            return
        
        # Group by month and product-location
        monthly_demand = self.df_processed.groupby([
            'month_key', 'Product', 'extracted_location', 'assigned_site'
        ])['Qty'].sum().reset_index()
        
        # Get all unique product-location combinations
        product_locations = monthly_demand[['Product', 'extracted_location']].drop_duplicates()
        
        # Load opening inventory for these combinations
        opening_inv = {}
        if not self.df_inventory.empty:
            inv_summary = self.df_inventory.groupby(['product', 'site_id']).agg({
                'total_inventory': 'sum'
            }).reset_index()
            
            for _, row in inv_summary.iterrows():
                opening_inv[(row['product'], row['site_id'])] = row['total_inventory']
        
        # Load safety stocks for these combinations
        safety_lookup = {}
        if not self.df_safety.empty:
            for _, row in self.df_safety.iterrows():
                safety_lookup[(row['Plant'], row['PartNum'])] = row['total_safety']
        
        # Process each product-location combination
        replenishment_records = []
        months = sorted(monthly_demand['month_key'].unique())
        
        for _, pl_row in product_locations.iterrows():
            product = pl_row['Product']
            location = pl_row['extracted_location']
            
            # Initialize inventory
            current_inventory = opening_inv.get((product, location), 0)
            
            # Get demands for this product-location across all months
            pl_demands = monthly_demand[
                (monthly_demand['Product'] == product) & 
                (monthly_demand['extracted_location'] == location)
            ].set_index('month_key')['Qty'].reindex(months, fill_value=0)
            
            # Get assigned site (should be consistent)
            assigned_site = monthly_demand[
                (monthly_demand['Product'] == product) & 
                (monthly_demand['extracted_location'] == location)
            ]['assigned_site'].iloc[0]
            
            # Get safety stock
            safety_stock = safety_lookup.get((location, product), 0)
            
            # Process each month
            for month in months:
                demand = pl_demands.get(month, 0)
                
                if demand > 0:
                    # Calculate net requirement
                    net_requirement = demand + safety_stock - current_inventory
                    
                    if net_requirement > 0:
                        # Create replenishment record
                        month_date = month.to_timestamp()
                        
                        replenishment_records.append({
                            'version_id': scenario.version,
                            'Product_id': product,
                            'Location': location,
                            'Site_id': assigned_site,
                            'ShippingDate': month_date,
                            'ReplenishmentQty': net_requirement
                        })
                        
                        # Update inventory
                        current_inventory += net_requirement
                    
                    # Deduct demand
                    current_inventory = max(0, current_inventory - demand)
        
        # Bulk create records
        if replenishment_records:
            # Convert to DataFrame for easier handling
            df_replenishment = pd.DataFrame(replenishment_records)
            
            # Create Django model instances
            model_instances = []
            product_map = {p.Product: p for p in MasterDataProductModel.objects.all()}
            plant_map = {p.SiteName: p for p in MasterDataPlantModel.objects.all()}
            
            for _, row in df_replenishment.iterrows():
                product_instance = product_map.get(row['Product_id'])
                site_instance = plant_map.get(row['Site_id'])
                
                if product_instance and site_instance:
                    model_instances.append(
                        CalcualtedReplenishmentModel(
                            version=scenario,
                            Product=product_instance,
                            Location=row['Location'],
                            Site=site_instance,
                            ShippingDate=row['ShippingDate'],
                            ReplenishmentQty=row['ReplenishmentQty']
                        )
                    )
            
            # Bulk create with larger batch size
            with transaction.atomic():
                CalcualtedReplenishmentModel.objects.bulk_create(model_instances, batch_size=5000)
            
            self.stdout.write(f"Created {len(model_instances):,} replenishment records")
        else:
            self.stdout.write("No replenishment records created")
        
        # Final summary
        final_count = CalcualtedReplenishmentModel.objects.filter(version=scenario).count()
        self.stdout.write(f"Total replenishment records in database: {final_count:,}")
