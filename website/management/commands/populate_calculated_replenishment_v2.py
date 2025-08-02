from datetime import timedelta, datetime
from django.core.management.base import BaseCommand
from django.db import transaction
from django.core.cache import cache
from django.db.models import Sum
from collections import defaultdict
import polars as pl
import pandas as pd
from website.models import (
    scenarios,
    SMART_Forecast_Model,
    MasterDataProductModel,
    MasterDataPlantModel,
    CalcualtedReplenishmentModel,
    CalculatedProductionModel,
    MasterDataCastToDespatchModel,
    MasterDataInventory,
    MasterDataSafetyStocks
)

def extract_site_code(location):
    if not isinstance(location, str):
        return None
    # Split by '-' and get the part after the dash
    parts = location.split('-')
    if len(parts) > 1:
        candidate = parts[-1]
        # If there are underscores, get the last part after '_'
        candidate = candidate.split('_')[-1]
        if len(candidate) == 4 and candidate.isalnum():
            return candidate
    return None

class Command(BaseCommand):
    help = 'Calculate replenishment needs based on SMART forecast and inventory data'

    def add_arguments(self, parser):
        parser.add_argument(
            'version',
            type=str,
            help="The version of the scenario to calculate replenishment for.",
        )
        parser.add_argument(
            '--product',
            type=str,
            help="Optional: Calculate for a specific product only.",
        )

    def handle(self, *args, **kwargs):
        version = kwargs['version']
        single_product = kwargs.get('product')
        
        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario version '{version}' not found."))
            return

        # Simple execution lock
        cache_key = f"replenishment_v2_running_{scenario.version.replace(' ', '_')}"
        if single_product:
            cache_key += f"_{single_product}"
        
        if cache.get(cache_key):
            self.stdout.write(self.style.ERROR(f"Replenishment V2 calculation already running for version {version}"))
            return

        # Set the execution lock
        cache.set(cache_key, True, timeout=3600)  # 1 hour timeout

        try:
            with transaction.atomic():
                # Delete existing records
                if single_product:
                    deleted_replenishment = CalcualtedReplenishmentModel.objects.filter(
                        version=scenario,
                        Product__Product=single_product
                    ).delete()[0]
                    deleted_production = CalculatedProductionModel.objects.filter(
                        version=scenario,
                        product__Product=single_product
                    ).delete()[0]
                    self.stdout.write(f"Deleted {deleted_replenishment} replenishment and {deleted_production} production records for product '{single_product}'")
                else:
                    deleted_replenishment = CalcualtedReplenishmentModel.objects.filter(version=scenario).delete()[0]
                    deleted_production = CalculatedProductionModel.objects.filter(version=scenario).delete()[0]
                    self.stdout.write(f"Deleted {deleted_replenishment} replenishment and {deleted_production} production records")

                # STEP 1: Load and filter forecast data
                self.stdout.write("STEP 1: Loading SMART forecast data...")
                
                # Load all SMART forecast data for this version using pandas first - Filter out zero quantities
                forecast_data = list(SMART_Forecast_Model.objects.filter(
                    version=scenario,
                    Qty__gt=0  # Only include records with Qty > 0 for performance
                ).values(
                    'Product', 'Qty', 'Period_AU', 'Data_Source', 'Forecast_Region', 
                    'Customer_code', 'Location', 'ProductFamilyDescription'
                ))
                
                if not forecast_data:
                    self.stdout.write(self.style.WARNING(f"No SMART forecast data found for version {version}"))
                    return

                # Convert to pandas first to handle mixed data types properly
                forecast_df_pd = pd.DataFrame(forecast_data)
                
                # Convert to polars using pyarrow for better schema handling
                try:
                    forecast_df = pl.from_pandas(forecast_df_pd)
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error converting forecast data to polars: {str(e)}"))
                    return
                
                # Filter out Fixed Plant and Revenue Forecast - only process regular SMART forecast
                forecast_df = forecast_df.filter(
                    (~pl.col('Data_Source').is_in(['Fixed Plant', 'Revenue Forecast'])) &
                    (pl.col('Qty') > 0) &
                    (pl.col('Product').is_not_null())
                )

                if single_product:
                    forecast_df = forecast_df.filter(pl.col('Product') == single_product)

                if len(forecast_df) == 0:
                    self.stdout.write(self.style.WARNING(f"No forecast data to process for version {version}"))
                    return

                self.stdout.write(f"Loaded {len(forecast_df)} forecast records for processing")

                # STEP 2: Load master data
                self.stdout.write("STEP 2: Loading master data...")
                
                # Load product data
                products = list(MasterDataProductModel.objects.all().values(
                    'Product', 'DressMass', 'ProductGroup', 'ParentProductGroupDescription'
                ))
                product_df = pl.from_pandas(pd.DataFrame(products))
                
                # Load plant data
                plants = list(MasterDataPlantModel.objects.all().values('SiteName'))
                plant_df = pl.from_pandas(pd.DataFrame(plants))
                
                # Load cast to despatch data
                cast_to_despatch = list(MasterDataCastToDespatchModel.objects.filter(version=scenario).values(
                    'Foundry__SiteName', 'CastToDespatchDays'
                ))
                cast_to_despatch_df = pl.from_pandas(pd.DataFrame(cast_to_despatch)) if cast_to_despatch else pl.DataFrame()
                
                # Load inventory data
                inventory_data = list(MasterDataInventory.objects.filter(version=scenario).values(
                    'product', 'site_id', 'onhandstock_qty', 'intransitstock_qty', 'wip_stock_qty'
                ))
                inventory_df = pl.from_pandas(pd.DataFrame(inventory_data)) if inventory_data else pl.DataFrame()
                
                # Load safety stock data
                safety_stock_data = list(MasterDataSafetyStocks.objects.filter(version=scenario).values(
                    'PartNum', 'Plant', 'SafetyQty'
                ))
                safety_stock_df = pl.from_pandas(pd.DataFrame(safety_stock_data)) if safety_stock_data else pl.DataFrame()

                self.stdout.write("Master data loaded successfully")

                # STEP 3: Process forecast data with site extraction
                self.stdout.write("STEP 3: Processing forecast with site extraction...")
                
                # Extract site codes from Location field
                forecast_df = forecast_df.with_columns([
                    pl.col('Location').map_elements(extract_site_code, return_dtype=pl.Utf8).alias('site_code')
                ])
                
                # Filter to keep only records with valid site codes that exist in plant master data
                # Get list of valid site names
                valid_sites = set(plant_df['SiteName'].to_list())
                forecast_df = forecast_df.filter(pl.col('site_code').is_in(valid_sites))
                
                if len(forecast_df) == 0:
                    self.stdout.write(self.style.WARNING("No forecast records with valid site codes in plant master data"))
                    return

                self.stdout.write(f"After filtering invalid sites: {len(forecast_df)} forecast records")

                # STEP 4: Calculate shipping dates and replenishment
                self.stdout.write("STEP 4: Calculating replenishment...")
                
                # Join with product data to filter out invalid products
                forecast_df = forecast_df.join(
                    product_df,
                    left_on='Product',
                    right_on='Product',
                    how='inner'  # Only keep products that exist in master data
                )
                
                if len(forecast_df) == 0:
                    self.stdout.write(self.style.WARNING("No valid forecast records with products in master data"))
                    return

                self.stdout.write(f"After filtering invalid products: {len(forecast_df)} forecast records")
                
                # Calculate pouring dates (simplified - subtract 30 days)
                forecast_df = forecast_df.with_columns([
                    (pl.col('Period_AU') - pl.duration(days=30)).alias('pouring_date')
                ])
                
                # Create replenishment records
                replenishment_records = []
                production_records = []
                
                for row in forecast_df.iter_rows(named=True):
                    if not row['Product'] or not row['site_code']:
                        continue
                        
                    # Create replenishment record
                    replenishment_records.append(CalcualtedReplenishmentModel(
                        version=scenario,
                        Product_id=row['Product'],
                        Site_id=row['site_code'],
                        Location=row.get('Location', ''),
                        ShippingDate=row['Period_AU'],
                        ReplenishmentQty=row['Qty']
                    ))

                # Bulk create records
                if replenishment_records:
                    CalcualtedReplenishmentModel.objects.bulk_create(replenishment_records, batch_size=1000)
                    self.stdout.write(f"Created {len(replenishment_records)} replenishment records")

                self.stdout.write(self.style.SUCCESS(f"Replenishment calculation completed for version {version}"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error in replenishment calculation: {str(e)}"))
            raise
        finally:
            # Always release the execution lock
            cache.delete(cache_key)

    def calculate_pouring_date_optimized(self, shipping_date, production_site, snapshot_date):
        """Calculate pouring date based on shipping date and production site"""
        # Simplified version - subtract 30 days as default
        return shipping_date - timedelta(days=30)
