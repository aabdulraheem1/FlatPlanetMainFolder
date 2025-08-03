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
from website.powerbi_invoice_integration import get_customer_mapping_dict

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

def select_site(
    product,
    period,
    customer_code,
    forecast_region,
    scenario,
    order_book_map,
    production_map,
    supplier_map,
    manual_assign_map,
    plant_map,
    foundry_sites,
    can_assign_foundry_fn
):
    # Priority 1: Manual assignment
    manual_lookup_key = (scenario.version, product, period)
    manual_site = manual_assign_map.get(manual_lookup_key)
    if manual_site:
        site = getattr(manual_site, 'SiteName', manual_site)
        return site
    # Priority 2: Order Book
    site = order_book_map.get((scenario.version, product))
    if site:
        return site
    # Priority 3: Production History
    foundry = production_map.get((scenario.version, product))
    if foundry:
        return foundry
    # Priority 4: Supplier
    vendor_id = supplier_map.get((scenario.version, product))
    if vendor_id:
        return vendor_id
    # Foundry assignment check
    if site and site in foundry_sites and not manual_site:
        if not can_assign_foundry_fn(product):
            return None
    return site

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
        cache_key = f"replenishment_v2_running_{scenario.version.replace(' ', '_')}"
        if single_product:
            cache_key += f"_{single_product}"
        if cache.get(cache_key):
            self.stdout.write(self.style.ERROR(f"Replenishment V2 calculation already running for version {version}"))
            return
        cache.set(cache_key, True, timeout=3600)
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
                self.stdout.write("STEP 1: Loading SMART forecast data...")
                forecast_data = list(SMART_Forecast_Model.objects.filter(
                    version=scenario,
                    Qty__gt=0
                ).values(
                    'Product', 'Qty', 'Period_AU', 'Data_Source', 'Forecast_Region',
                    'Customer_code', 'Location', 'ProductFamilyDescription'
                ))
                if not forecast_data:
                    self.stdout.write(self.style.WARNING(f"No SMART forecast data found for version {version}"))
                    return
                forecast_df_pd = pd.DataFrame(forecast_data)
                try:
                    forecast_df = pl.from_pandas(forecast_df_pd)
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error converting forecast data to polars: {str(e)}"))
                    return
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
                self.stdout.write("STEP 2: Loading master data...")
                products = list(MasterDataProductModel.objects.all().values(
                    'Product', 'DressMass', 'ProductGroup', 'ParentProductGroupDescription'
                ))
                product_df = pl.from_pandas(pd.DataFrame(products))
                plants = list(MasterDataPlantModel.objects.all().values('SiteName'))
                plant_df = pl.from_pandas(pd.DataFrame(plants))
                cast_to_despatch = list(MasterDataCastToDespatchModel.objects.filter(version=scenario).values(
                    'Foundry__SiteName', 'CastToDespatchDays'
                ))
                cast_to_despatch_df = pl.from_pandas(pd.DataFrame(cast_to_despatch)) if cast_to_despatch else pl.DataFrame()
                inventory_data = list(MasterDataInventory.objects.filter(version=scenario).values(
                    'product', 'site_id', 'onhandstock_qty', 'intransitstock_qty', 'wip_stock_qty'
                ))
                inventory_df = pl.from_pandas(pd.DataFrame(inventory_data)) if inventory_data else pl.DataFrame()
                safety_stock_data = list(MasterDataSafetyStocks.objects.filter(version=scenario).values(
                    'PartNum', 'Plant', 'SafetyQty'
                ))
                safety_stock_df = pl.from_pandas(pd.DataFrame(safety_stock_data)) if safety_stock_data else pl.DataFrame()
                self.stdout.write("Master data loaded successfully")
                
                # Fetch customer invoice mapping for this scenario
                self.stdout.write("Fetching customer invoice data from PowerBI...")
                try:
                    customer_mapping = get_customer_mapping_dict()
                    self.stdout.write(f"Loaded {len(customer_mapping)} customer invoice mappings")
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"Failed to load customer data: {e}"))
                    customer_mapping = {}
                
                # Prepare lookup dictionaries for site selection
                product_map = {p['Product']: p for p in products}
                plant_map = {p['SiteName']: p for p in plants}
                # Order Book mapping
                from website.models import MasterDataOrderBook, MasterDataHistoryOfProductionModel, MasterDataEpicorSupplierMasterDataModel, MasterDataManuallyAssignProductionRequirement
                order_book_map = {
                    (ob.version.version, ob.productkey): ob.site
                    for ob in MasterDataOrderBook.objects.filter(version=scenario).exclude(site__isnull=True).exclude(site__exact='')
                }
                # Production History mapping
                production_map = {
                    (prod.version.version, prod.Product): prod.Foundry
                    for prod in MasterDataHistoryOfProductionModel.objects.filter(version=scenario).exclude(Foundry__isnull=True).exclude(Foundry__exact='')
                }
                # Supplier mapping
                supplier_map = {
                    (sup.version.version, sup.PartNum): sup.VendorID
                    for sup in MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario).exclude(VendorID__isnull=True).exclude(VendorID__exact='')
                }
                # Manual assignment mapping
                manual_assign_map = {
                    (m.version.version, m.Product.Product, m.ShippingDate): m.Site
                    for m in MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario)
                }
                foundry_sites = {'XUZ1', 'MTJ1', 'COI2', 'MER1', 'WUN1', 'WOD1', 'CHI1'}
                def can_assign_foundry_fn(product):
                    # Implement foundry assignment check if needed
                    # Example: Only allow foundry assignment if product exists in production_map
                    return (scenario.version, product) in production_map
                forecast_df = forecast_df.with_columns([
                    pl.col('Location').map_elements(extract_site_code, return_dtype=pl.Utf8).alias('site_code')
                ])
                valid_sites = set(plant_df['SiteName'].to_list())
                forecast_df = forecast_df.filter(pl.col('site_code').is_in(valid_sites))
                if len(forecast_df) == 0:
                    self.stdout.write(self.style.WARNING("No forecast records with valid site codes in plant master data"))
                    return
                self.stdout.write(f"After filtering invalid sites: {len(forecast_df)} forecast records")
                forecast_df = forecast_df.join(
                    product_df,
                    left_on='Product',
                    right_on='Product',
                    how='inner'
                )
                if len(forecast_df) == 0:
                    self.stdout.write(self.style.WARNING("No valid forecast records with products in master data"))
                    return
                self.stdout.write(f"After filtering invalid products: {len(forecast_df)} forecast records")
                forecast_df = forecast_df.with_columns([
                    (pl.col('Period_AU') - pl.duration(days=30)).alias('pouring_date')
                ])
                replenishment_records = []
                for row in forecast_df.iter_rows(named=True):
                    if not row['Product'] or not row['site_code']:
                        continue
                    # Use the correct site selection logic
                    site = select_site(
                        row['Product'],
                        row['Period_AU'],
                        row.get('Customer_code', None),
                        row.get('Forecast_Region', None),
                        scenario,
                        order_book_map,
                        production_map,
                        supplier_map,
                        manual_assign_map,
                        plant_map,
                        foundry_sites,
                        can_assign_foundry_fn
                    )
                    if not site:
                        continue
                    
                    # Get customer invoice data
                    customer_data = customer_mapping.get(row['Product'], {})
                    latest_customer_invoice = customer_data.get('customer_name')
                    latest_customer_invoice_date = customer_data.get('invoice_date')
                    
                    replenishment_records.append(CalcualtedReplenishmentModel(
                        version=scenario,
                        Product_id=row['Product'],
                        Site_id=site,
                        Location=row.get('Location', ''),
                        ShippingDate=row['Period_AU'],
                        ReplenishmentQty=row['Qty'],
                        latest_customer_invoice=latest_customer_invoice,
                        latest_customer_invoice_date=latest_customer_invoice_date,
                    ))
                if replenishment_records:
                    CalcualtedReplenishmentModel.objects.bulk_create(replenishment_records, batch_size=1000)
                    self.stdout.write(f"Created {len(replenishment_records)} replenishment records")
                self.stdout.write(self.style.SUCCESS(f"Replenishment calculation completed for version {version}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error in replenishment calculation: {str(e)}"))
            raise
        finally:
            cache.delete(cache_key)

    def calculate_pouring_date_optimized(self, shipping_date, production_site, snapshot_date):
        """Calculate pouring date based on shipping date and production site"""
        # Simplified version - subtract 30 days as default
        return shipping_date - timedelta(days=30)
