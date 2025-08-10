from datetime import timedelta, datetime
from django.core.management.base import BaseCommand
from django.db import transaction
from django.core.cache import cache
from django.db.models import Sum
from collections import defaultdict
import polars as pl
import pandas as pd
import time
from functools import wraps
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
# Remove the slow PowerBI import - we'll read customer data locally
# from website.powerbi_invoice_integration import get_customer_mapping_dict

def timing_step(step_name):
    """Decorator to measure execution time of each step in replenishment"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            self.stdout.write(f"⏱️  [{datetime.now().strftime('%H:%M:%S')}] Starting: {step_name}")
            
            try:
                result = func(self, *args, **kwargs)
                duration = time.time() - start_time
                self.stdout.write(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Completed: {step_name} ({duration:.2f}s)")
                return result
            except Exception as e:
                duration = time.time() - start_time
                self.stdout.write(f"❌ [{datetime.now().strftime('%H:%M:%S')}] Failed: {step_name} ({duration:.2f}s) - {str(e)}")
                raise
                
        return wrapper
    return decorator

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
        # Start overall timing
        overall_start_time = time.time()
        self.stdout.write("=" * 80)
        self.stdout.write(f"🚀 STARTING REPLENISHMENT V2 CALCULATION")
        self.stdout.write(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.stdout.write("=" * 80)
        
        version = kwargs['version']
        single_product = kwargs.get('product')
        
        if single_product:
            self.stdout.write(f"🎯 SINGLE PRODUCT MODE: {single_product}")
        else:
            self.stdout.write("🌐 ALL PRODUCTS MODE")
        
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
                # Step 1: Delete existing records
                step_start = time.time()
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
                step_duration = time.time() - step_start
                self.stdout.write(f"✅ Step 1: Delete existing records ({step_duration:.3f}s)")
                
                # Step 2: Load SMART forecast data
                step_start = time.time()
                self.stdout.write("STEP 2: Loading SMART forecast data...")
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
                step_duration = time.time() - step_start
                self.stdout.write(f"✅ Step 2: SMART forecast data loaded - {len(forecast_df)} records ({step_duration:.3f}s)")
                
                # Step 3: Load master data
                step_start = time.time()
                self.stdout.write("STEP 3: Loading master data...")
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
                step_duration = time.time() - step_start
                self.stdout.write(f"✅ Step 3: Master data loaded ({step_duration:.3f}s)")
                
                # Step 4: Prepare lookup dictionaries (customer data removed per request)
                step_start = time.time()
                self.stdout.write("STEP 4: Preparing lookup dictionaries...")
                product_map = {p['Product']: p for p in products}
                plant_map = {p['SiteName']: p for p in plants}
                
                # Order Book mapping
                from website.models import MasterDataOrderBook, MasterDataHistoryOfProductionModel, MasterDataEpicorSupplierMasterDataModel, MasterDataManuallyAssignProductionRequirement
                
                # Time each dictionary separately to identify bottlenecks
                dict_start = time.time()
                order_book_data = MasterDataOrderBook.objects.filter(version=scenario).exclude(site__isnull=True).exclude(site__exact='')
                self.stdout.write(f"   📋 Order Book query: {time.time() - dict_start:.3f}s ({len(order_book_data)} records)")
                
                dict_start = time.time()
                production_data = MasterDataHistoryOfProductionModel.objects.filter(version=scenario).exclude(Foundry__isnull=True).exclude(Foundry__exact='')
                self.stdout.write(f"   🏭 Production History query: {time.time() - dict_start:.3f}s ({len(production_data)} records)")
                
                dict_start = time.time()
                supplier_data = MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario).exclude(VendorID__isnull=True).exclude(VendorID__exact='')
                self.stdout.write(f"   🚚 Supplier query: {time.time() - dict_start:.3f}s ({len(supplier_data)} records)")
                
                dict_start = time.time()
                manual_assign_data = MasterDataManuallyAssignProductionRequirement.objects.filter(
                    version=scenario
                ).select_related('Product', 'Site').values(
                    'version__version', 'Product__Product', 'ShippingDate', 'Site__SiteName'
                )
                self.stdout.write(f"   📍 Manual Assignment query: {time.time() - dict_start:.3f}s ({len(manual_assign_data)} records)")
                
                # Build dictionaries from the data
                dict_start = time.time()
                
                order_book_map = {
                    (ob.version.version, ob.productkey): ob.site
                    for ob in order_book_data
                }
                
                production_map = {
                    (prod.version.version, prod.Product): prod.Foundry
                    for prod in production_data
                }
                
                supplier_map = {
                    (sup.version.version, sup.PartNum): sup.VendorID
                    for sup in supplier_data
                }
                
                manual_assign_map = {
                    (m['version__version'], m['Product__Product'], m['ShippingDate']): m['Site__SiteName']
                    for m in manual_assign_data
                    if m['Product__Product'] and m['Site__SiteName']  # Filter out nulls
                }
                
                self.stdout.write(f"   🔧 Dictionary construction: {time.time() - dict_start:.3f}s")
                self.stdout.write(f"   📊 Final counts - Order Book: {len(order_book_map)}, Production: {len(production_map)}, Supplier: {len(supplier_map)}, Manual: {len(manual_assign_map)}")
                
                foundry_sites = {'XUZ1', 'MTJ1', 'COI2', 'MER1', 'WUN1', 'WOD1', 'CHI1'}
                step_duration = time.time() - step_start
                self.stdout.write(f"✅ Step 4: Lookup dictionaries prepared ({step_duration:.3f}s)")
                
                # Step 5: Data validation and filtering
                step_start = time.time()
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
                step_duration = time.time() - step_start
                self.stdout.write(f"✅ Step 5: Data validation and filtering ({step_duration:.3f}s)")
                
                # Step 6: Process replenishment records (CRITICAL PATH)
                step_start = time.time()
                replenishment_records = []
                records_processed = 0
                site_selection_time = 0
                
                for row in forecast_df.iter_rows(named=True):
                    if not row['Product'] or not row['site_code']:
                        continue
                    
                    # Time site selection (this might be slow)
                    site_start = time.time()
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
                    site_selection_time += time.time() - site_start
                    
                    if not site:
                        continue
                    
                    # Customer data fields left blank per request
                    replenishment_records.append(CalcualtedReplenishmentModel(
                        version=scenario,
                        Product_id=row['Product'],
                        Site_id=site,
                        Location=row.get('Location', ''),
                        ShippingDate=row['Period_AU'],
                        ReplenishmentQty=row['Qty'],
                        latest_customer_invoice=None,
                        latest_customer_invoice_date=None,
                    ))
                    
                    records_processed += 1
                    
                    # Progress reporting
                    if records_processed % 100 == 0:
                        self.stdout.write(f"   📊 Processed {records_processed} records...")
                
                step_duration = time.time() - step_start
                self.stdout.write(f"✅ Step 6: Replenishment record processing - {len(replenishment_records)} records ({step_duration:.3f}s)")
                
                # Avoid division by zero for very fast operations
                if step_duration > 0:
                    site_selection_pct = site_selection_time/step_duration*100
                else:
                    site_selection_pct = 0.0
                    
                self.stdout.write(f"   🎯 Site selection time: {site_selection_time:.3f}s ({site_selection_pct:.1f}%)")
                
                # Step 7: Bulk create records
                step_start = time.time()
                if replenishment_records:
                    CalcualtedReplenishmentModel.objects.bulk_create(replenishment_records, batch_size=1000)
                    self.stdout.write(f"Created {len(replenishment_records)} replenishment records")
                step_duration = time.time() - step_start
                self.stdout.write(f"✅ Step 7: Database bulk create ({step_duration:.3f}s)")
                
                # Final summary
                overall_duration = time.time() - overall_start_time
                self.stdout.write("=" * 80)
                self.stdout.write(f"🎉 REPLENISHMENT V2 CALCULATION COMPLETED")
                self.stdout.write(f"⏱️  Total execution time: {overall_duration:.2f} seconds")
                self.stdout.write(f"📅 Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                if single_product:
                    self.stdout.write(f"🎯 Single product mode: {single_product}")
                self.stdout.write("=" * 80)
                
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
