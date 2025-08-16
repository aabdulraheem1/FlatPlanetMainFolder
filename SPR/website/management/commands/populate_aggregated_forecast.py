from django.core.management.base import BaseCommand
from django.conf import settings
from website.models import (
    AggregatedForecast, SMART_Forecast_Model, MasterDataProductModel,
    ProductSiteCostModel, MasterDataInventory, scenarios,
    FixedPlantConversionModifiersModel, RevenueToCogsConversionModel, SiteAllocationModel
)
import polars as pl
import pandas as pd  # Keep for SQL read operations
from sqlalchemy import create_engine
from django.db.models import Sum
import time
from datetime import datetime
import time
from datetime import datetime
from functools import wraps

def time_step(step_name):
    """Decorator to measure execution time of each step"""
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

class Command(BaseCommand):
    help = 'Populate the AggregatedForecast model with data from all sources (Regular SMART, Fixed Plant, Revenue Forecast)'
    
    

    def add_arguments(self, parser):
        parser.add_argument(
            'version',
            type=str,
            help="The version of the scenario to populate data for.",
        )
        parser.add_argument(
            '--product',
            type=str,
            help="Optional: Filter calculation to a specific product only (much faster for testing)",
        )

    def handle(self, *args, **kwargs):
        # Start overall timing
        overall_start_time = time.time()
        self.stdout.write("=" * 80)
        self.stdout.write(f"🚀 STARTING AGGREGATED FORECAST CALCULATION")
        self.stdout.write(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.stdout.write("=" * 80)
        
        version = kwargs['version']
        single_product = kwargs.get('product')
        
        if single_product:
            self.stdout.write(f"🎯 SINGLE PRODUCT MODE: {single_product}")
        else:
            self.stdout.write("🌐 ALL PRODUCTS MODE")
        
        # Step 1: Validate scenario
        step_start = time.time()
        try:
            scenario = scenarios.objects.get(version=version)
            step_duration = time.time() - step_start
            self.stdout.write(f"✅ Step 1: Scenario validation ({step_duration:.3f}s)")
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario version '{version}' not found."))
            return

        # Step 2: Delete old records
        step_start = time.time()
        if single_product:
            deleted_count = AggregatedForecast.objects.filter(
                version=scenario,
                product_id=single_product
            ).delete()[0]
            step_duration = time.time() - step_start
            self.stdout.write(f"✅ Step 2: Deleted {deleted_count} existing records for product {single_product} ({step_duration:.3f}s)")
        else:
            AggregatedForecast.objects.filter(version=scenario).delete()
            step_duration = time.time() - step_start
            self.stdout.write(f"✅ Step 2: Deleted all existing aggregated forecast data ({step_duration:.3f}s)")

        # Step 3: Build database engine
        step_start = time.time()
        db_settings = settings.DATABASES['default']
        if db_settings['ENGINE'].endswith('sqlite3'):
            engine = create_engine(f"sqlite:///{db_settings['NAME']}")
        else:
            user = db_settings['USER']
            password = db_settings['PASSWORD']
            host = db_settings['HOST']
            port = db_settings['PORT']
            name = db_settings['NAME']
            if port:
                engine = create_engine(f"mssql+pyodbc://{user}:{password}@{host}:{port}/{name}?driver=ODBC+Driver+17+for+SQL+Server")
            else:
                engine = create_engine(f"mssql+pyodbc://{user}:{password}@{host}/{name}?driver=ODBC+Driver+17+for+SQL+Server")
        step_duration = time.time() - step_start
        self.stdout.write(f"✅ Step 3: Database engine setup ({step_duration:.3f}s)")

        # Step 4: Load SMART forecast data
        step_start = time.time()
        if single_product:
            forecast_df_pd = pd.read_sql_query(
                """SELECT * FROM website_smart_forecast_model 
                   WHERE version_id = ? 
                   AND Product = ?""", 
                engine, params=(version, single_product)
            )
            self.stdout.write(f"🎯 Loading ALL SMART forecast data for single product: {single_product} (including zero quantities)")
        else:
            forecast_df_pd = pd.read_sql_query(
                """SELECT * FROM website_smart_forecast_model 
                   WHERE version_id = ?""", 
                engine, params=(version,)
            )
            self.stdout.write("🌐 Loading ALL SMART forecast data for all products (including zero quantities)")
        
        step_duration = time.time() - step_start
        self.stdout.write(f"✅ Step 4: SMART forecast data loaded - {len(forecast_df_pd)} records ({step_duration:.3f}s)")
        
        # Step 5: Data type conversion
        step_start = time.time()
        # Convert datetime columns to proper format if needed
        for col in forecast_df_pd.columns:
            if forecast_df_pd[col].dtype == 'object':
                # Check if this might be a datetime column
                if 'date' in col.lower() or 'period' in col.lower():
                    try:
                        forecast_df_pd[col] = pd.to_datetime(forecast_df_pd[col], errors='ignore')
                    except:
                        pass
        
        try:
            forecast_df = pl.from_pandas(forecast_df_pd)
        except Exception as e:
            self.stdout.write(f"Error converting to polars: {e}")
            self.stdout.write("Falling back to pandas processing...")
            raise
        
        step_duration = time.time() - step_start
        self.stdout.write(f"✅ Step 5: Data type conversion and Polars conversion ({step_duration:.3f}s)")
        
        if forecast_df.is_empty():
            self.stdout.write(self.style.WARNING(f"No SMART forecast data found for version {version}"))
            return

        # Step 6: Load master data
        step_start = time.time()
        product_df_pd = pd.read_sql_query(
            "SELECT Product, DressMass, ProductGroupDescription, ParentProductGroupDescription FROM website_masterdataproductmodel",
            engine
        )
        product_df = pl.from_pandas(product_df_pd)
        step_duration = time.time() - step_start
        self.stdout.write(f"✅ Step 6: Master data loaded - {len(product_df_pd)} products ({step_duration:.3f}s)")

        # Step 7: Load cost data
        step_start = time.time()
        product_site_cost_df_pd = pd.read_sql_query(
            "SELECT version_id, product_id, site_id, cost_aud, revenue_cost_aud FROM website_productsitecostmodel WHERE version_id = ?",
            engine, params=(version,)
        )
        product_site_cost_df = pl.from_pandas(product_site_cost_df_pd)
        
        inventory_df_pd = pd.read_sql_query(
            "SELECT version_id, product, site_id, cost_aud FROM website_masterdatainventory WHERE version_id = ?",
            engine, params=(version,)
        )
        inventory_df = pl.from_pandas(inventory_df_pd)
        step_duration = time.time() - step_start
        self.stdout.write(f"✅ Step 7: Cost data loaded - {len(product_site_cost_df_pd)} cost records, {len(inventory_df_pd)} inventory records ({step_duration:.3f}s)")

        # Step 8: Process data by source
        step_start = time.time()
        all_agg_objs = []
        
        # Track data source metrics
        data_source_counts = forecast_df.group_by("Data_Source").agg(pl.len().alias("count")).to_pandas()
        data_source_dict = dict(zip(data_source_counts["Data_Source"], data_source_counts["count"]))
        self.stdout.write(f"📊 Data source breakdown: {data_source_dict}")
        step_duration = time.time() - step_start
        self.stdout.write(f"✅ Step 8: Data source analysis ({step_duration:.3f}s)")
        
        # Step 9: Process Regular SMART Forecast
        regular_df = forecast_df.filter(~pl.col("Data_Source").is_in(['Fixed Plant', 'Revenue Forecast']))
        if not regular_df.is_empty():
            step_start = time.time()
            regular_objs = self._process_regular_forecast_timed(regular_df, product_df, product_site_cost_df, inventory_df, scenario)
            all_agg_objs.extend(regular_objs)
            regular_tonnes = sum(obj.tonnes for obj in regular_objs)
            step_duration = time.time() - step_start
            self.stdout.write(f"✅ Step 9: Regular forecast processed - {len(regular_objs)} records, {regular_tonnes:.2f} tonnes ({step_duration:.3f}s)")

        # Step 10: Process Fixed Plant
        fixed_plant_df = forecast_df.filter(pl.col("Data_Source") == 'Fixed Plant')
        if not fixed_plant_df.is_empty():
            step_start = time.time()
            fixed_plant_objs = self._process_fixed_plant_forecast_timed(fixed_plant_df, product_df, scenario)
            all_agg_objs.extend(fixed_plant_objs)
            fixed_plant_tonnes = sum(obj.tonnes for obj in fixed_plant_objs)
            step_duration = time.time() - step_start
            self.stdout.write(f"✅ Step 10: Fixed Plant processed - {len(fixed_plant_objs)} records, {fixed_plant_tonnes:.2f} tonnes ({step_duration:.3f}s)")

        # Step 11: Process Revenue Forecast
        revenue_df = forecast_df.filter(pl.col("Data_Source") == 'Revenue Forecast')
        if not revenue_df.is_empty():
            step_start = time.time()
            revenue_objs = self._process_revenue_forecast_timed(revenue_df, product_df, scenario)
            all_agg_objs.extend(revenue_objs)
            revenue_tonnes = sum(obj.tonnes for obj in revenue_objs)
            step_duration = time.time() - step_start
            self.stdout.write(f"✅ Step 11: Revenue Forecast processed - {len(revenue_objs)} records, {revenue_tonnes:.2f} tonnes ({step_duration:.3f}s)")

        # Step 12: Bulk create records
        if all_agg_objs:
            step_start = time.time()
            AggregatedForecast.objects.bulk_create(all_agg_objs, batch_size=1000)
            step_duration = time.time() - step_start
            
            total_tonnes = sum(obj.tonnes for obj in all_agg_objs)
            total_revenue = sum(obj.revenue_aud for obj in all_agg_objs)
            total_cogs = sum(obj.cogs_aud for obj in all_agg_objs)
            
            self.stdout.write(f"✅ Step 12: Database bulk create - {len(all_agg_objs)} records ({step_duration:.3f}s)")
            self.stdout.write(f"📊 Final Summary: {total_tonnes:.2f} tonnes, ${total_revenue:,.2f} revenue, ${total_cogs:,.2f} COGS")
        else:
            self.stdout.write(self.style.WARNING("No records were created"))

        # Step 13: Validation
        step_start = time.time()
        self._validate_products(scenario)
        step_duration = time.time() - step_start
        self.stdout.write(f"✅ Step 13: Product validation ({step_duration:.3f}s)")

        # Final timing summary
        overall_duration = time.time() - overall_start_time
        self.stdout.write("=" * 80)
        self.stdout.write(f"🎉 AGGREGATED FORECAST CALCULATION COMPLETED")
        self.stdout.write(f"⏱️  Total execution time: {overall_duration:.2f} seconds")
        self.stdout.write(f"📅 Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if single_product:
            self.stdout.write(f"🎯 Single product mode: {single_product}")
        self.stdout.write("=" * 80)

    def _process_regular_forecast_timed(self, forecast_df, product_df, product_site_cost_df, inventory_df, scenario):
        """Process regular SMART forecast data with polars optimization and detailed timing."""
        sub_start = time.time()
        
        # Merge forecast and product data
        merged = forecast_df.join(
            product_df,
            left_on='Product',
            right_on='Product',
            how='left',
            suffix='_product'
        )
        merge_time = time.time() - sub_start
        self.stdout.write(f"   🔗 Data merge: {merge_time:.3f}s")

        # Calculate tonnes with polars expressions
        calc_start = time.time()
        merged = merged.with_columns([
            pl.when(
                (pl.col('Qty').is_not_null()) & 
                (pl.col('DressMass').is_not_null()) & 
                (pl.col('DressMass') != 0)
            ).then(pl.col('Qty') * pl.col('DressMass') / 1000)
            .when(
                (pl.col('Qty').is_not_null()) & 
                (pl.col('PriceAUD').is_not_null())
            ).then((pl.col('Qty') * pl.col('PriceAUD') * 0.65) / 5000)
            .otherwise(0)
            .alias('tonnes'),
            
            # Extract site code
            pl.col('Location').str.split('-').list.last().str.split('_').list.last().alias('site_code'),
            
            # Calculate revenue
            (pl.col('Qty') * pl.col('PriceAUD')).alias('revenue_aud')
        ])
        calc_time = time.time() - calc_start
        self.stdout.write(f"   🧮 Calculations: {calc_time:.3f}s")

        # Create aggregation DataFrame
        agg_start = time.time()
        agg_df = merged.select([
            pl.col('version_id'),
            pl.col('tonnes'),
            pl.col('Forecast_Region').alias('forecast_region'),
            pl.col('Customer_code').alias('customer_code'),
            pl.col('Period_AU').alias('period'),
            pl.col('Product').alias('product_id'),
            pl.col('ProductGroupDescription').alias('product_group_description'),
            pl.col('ParentProductGroupDescription').alias('parent_product_group_description'),
            pl.col('site_code').alias('site_id'),
            pl.col('Qty'),
            pl.col('revenue_aud')
        ])

        # Clean product_id: convert to string, strip whitespace, filter out empty
        agg_df = agg_df.with_columns([
            pl.col('product_id').cast(pl.Utf8).str.strip_chars().alias('product_id')
        ]).filter(pl.col('product_id') != '')

        # Get all valid product codes from the DB
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        agg_df = agg_df.filter(pl.col('product_id').is_in(valid_products))
        agg_time = time.time() - agg_start
        self.stdout.write(f"   📋 Aggregation prep: {agg_time:.3f}s")

        # Convert to pandas for cost lookups
        pandas_start = time.time()
        agg_df_pd = agg_df.to_pandas()
        product_site_cost_df_pd = product_site_cost_df.to_pandas()
        inventory_df_pd = inventory_df.to_pandas()
        pandas_time = time.time() - pandas_start
        self.stdout.write(f"   🔄 Pandas conversion: {pandas_time:.3f}s")

        # Cost lookup preparation
        lookup_start = time.time()
        cost_lookup = {
            (str(row['version_id']), str(row['product_id']), str(row['site_id'])): row['cost_aud']
            for _, row in product_site_cost_df_pd.iterrows()
            if pd.notnull(row['cost_aud'])
        }
        inv_cost_lookup = {
            (str(row['version_id']), str(row['product']), str(row['site_id'])): row['cost_aud']
            for _, row in inventory_df_pd.iterrows()
            if pd.notnull(row['cost_aud'])
        }
        revenue_cost_lookup = {
            (str(row['version_id']), str(row['product_id']), str(row['site_id'])): row['revenue_cost_aud']
            for _, row in product_site_cost_df_pd.iterrows()
            if pd.notnull(row['revenue_cost_aud'])
        }
        lookup_time = time.time() - lookup_start
        self.stdout.write(f"   💰 Cost lookup prep: {lookup_time:.3f}s")

        def fast_cogs_aud(row):
            key = (str(row['version_id']), str(row['product_id']), str(row['site_id']))
            qty = row.get('Qty', 1)
            cost = cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            cost = inv_cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            cost = revenue_cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            return 0

        # Apply cost calculations
        cost_calc_start = time.time()
        agg_df_pd['cogs_aud'] = agg_df_pd.apply(fast_cogs_aud, axis=1)
        for col in ['cogs_aud', 'revenue_aud', 'Qty', 'tonnes']:
            agg_df_pd[col] = pd.to_numeric(agg_df_pd[col], errors='coerce').fillna(0)
        cost_calc_time = time.time() - cost_calc_start
        self.stdout.write(f"   💸 Cost calculations: {cost_calc_time:.3f}s")

        # Create objects
        obj_start = time.time()
        objs = [
            AggregatedForecast(
                version=scenario,
                tonnes=float(row['tonnes']),
                forecast_region=row['forecast_region'],
                customer_code=row['customer_code'],
                period=row['period'],
                product_id=row['product_id'],
                product_group_description=row['product_group_description'],
                parent_product_group_description=row['parent_product_group_description'],
                cogs_aud=float(row['cogs_aud']),
                qty=float(row['Qty']),
                revenue_aud=float(row['revenue_aud']),
            )
            for _, row in agg_df_pd.iterrows()
        ]
        obj_time = time.time() - obj_start
        self.stdout.write(f"   🏗️  Object creation: {obj_time:.3f}s")
        
        total_time = time.time() - sub_start
        self.stdout.write(f"   ⏱️  Regular forecast total: {total_time:.3f}s ({len(objs)} objects)")
        return objs
        """Process regular SMART forecast data with polars optimization."""
        
        # Merge forecast and product data
        merged = forecast_df.join(
            product_df,
            left_on='Product',
            right_on='Product',
            how='left',
            suffix='_product'
        )

        # Calculate tonnes with polars expressions
        merged = merged.with_columns([
            pl.when(
                (pl.col('Qty').is_not_null()) & 
                (pl.col('DressMass').is_not_null()) & 
                (pl.col('DressMass') != 0)
            ).then(pl.col('Qty') * pl.col('DressMass') / 1000)
            .when(
                (pl.col('Qty').is_not_null()) & 
                (pl.col('PriceAUD').is_not_null())
            ).then((pl.col('Qty') * pl.col('PriceAUD') * 0.65) / 5000)
            .otherwise(0)
            .alias('tonnes'),
            
            # Extract site code
            pl.col('Location').str.split('-').list.last().str.split('_').list.last().alias('site_code'),
            
            # Calculate revenue
            (pl.col('Qty') * pl.col('PriceAUD')).alias('revenue_aud')
        ])

        # Create aggregation DataFrame
        agg_df = merged.select([
            pl.col('version_id'),
            pl.col('tonnes'),
            pl.col('Forecast_Region').alias('forecast_region'),
            pl.col('Customer_code').alias('customer_code'),
            pl.col('Period_AU').alias('period'),
            pl.col('Product').alias('product_id'),
            pl.col('ProductGroupDescription').alias('product_group_description'),
            pl.col('ParentProductGroupDescription').alias('parent_product_group_description'),
            pl.col('site_code').alias('site_id'),
            pl.col('Qty'),
            pl.col('revenue_aud')
        ])

        # Clean product_id: convert to string, strip whitespace, filter out empty
        agg_df = agg_df.with_columns([
            pl.col('product_id').cast(pl.Utf8).str.strip_chars().alias('product_id')
        ]).filter(pl.col('product_id') != '')

        # Get all valid product codes from the DB
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        agg_df = agg_df.filter(pl.col('product_id').is_in(valid_products))

        # Convert to pandas for cost lookups (more efficient for dict operations)
        agg_df_pd = agg_df.to_pandas()
        product_site_cost_df_pd = product_site_cost_df.to_pandas()
        inventory_df_pd = inventory_df.to_pandas()

        # --- FAST LOOKUP DICTS ---
        cost_lookup = {
            (str(row['version_id']), str(row['product_id']), str(row['site_id'])): row['cost_aud']
            for _, row in product_site_cost_df_pd.iterrows()
            if pd.notnull(row['cost_aud'])
        }
        inv_cost_lookup = {
            (str(row['version_id']), str(row['product']), str(row['site_id'])): row['cost_aud']
            for _, row in inventory_df_pd.iterrows()
            if pd.notnull(row['cost_aud'])
        }
        revenue_cost_lookup = {
            (str(row['version_id']), str(row['product_id']), str(row['site_id'])): row['revenue_cost_aud']
            for _, row in product_site_cost_df_pd.iterrows()
            if pd.notnull(row['revenue_cost_aud'])
        }

        def fast_cogs_aud(row):
            key = (str(row['version_id']), str(row['product_id']), str(row['site_id']))
            qty = row.get('Qty', 1)
            cost = cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            cost = inv_cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            cost = revenue_cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            return 0

        agg_df_pd['cogs_aud'] = agg_df_pd.apply(fast_cogs_aud, axis=1)
        for col in ['cogs_aud', 'revenue_aud', 'Qty', 'tonnes']:
            agg_df_pd[col] = pd.to_numeric(agg_df_pd[col], errors='coerce').fillna(0)

        objs = [
            AggregatedForecast(
                version=scenario,
                tonnes=float(row['tonnes']),
                forecast_region=row['forecast_region'],
                customer_code=row['customer_code'],
                period=row['period'],
                product_id=row['product_id'],
                product_group_description=row['product_group_description'],
                parent_product_group_description=row['parent_product_group_description'],
                cogs_aud=float(row['cogs_aud']),
                qty=float(row['Qty']),
                revenue_aud=float(row['revenue_aud']),
            )
            for _, row in agg_df_pd.iterrows()
        ]
        
        self.stdout.write(f"Created {len(objs)} regular forecast objects")
        return objs

    def _process_fixed_plant_forecast_timed(self, forecast_df, product_df, scenario):
        """Process Fixed Plant forecast data using conversion modifiers with detailed timing."""
        sub_start = time.time()
        
        # Get all valid product codes for validation
        valid_start = time.time()
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        valid_time = time.time() - valid_start
        self.stdout.write(f"   🔍 Product validation: {valid_time:.3f}s")
        
        # Merge with product data using inner join to exclude invalid products
        merge_start = time.time()
        merged = forecast_df.join(
            product_df,
            left_on='Product',
            right_on='Product',
            how='inner'  # Only keep records with valid products
        )
        merge_time = time.time() - merge_start
        self.stdout.write(f"   🔗 Data merge: {merge_time:.3f}s")

        if merged.is_empty():
            self.stdout.write("   ⚠️  No valid products found in Fixed Plant forecast data")
            return []

        objs = []
        processed_products = set()
        
        processing_start = time.time()
        for row in merged.iter_rows(named=True):
            try:
                product_code = str(row['Product']).strip()
                
                # Double-check product validation
                if product_code not in valid_products:
                    continue
                
                if row['Qty'] is None or row['Qty'] == 0:
                    continue

                qty = float(row['Qty'])
                
                # Only show debug for first occurrence of each product
                show_debug = product_code not in processed_products
                processed_products.add(product_code)

                # Get product object
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                except MasterDataProductModel.DoesNotExist:
                    if show_debug:
                        self.stdout.write(f"   ⚠️  Warning: Product {product_code} not found in master data")
                    continue

                try:
                    # Get Fixed Plant conversion modifier (no site filtering)
                    modifier = FixedPlantConversionModifiersModel.objects.get(
                        version=scenario,
                        Product=product_obj
                    )

                    # Revenue AUD = Qty
                    revenue_aud = qty

                    # Convert percentages to decimals
                    freight_percentage = (modifier.FreightPercentage or 0.0) / 100.0
                    gross_margin = (modifier.GrossMargin or 0.0) / 100.0
                    external_material_percentage = (modifier.ExternalMaterialComponents or 0.0) / 100.0
                    material_cost_percentage = (modifier.MaterialCostPercentage or 0.0) / 100.0
                    cost_per_sqm_or_kg = modifier.CostPerSQMorKgAUD or 0.0

                    # COGS = (1 - (freight_percentage + gross_margin)) * Qty
                    cogs_aud = (1 - (freight_percentage + gross_margin)) * qty

                    # Tonnes calculation - divide by 1000 to convert KG to tonnes
                    if cost_per_sqm_or_kg > 0:
                        tonnes = (
                            (1 - (freight_percentage + gross_margin)) * 
                            qty * 
                            (1 - external_material_percentage) * 
                            material_cost_percentage
                        ) / cost_per_sqm_or_kg / 1000
                    else:
                        tonnes = 0.0

                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=cogs_aud,
                        qty=qty,
                        revenue_aud=revenue_aud,
                    ))

                except FixedPlantConversionModifiersModel.DoesNotExist:
                    # Create default record with original DressMass logic
                    dress_mass = row['DressMass']
                    if dress_mass is not None and dress_mass != 0:
                        tonnes = qty * dress_mass / 1000
                    else:
                        tonnes = 0
                    
                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=0.0,
                        qty=qty,
                        revenue_aud=qty,
                    ))

            except Exception as e:
                self.stdout.write(f"   ❌ Error processing Fixed Plant record for {product_code}: {e}")
                continue

        processing_time = time.time() - processing_start
        total_time = time.time() - sub_start
        
        self.stdout.write(f"   🏭 Fixed Plant processing: {processing_time:.3f}s")
        self.stdout.write(f"   ⏱️  Fixed Plant total: {total_time:.3f}s ({len(objs)} objects)")
        
        # Summary of tonnes created
        total_tonnes = sum(obj.tonnes for obj in objs)
        self.stdout.write(f"   📊 Fixed Plant tonnes: {total_tonnes:.2f}")
        
        return objs
        """Process Fixed Plant forecast data using conversion modifiers with polars optimization."""
        
        # Get all valid product codes for validation
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        
        # Merge with product data using inner join to exclude invalid products
        merged = forecast_df.join(
            product_df,
            left_on='Product',
            right_on='Product',
            how='inner'  # Only keep records with valid products
        )

        if merged.is_empty():
            self.stdout.write("No valid products found in Fixed Plant forecast data")
            return []

        objs = []
        processed_products = set()
        
        for row in merged.iter_rows(named=True):
            try:
                product_code = str(row['Product']).strip()
                
                # Double-check product validation
                if product_code not in valid_products:
                    continue
                
                if row['Qty'] is None or row['Qty'] == 0:
                    continue

                qty = float(row['Qty'])
                
                # Only show debug for first occurrence of each product
                show_debug = product_code not in processed_products
                processed_products.add(product_code)

                # Get product object
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                except MasterDataProductModel.DoesNotExist:
                    if show_debug:
                        self.stdout.write(f"Warning: Product {product_code} not found in master data")
                    continue

                try:
                    # Get Fixed Plant conversion modifier (no site filtering)
                    modifier = FixedPlantConversionModifiersModel.objects.get(
                        version=scenario,
                        Product=product_obj
                    )

                    if show_debug:
                        self.stdout.write(f"Found Fixed Plant modifier for {product_code}")

                    # Revenue AUD = Qty
                    revenue_aud = qty

                    # Convert percentages to decimals
                    freight_percentage = (modifier.FreightPercentage or 0.0) / 100.0
                    gross_margin = (modifier.GrossMargin or 0.0) / 100.0
                    external_material_percentage = (modifier.ExternalMaterialComponents or 0.0) / 100.0
                    material_cost_percentage = (modifier.MaterialCostPercentage or 0.0) / 100.0
                    cost_per_sqm_or_kg = modifier.CostPerSQMorKgAUD or 0.0

                    # COGS = (1 - (freight_percentage + gross_margin)) * Qty
                    cogs_aud = (1 - (freight_percentage + gross_margin)) * qty

                    # Tonnes calculation - divide by 1000 to convert KG to tonnes
                    if cost_per_sqm_or_kg > 0:
                        tonnes = (
                            (1 - (freight_percentage + gross_margin)) * 
                            qty * 
                            (1 - external_material_percentage) * 
                            material_cost_percentage
                        ) / cost_per_sqm_or_kg / 1000
                    else:
                        tonnes = 0.0

                    if show_debug:
                        self.stdout.write(f"Fixed Plant {product_code}: {tonnes:.4f} tonnes per record (using modifier)")

                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=cogs_aud,
                        qty=qty,
                        revenue_aud=revenue_aud,
                    ))

                except FixedPlantConversionModifiersModel.DoesNotExist:
                    if show_debug:
                        self.stdout.write(f"Fixed Plant {product_code}: No modifier found - using DressMass fallback")
                    
                    # Create default record with original DressMass logic
                    dress_mass = row['DressMass']
                    if dress_mass is not None and dress_mass != 0:
                        tonnes = qty * dress_mass / 1000
                    else:
                        tonnes = 0
                    
                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=0.0,
                        qty=qty,
                        revenue_aud=qty,
                    ))

            except Exception as e:
                self.stdout.write(f"Error processing Fixed Plant record for {product_code}: {e}")
                continue

        self.stdout.write(f"Created {len(objs)} Fixed Plant objects")
        
        # Summary of tonnes created
        total_tonnes = sum(obj.tonnes for obj in objs)
        self.stdout.write(f"Fixed Plant total tonnes: {total_tonnes:.2f}")
        
        return objs

    def _process_revenue_forecast_timed(self, forecast_df, product_df, scenario):
        """Process Revenue Forecast data using conversion modifiers and site allocation with detailed timing."""
        sub_start = time.time()
        
        # Get all valid product codes for validation
        valid_start = time.time()
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        valid_time = time.time() - valid_start
        self.stdout.write(f"   🔍 Product validation: {valid_time:.3f}s")
        
        # Merge with product data using inner join to exclude invalid products
        merge_start = time.time()
        merged = forecast_df.join(
            product_df,
            left_on='Product',
            right_on='Product',
            how='inner'  # Only keep records with valid products
        )
        merge_time = time.time() - merge_start
        self.stdout.write(f"   🔗 Data merge: {merge_time:.3f}s")

        if merged.is_empty():
            self.stdout.write("   ⚠️  No valid products found in Revenue Forecast data")
            return []

        objs = []
        processing_start = time.time()
        
        for row in merged.iter_rows(named=True):
            try:
                product_code = str(row['Product']).strip()
                
                # Double-check product validation
                if product_code not in valid_products:
                    self.stdout.write(f"   ⚠️  Skipping invalid Revenue Forecast product: {product_code}")
                    continue
                
                if row['Qty'] is None or row['Qty'] == 0:
                    continue

                qty = float(row['Qty'])

                # Get product object
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                except MasterDataProductModel.DoesNotExist:
                    self.stdout.write(f"   ⚠️  Warning: Product {product_code} not found in master data")
                    continue

                try:
                    # Step 1: Convert Revenue to COGS and Tonnes
                    conversion_modifier = RevenueToCogsConversionModel.objects.get(
                        version=scenario,
                        Product=product_obj
                    )

                    # Revenue = Qty
                    revenue_aud = qty

                    # Convert percentages to decimals
                    gross_margin = (conversion_modifier.GrossMargin or 0.0) / 100.0
                    inhouse_production = (conversion_modifier.InHouseProduction or 0.0) / 100.0
                    cost_aud_per_kg = conversion_modifier.CostAUDPerKG or 0.0

                    # COGS = qty * (1 - GrossMargin)
                    total_cogs_aud = qty * (1 - gross_margin)

                    # Tonnes = (COGS * inhouse_production) / costAUDPerKG / 1000 (convert KG to tonnes)
                    if cost_aud_per_kg > 0:
                        total_tonnes = (total_cogs_aud * inhouse_production) / cost_aud_per_kg / 1000
                    else:
                        total_tonnes = 0.0

                    # Step 2: Allocate to sites based on allocation percentages
                    site_allocations = SiteAllocationModel.objects.filter(
                        version=scenario,
                        Product=product_obj
                    )

                    if site_allocations.exists():
                        for allocation in site_allocations:
                            allocation_percentage = (allocation.AllocationPercentage or 0.0) / 100.0

                            # Allocate based on percentage
                            allocated_revenue = revenue_aud * allocation_percentage
                            allocated_cogs = total_cogs_aud * allocation_percentage
                            allocated_tonnes = total_tonnes * allocation_percentage
                            allocated_qty = qty * allocation_percentage

                            objs.append(AggregatedForecast(
                                version=scenario,
                                tonnes=allocated_tonnes,
                                forecast_region=row['Forecast_Region'],
                                customer_code=row['Customer_code'],
                                period=row['Period_AU'],
                                product_id=product_code,
                                product_group_description=row['ProductGroupDescription'],
                                parent_product_group_description=row['ParentProductGroupDescription'],
                                cogs_aud=allocated_cogs,
                                qty=allocated_qty,
                                revenue_aud=allocated_revenue,
                            ))
                    else:
                        self.stdout.write(f"   ⚠️  Warning: No site allocation found for product {product_code}")
                        # Create single record with no site allocation
                        objs.append(AggregatedForecast(
                            version=scenario,
                            tonnes=total_tonnes,
                            forecast_region=row['Forecast_Region'],
                            customer_code=row['Customer_code'],
                            period=row['Period_AU'],
                            product_id=product_code,
                            product_group_description=row['ProductGroupDescription'],
                            parent_product_group_description=row['ParentProductGroupDescription'],
                            cogs_aud=total_cogs_aud,
                            qty=qty,
                            revenue_aud=revenue_aud,
                        ))

                except RevenueToCogsConversionModel.DoesNotExist:
                    self.stdout.write(f"   ⚠️  Warning: No revenue conversion modifier found for product {product_code}")
                    # Create default record with original logic
                    dress_mass = row['DressMass']
                    if dress_mass is not None and dress_mass != 0:
                        tonnes = qty * dress_mass / 1000
                    else:
                        tonnes = 0
                    
                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=0.0,
                        qty=qty,
                        revenue_aud=qty,
                    ))

            except Exception as e:
                self.stdout.write(f"   ❌ Error processing Revenue Forecast record: {e}")
                continue

        processing_time = time.time() - processing_start
        total_time = time.time() - sub_start
        
        self.stdout.write(f"   💰 Revenue processing: {processing_time:.3f}s")
        self.stdout.write(f"   ⏱️  Revenue Forecast total: {total_time:.3f}s ({len(objs)} objects)")
        return objs
        """Process Revenue Forecast data using conversion modifiers and site allocation with polars optimization."""
        
        # Get all valid product codes for validation
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        
        # Merge with product data using inner join to exclude invalid products
        merged = forecast_df.join(
            product_df,
            left_on='Product',
            right_on='Product',
            how='inner'  # Only keep records with valid products
        )

        if merged.is_empty():
            self.stdout.write("No valid products found in Revenue Forecast data")
            return []

        objs = []
        for row in merged.iter_rows(named=True):
            try:
                product_code = str(row['Product']).strip()
                
                # Double-check product validation
                if product_code not in valid_products:
                    self.stdout.write(f"Skipping invalid Revenue Forecast product: {product_code}")
                    continue
                
                if row['Qty'] is None or row['Qty'] == 0:
                    continue

                qty = float(row['Qty'])

                # Get product object
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                except MasterDataProductModel.DoesNotExist:
                    self.stdout.write(f"Warning: Product {product_code} not found in master data")
                    continue

                try:
                    # Step 1: Convert Revenue to COGS and Tonnes
                    conversion_modifier = RevenueToCogsConversionModel.objects.get(
                        version=scenario,
                        Product=product_obj
                    )

                    # Revenue = Qty
                    revenue_aud = qty

                    # Convert percentages to decimals
                    gross_margin = (conversion_modifier.GrossMargin or 0.0) / 100.0
                    inhouse_production = (conversion_modifier.InHouseProduction or 0.0) / 100.0
                    cost_aud_per_kg = conversion_modifier.CostAUDPerKG or 0.0

                    # COGS = qty * (1 - GrossMargin)
                    total_cogs_aud = qty * (1 - gross_margin)

                    # Tonnes = (COGS * inhouse_production) / costAUDPerKG / 1000 (convert KG to tonnes)
                    if cost_aud_per_kg > 0:
                        total_tonnes = (total_cogs_aud * inhouse_production) / cost_aud_per_kg / 1000
                    else:
                        total_tonnes = 0.0

                    # Step 2: Allocate to sites based on allocation percentages
                    site_allocations = SiteAllocationModel.objects.filter(
                        version=scenario,
                        Product=product_obj
                    )

                    if site_allocations.exists():
                        for allocation in site_allocations:
                            allocation_percentage = (allocation.AllocationPercentage or 0.0) / 100.0

                            # Allocate based on percentage
                            allocated_revenue = revenue_aud * allocation_percentage
                            allocated_cogs = total_cogs_aud * allocation_percentage
                            allocated_tonnes = total_tonnes * allocation_percentage
                            allocated_qty = qty * allocation_percentage

                            objs.append(AggregatedForecast(
                                version=scenario,
                                tonnes=allocated_tonnes,
                                forecast_region=row['Forecast_Region'],
                                customer_code=row['Customer_code'],
                                period=row['Period_AU'],
                                product_id=product_code,
                                product_group_description=row['ProductGroupDescription'],
                                parent_product_group_description=row['ParentProductGroupDescription'],
                                cogs_aud=allocated_cogs,
                                qty=allocated_qty,
                                revenue_aud=allocated_revenue,
                            ))
                    else:
                        self.stdout.write(f"Warning: No site allocation found for product {product_code}")
                        # Create single record with no site allocation
                        objs.append(AggregatedForecast(
                            version=scenario,
                            tonnes=total_tonnes,
                            forecast_region=row['Forecast_Region'],
                            customer_code=row['Customer_code'],
                            period=row['Period_AU'],
                            product_id=product_code,
                            product_group_description=row['ProductGroupDescription'],
                            parent_product_group_description=row['ParentProductGroupDescription'],
                            cogs_aud=total_cogs_aud,
                            qty=qty,
                            revenue_aud=revenue_aud,
                        ))

                except RevenueToCogsConversionModel.DoesNotExist:
                    self.stdout.write(f"Warning: No revenue conversion modifier found for product {product_code}")
                    # Create default record with original logic
                    dress_mass = row['DressMass']
                    if dress_mass is not None and dress_mass != 0:
                        tonnes = qty * dress_mass / 1000
                    else:
                        tonnes = 0
                    
                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=0.0,
                        qty=qty,
                        revenue_aud=qty,
                    ))

            except Exception as e:
                self.stdout.write(f"Error processing Revenue Forecast record: {e}")
                continue

        self.stdout.write(f"Created {len(objs)} Revenue Forecast objects")
        return objs


    def _validate_products(self, scenario):
        """Validate products and show any data quality issues."""
        # Get all products from SMART_Forecast_Model for this version
        forecast_products = set(
            SMART_Forecast_Model.objects.filter(version=scenario)
            .values_list('Product', flat=True)
            .distinct()
        )

        # Get all valid products from MasterDataProductModel
        valid_products = set(
            MasterDataProductModel.objects.values_list('Product', flat=True)
        )

        # Find invalid products
        invalid_products = forecast_products - valid_products

        self.stdout.write(f"\nProduct validation:")
        self.stdout.write(f"  - Total products in SMART forecast: {len(forecast_products)}")
        self.stdout.write(f"  - Valid products in master data: {len(valid_products)}")
        self.stdout.write(f"  - Invalid products found: {len(invalid_products)}")

        if invalid_products:
            self.stdout.write(self.style.WARNING("\nInvalid products (exist in SMART forecast but not in master data):"))
            for product in sorted(invalid_products):
                count = SMART_Forecast_Model.objects.filter(
                    version=scenario, 
                    Product=product
                ).count()
                self.stdout.write(f"  - '{product}' ({count} records)")
            
            self.stdout.write(self.style.WARNING(f"\nAction required: These {len(invalid_products)} products need to be either:"))
            self.stdout.write("  1. Added to MasterDataProductModel, or")
            self.stdout.write("  2. Removed/corrected in SMART_Forecast_Model")
        else:
            self.stdout.write(self.style.SUCCESS("  ✓ All products in SMART forecast are valid!"))

        # Check for products with zero tonnes
        zero_tonnes_count = AggregatedForecast.objects.filter(version=scenario, tonnes=0).count()
        if zero_tonnes_count > 0:
            self.stdout.write(self.style.WARNING(f"\nWarning: {zero_tonnes_count} records have zero tonnes"))
            
            # Show breakdown by product
            from django.db.models import Count
            zero_by_product = (
                AggregatedForecast.objects
                .filter(version=scenario, tonnes=0)
                .values('product_id')
                .annotate(count=Count('id'))
                .order_by('-count')[:5]
            )
            
            if zero_by_product:
                self.stdout.write("  Top products with zero tonnes:")
                for item in zero_by_product:
                    self.stdout.write(f"    - {item['product_id']}: {item['count']} records")

        # Show Fixed Plant specific issues
        self._validate_fixed_plant_modifiers(scenario)

    def _validate_fixed_plant_modifiers(self, scenario):
        """Validate Fixed Plant conversion modifiers."""
        # Get all Fixed Plant products from SMART forecast
        fixed_plant_products = set(
            SMART_Forecast_Model.objects.filter(
                version=scenario,
                Data_Source='Fixed Plant'
            ).values_list('Product', flat=True).distinct()
        )

        if not fixed_plant_products:
            return

        self.stdout.write(f"\nFixed Plant validation:")
        self.stdout.write(f"  - Fixed Plant products in forecast: {len(fixed_plant_products)}")

        # Check which products have conversion modifiers (no site filtering)
        products_with_modifiers = set(
            FixedPlantConversionModifiersModel.objects.filter(
                version=scenario
            ).values_list('Product__Product', flat=True)
        )

        # Check missing modifiers
        missing_modifiers = fixed_plant_products - products_with_modifiers

        if missing_modifiers:
            self.stdout.write(self.style.WARNING("  - Missing Fixed Plant conversion modifiers:"))
            for product in sorted(missing_modifiers):
                self.stdout.write(f"    - {product}")
            self.stdout.write("  - These products will use DressMass fallback calculation")
        else:
            self.stdout.write(self.style.SUCCESS("  ✓ All Fixed Plant products have conversion modifiers!"))

        # Show Revenue Forecast validation
        self._validate_revenue_forecast_modifiers(scenario)

    def _validate_revenue_forecast_modifiers(self, scenario):
        """Validate Revenue Forecast conversion modifiers."""
        # Get all Revenue Forecast products from SMART forecast
        revenue_products = set(
            SMART_Forecast_Model.objects.filter(
                version=scenario,
                Data_Source='Revenue Forecast'
            ).values_list('Product', flat=True).distinct()
        )

        if not revenue_products:
            return

        self.stdout.write(f"\nRevenue Forecast validation:")
        self.stdout.write(f"  - Revenue Forecast products in forecast: {len(revenue_products)}")

        # Check which products have conversion modifiers
        products_with_revenue_modifiers = set(
            RevenueToCogsConversionModel.objects.filter(
                version=scenario
            ).values_list('Product__Product', flat=True)
        )

        # Check which products have site allocations
        products_with_allocations = set(
            SiteAllocationModel.objects.filter(
                version=scenario
            ).values_list('Product__Product', flat=True)
        )

        # Check missing modifiers
        missing_revenue_modifiers = revenue_products - products_with_revenue_modifiers
        missing_allocations = revenue_products - products_with_allocations

        if missing_revenue_modifiers:
            self.stdout.write(self.style.WARNING("  - Missing Revenue-to-COGS conversion modifiers:"))
            for product in sorted(missing_revenue_modifiers):
                self.stdout.write(f"    - {product}")

        if missing_allocations:
            self.stdout.write(self.style.WARNING("  - Missing site allocations:"))
            for product in sorted(missing_allocations):
                self.stdout.write(f"    - {product}")

        if not missing_revenue_modifiers and not missing_allocations:
            self.stdout.write(self.style.SUCCESS("  ✓ All Revenue Forecast products have conversion modifiers and site allocations!"))
