"""
Advanced Performance Optimization for Calculate Model
Addresses specific bottlenecks identified in timing analysis
"""

import time
import gc
import polars as pl
import pandas as pd
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.db.models import Q
from website.models import (
    Smart_Forecast_Model, MasterDataInventory, 
    CalcualtedReplenishmentModel, CalculatedProductionModel,
    CalculatedAggregatedForecast, ProductSiteCostModel,
    MasterDataCastToDespatchModel
)

class Command(BaseCommand):
    help = 'Optimized calculate_model with advanced performance techniques'

    def add_arguments(self, parser):
        parser.add_argument(
            '--scenario',
            type=str,
            required=True,
            help="Scenario version to process",
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=5000,
            help="Batch size for bulk operations",
        )
        parser.add_argument(
            '--parallel',
            action='store_true',
            help="Enable parallel processing where possible",
        )
        parser.add_argument(
            '--skip-cleanup',
            action='store_true',
            help="Skip data cleanup to save time",
        )

    def handle(self, *args, **options):
        scenario = options['scenario']
        batch_size = options['batch_size']
        use_parallel = options['parallel']
        skip_cleanup = options['skip_cleanup']
        
        print("="*70)
        print("🚀 ADVANCED PERFORMANCE CALCULATE MODEL")
        print("="*70)
        print(f"📊 Scenario: {scenario}")
        print(f"📦 Batch Size: {batch_size:,}")
        print(f"⚡ Parallel: {'Enabled' if use_parallel else 'Disabled'}")
        print("="*70)
        
        total_start = time.time()
        
        try:
            # Step 1: Optimized Data Cleanup
            if not skip_cleanup:
                self.optimized_cleanup(scenario)
            
            # Step 2: Pre-load Master Data
            master_data = self.preload_master_data(scenario)
            
            # Step 3: Optimized Aggregated Forecast
            self.optimized_aggregated_forecast(scenario, master_data, batch_size)
            
            # Step 4: Optimized Replenishment (Biggest Bottleneck)
            self.optimized_replenishment(scenario, master_data, batch_size, use_parallel)
            
            # Step 5: Optimized Production
            self.optimized_production(scenario, master_data, batch_size)
            
            # Step 6: Final Aggregation
            self.optimized_final_aggregation(scenario, master_data)
            
            total_time = time.time() - total_start
            print(f"\n🎉 OPTIMIZATION COMPLETE: {total_time:.1f}s total")
            
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
            raise

    def optimized_cleanup(self, scenario):
        """Ultra-fast cleanup using raw SQL"""
        print("\n🧹 Optimized Cleanup...")
        start_time = time.time()
        
        version_obj = Smart_Forecast_Model.objects.filter(version=scenario).first()
        if not version_obj:
            raise ValueError(f"Scenario '{scenario}' not found")
        
        # Use raw SQL for faster cleanup
        with connection.cursor() as cursor:
            # Get counts first
            cursor.execute("SELECT COUNT(*) FROM website_calculatedaggregatedforecast WHERE version_id = %s", [version_obj.id])
            agg_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM website_calcualtedreplenishmentmodel WHERE version_id = %s", [version_obj.id])
            rep_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM website_calculatedproductionmodel WHERE version_id = %s", [version_obj.id])
            prod_count = cursor.fetchone()[0]
            
            print(f"   🗑️  Deleting {agg_count:,} aggregated, {rep_count:,} replenishment, {prod_count:,} production records...")
            
            # Fast delete with minimal logging
            cursor.execute("DELETE FROM website_calculatedaggregatedforecast WHERE version_id = %s", [version_obj.id])
            cursor.execute("DELETE FROM website_calcualtedreplenishmentmodel WHERE version_id = %s", [version_obj.id])
            cursor.execute("DELETE FROM website_calculatedproductionmodel WHERE version_id = %s", [version_obj.id])
        
        cleanup_time = time.time() - start_time
        print(f"   ✅ Cleanup completed in {cleanup_time:.1f}s")
        
        # Force garbage collection
        gc.collect()

    def preload_master_data(self, scenario):
        """Pre-load all master data into memory for fast lookups"""
        print("\n📚 Pre-loading Master Data...")
        start_time = time.time()
        
        version_obj = Smart_Forecast_Model.objects.filter(version=scenario).first()
        
        master_data = {}
        
        # Load inventory data
        print("   📦 Loading inventory data...")
        inventory_sql = """
        SELECT product, site_id, sum_total_stock, sum_wip, cost
        FROM website_masterdatainventory 
        WHERE version_id = %s
        """
        inventory_df = pl.from_pandas(pd.read_sql(inventory_sql, connection, params=[version_obj.id]))
        master_data['inventory'] = inventory_df
        print(f"      📊 {len(inventory_df):,} inventory records")
        
        # Load product costs
        print("   💰 Loading cost data...")
        cost_sql = """
        SELECT product_id, site_id, cost, version_id
        FROM website_productsitecostmodel 
        WHERE version_id = %s
        """
        cost_df = pl.from_pandas(pd.read_sql(cost_sql, connection, params=[version_obj.id]))
        master_data['costs'] = cost_df
        print(f"      💰 {len(cost_df):,} cost records")
        
        # Load cast to despatch data
        print("   🏭 Loading cast to despatch data...")
        cast_sql = """
        SELECT product_group, days, version_id
        FROM website_masterdatacasttodespatchmodel 
        WHERE version_id = %s
        """
        cast_df = pl.from_pandas(pd.read_sql(cast_sql, connection, params=[version_obj.id]))
        master_data['cast_despatch'] = cast_df
        print(f"      🏭 {len(cast_df):,} cast to despatch records")
        
        # Store version info
        master_data['version_id'] = version_obj.id
        master_data['version_obj'] = version_obj
        
        preload_time = time.time() - start_time
        print(f"   ✅ Master data loaded in {preload_time:.1f}s")
        
        return master_data

    def optimized_aggregated_forecast(self, scenario, master_data, batch_size):
        """Optimized aggregated forecast with polars"""
        print("\n📈 Optimized Aggregated Forecast...")
        start_time = time.time()
        
        version_id = master_data['version_id']
        
        # Use optimized SQL query with zero filtering for better performance
        forecast_sql = """
        SELECT Product, 
               SUM(CASE WHEN Data_Source = 'Feb Forecast' THEN "Feb 25" ELSE 0 END) as feb_25,
               SUM(CASE WHEN Data_Source = 'Feb Forecast' THEN "Mar 25" ELSE 0 END) as mar_25,
               SUM(CASE WHEN Data_Source = 'Feb Forecast' THEN "Apr 25" ELSE 0 END) as apr_25,
               SUM(CASE WHEN Data_Source = 'Fixed Plant' THEN "Feb 25" ELSE 0 END) as fixed_feb_25,
               SUM(CASE WHEN Data_Source = 'Fixed Plant' THEN "Mar 25" ELSE 0 END) as fixed_mar_25,
               SUM(CASE WHEN Data_Source = 'Fixed Plant' THEN "Apr 25" ELSE 0 END) as fixed_apr_25
        FROM website_smart_forecast_model 
        WHERE version_id = %s 
        AND Data_Source IN ('Feb Forecast', 'Fixed Plant')
        AND (Qty IS NOT NULL AND Qty > 0)
        GROUP BY Product
        """
        
        forecast_df = pl.from_pandas(pd.read_sql(forecast_sql, connection, params=[version_id]))
        print(f"   📊 Processing {len(forecast_df):,} products")
        
        # Fast polars calculations
        aggregated_df = forecast_df.with_columns([
            (pl.col("feb_25") + pl.col("fixed_feb_25")).alias("total_feb_25"),
            (pl.col("mar_25") + pl.col("fixed_mar_25")).alias("total_mar_25"),
            (pl.col("apr_25") + pl.col("fixed_apr_25")).alias("total_apr_25")
        ])
        
        # Bulk insert with batching
        records = []
        for row in aggregated_df.iter_rows(named=True):
            records.append(CalculatedAggregatedForecast(
                version=master_data['version_obj'],
                Product=row['Product'],
                feb_25=row['total_feb_25'],
                mar_25=row['total_mar_25'],
                apr_25=row['total_apr_25']
            ))
            
            if len(records) >= batch_size:
                CalculatedAggregatedForecast.objects.bulk_create(records, batch_size=batch_size)
                records = []
                print(f"      💾 Saved batch of {batch_size:,} records")
        
        # Save remaining records
        if records:
            CalculatedAggregatedForecast.objects.bulk_create(records, batch_size=batch_size)
            print(f"      💾 Saved final batch of {len(records):,} records")
        
        agg_time = time.time() - start_time
        print(f"   ✅ Aggregated forecast completed in {agg_time:.1f}s")

    def optimized_replenishment(self, scenario, master_data, batch_size, use_parallel):
        """Heavily optimized replenishment processing"""
        print("\n🔄 Optimized Replenishment (Main Bottleneck)...")
        start_time = time.time()
        
        version_id = master_data['version_id']
        inventory_df = master_data['inventory']
        
        # Get forecast data with optimized query filtering zeros for performance
        print("   📊 Loading forecast data...")
        forecast_sql = """
        SELECT Product, Site, "Feb 25", "Mar 25", "Apr 25", "May 25", "Jun 25", "Jul 25"
        FROM website_smart_forecast_model 
        WHERE version_id = %s 
        AND Data_Source = 'Feb Forecast'
        AND (Qty IS NOT NULL AND Qty > 0)
        """
        
        forecast_df = pl.from_pandas(pd.read_sql(forecast_sql, connection, params=[version_id]))
        print(f"      📈 {len(forecast_df):,} forecast records loaded")
        
        # Pre-create site lookup
        print("   🏭 Creating site lookup...")
        site_lookup = {site_name: site_id for site_id, site_name in [
            (1, "LINYI"), (2, "ULSAN"), (3, "LONGKOU"), (4, "KUNSHAN"), (5, "THAILAND")
        ]}
        
        # Process in optimized batches
        print("   ⚡ Processing replenishment calculations...")
        
        # Create product-site combinations for faster processing
        product_sites = forecast_df.select([
            pl.col("Product"),
            pl.col("Site").map_elements(lambda x: site_lookup.get(x, 1))
        ]).unique()
        
        print(f"      🔧 Processing {len(product_sites):,} product-site combinations")
        
        records = []
        processed = 0
        
        for batch_start in range(0, len(forecast_df), batch_size):
            batch_end = min(batch_start + batch_size, len(forecast_df))
            batch_df = forecast_df[batch_start:batch_end]
            
            # Fast join with inventory
            batch_with_inventory = batch_df.join(
                inventory_df.select(['product', 'site_id', 'sum_total_stock', 'sum_wip']),
                left_on=['Product', 'Site'],
                right_on=['product', 'site_id'],
                how='left'
            ).fill_null(0)
            
            # Calculate replenishment needs
            replenishment_df = batch_with_inventory.with_columns([
                (pl.col("Feb 25") - pl.col("sum_total_stock") - pl.col("sum_wip")).clip(0, None).alias("feb_replenishment"),
                (pl.col("Mar 25") - pl.col("sum_total_stock") - pl.col("sum_wip")).clip(0, None).alias("mar_replenishment"),
                (pl.col("Apr 25") - pl.col("sum_total_stock") - pl.col("sum_wip")).clip(0, None).alias("apr_replenishment"),
                (pl.col("May 25") - pl.col("sum_total_stock") - pl.col("sum_wip")).clip(0, None).alias("may_replenishment"),
                (pl.col("Jun 25") - pl.col("sum_total_stock") - pl.col("sum_wip")).clip(0, None).alias("jun_replenishment"),
                (pl.col("Jul 25") - pl.col("sum_total_stock") - pl.col("sum_wip")).clip(0, None).alias("jul_replenishment")
            ])
            
            # Create records for bulk insert
            for row in replenishment_df.iter_rows(named=True):
                site_id = site_lookup.get(row['Site'], 1)
                records.append(CalcualtedReplenishmentModel(
                    version=master_data['version_obj'],
                    Product_id=row['Product'],
                    Site_id=site_id,
                    Feb_25=row['feb_replenishment'],
                    Mar_25=row['mar_replenishment'],
                    Apr_25=row['apr_replenishment'],
                    May_25=row['may_replenishment'],
                    Jun_25=row['jun_replenishment'],
                    Jul_25=row['jul_replenishment']
                ))
            
            processed += len(batch_df)
            
            # Bulk save when batch is full
            if len(records) >= batch_size:
                CalcualtedReplenishmentModel.objects.bulk_create(records, batch_size=batch_size)
                print(f"      💾 Saved {len(records):,} records ({processed:,}/{len(forecast_df):,} processed)")
                records = []
                
                # Memory management
                gc.collect()
        
        # Save remaining records
        if records:
            CalcualtedReplenishmentModel.objects.bulk_create(records, batch_size=batch_size)
            print(f"      💾 Saved final {len(records):,} records")
        
        replenishment_time = time.time() - start_time
        print(f"   ✅ Replenishment completed in {replenishment_time:.1f}s")

    def optimized_production(self, scenario, master_data, batch_size):
        """Optimized production calculation"""
        print("\n🏭 Optimized Production...")
        start_time = time.time()
        
        version_id = master_data['version_id']
        inventory_df = master_data['inventory']
        cost_df = master_data['costs']
        
        # Load replenishment data
        replenishment_sql = """
        SELECT Product_id, Site_id, Feb_25, Mar_25, Apr_25, May_25, Jun_25, Jul_25
        FROM website_calcualtedreplenishmentmodel 
        WHERE version_id = %s
        """
        
        replenishment_df = pl.from_pandas(pd.read_sql(replenishment_sql, connection, params=[version_id]))
        print(f"   📊 Processing {len(replenishment_df):,} replenishment records")
        
        # Join with costs for faster processing
        production_df = replenishment_df.join(
            cost_df.select(['product_id', 'site_id', 'cost']),
            left_on=['Product_id', 'Site_id'],
            right_on=['product_id', 'site_id'],
            how='left'
        ).fill_null({'cost': 0})
        
        # Calculate production costs
        production_df = production_df.with_columns([
            (pl.col("Feb_25") * pl.col("cost")).alias("feb_cost"),
            (pl.col("Mar_25") * pl.col("cost")).alias("mar_cost"),
            (pl.col("Apr_25") * pl.col("cost")).alias("apr_cost"),
            (pl.col("May_25") * pl.col("cost")).alias("may_cost"),
            (pl.col("Jun_25") * pl.col("cost")).alias("jun_cost"),
            (pl.col("Jul_25") * pl.col("cost")).alias("jul_cost")
        ])
        
        # Bulk insert production records
        records = []
        for row in production_df.iter_rows(named=True):
            records.append(CalculatedProductionModel(
                version=master_data['version_obj'],
                product_id=row['Product_id'],
                site_id=row['Site_id'],
                Feb_25_tonnes=row['Feb_25'],
                Mar_25_tonnes=row['Mar_25'],
                Apr_25_tonnes=row['Apr_25'],
                May_25_tonnes=row['May_25'],
                Jun_25_tonnes=row['Jun_25'],
                Jul_25_tonnes=row['Jul_25'],
                Feb_25_cost=row['feb_cost'],
                Mar_25_cost=row['mar_cost'],
                Apr_25_cost=row['apr_cost'],
                May_25_cost=row['may_cost'],
                Jun_25_cost=row['jun_cost'],
                Jul_25_cost=row['jul_cost']
            ))
            
            if len(records) >= batch_size:
                CalculatedProductionModel.objects.bulk_create(records, batch_size=batch_size)
                records = []
                print(f"      💾 Saved batch of {batch_size:,} records")
        
        # Save remaining records
        if records:
            CalculatedProductionModel.objects.bulk_create(records, batch_size=batch_size)
        
        production_time = time.time() - start_time
        print(f"   ✅ Production completed in {production_time:.1f}s")

    def optimized_final_aggregation(self, scenario, master_data):
        """Quick final aggregation and reporting"""
        print("\n📊 Final Aggregation...")
        start_time = time.time()
        
        version_id = master_data['version_id']
        
        with connection.cursor() as cursor:
            # Get totals using SQL for speed
            cursor.execute("""
                SELECT 
                    COUNT(*) as record_count,
                    SUM(Feb_25_tonnes + Mar_25_tonnes + Apr_25_tonnes + 
                        May_25_tonnes + Jun_25_tonnes + Jul_25_tonnes) as total_tonnes,
                    SUM(Feb_25_cost + Mar_25_cost + Apr_25_cost + 
                        May_25_cost + Jun_25_cost + Jul_25_cost) as total_cost
                FROM website_calculatedproductionmodel 
                WHERE version_id = %s
            """, [version_id])
            
            result = cursor.fetchone()
            record_count, total_tonnes, total_cost = result
        
        final_time = time.time() - start_time
        
        print(f"   ✅ Final aggregation completed in {final_time:.1f}s")
        print(f"\n🎯 FINAL RESULTS:")
        print(f"   📊 Records: {record_count:,}")
        print(f"   ⚖️  Total Tonnes: {total_tonnes:,.1f}")
        print(f"   💰 Total Cost: ${total_cost:,.2f}")
