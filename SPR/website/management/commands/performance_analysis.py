"""
Performance Analysis Management Command for calculate_model
This command measures the execution time of each step in the calculate_model process
and provides recommendations for performance improvements.
"""

import time
import gc
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connection, reset_queries
from django.conf import settings

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from website.models import scenarios
from website.management.commands.populate_aggregated_forecast import Command as AggForecastCommand
from website.management.commands.populate_calculated_replenishment_v2 import Command as ReplenishmentCommand
from website.management.commands.populate_calculated_production import Command as ProductionCommand
from website.customized_function import populate_all_aggregated_data

class PerformanceTimer:
    def __init__(self, name):
        self.name = name
        self.start_time = None
        self.start_memory = None
        self.start_queries = len(connection.queries) if settings.DEBUG else 0

    def __enter__(self):
        gc.collect()  # Clean up memory before measurement
        self.start_time = time.time()
        if PSUTIL_AVAILABLE:
            self.start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        self.start_queries = len(connection.queries) if settings.DEBUG else 0
        reset_queries()
        print(f"\n🔄 Starting: {self.name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.time()
        if PSUTIL_AVAILABLE:
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        end_queries = len(connection.queries) if settings.DEBUG else 0
        
        duration = end_time - self.start_time
        if PSUTIL_AVAILABLE:
            memory_used = end_memory - self.start_memory
        queries_count = end_queries
        
        print(f"✅ Completed: {self.name}")
        print(f"   ⏱️  Duration: {duration:.2f} seconds")
        if PSUTIL_AVAILABLE:
            print(f"   💾 Memory used: {memory_used:.1f} MB")
        print(f"   🗄️  Database queries: {queries_count}")
        
        if duration > 60:
            print(f"   ⚠️  SLOW PROCESS - {duration/60:.1f} minutes")
        
        if queries_count > 100:
            print(f"   ⚠️  HIGH QUERY COUNT - Consider optimization")
            
        return False

class Command(BaseCommand):
    help = 'Performance analysis of calculate_model process'

    def add_arguments(self, parser):
        parser.add_argument(
            '--scenario',
            type=str,
            default='Jul 25 SPR',
            help="The scenario version to analyze (default: 'Jul 25 SPR')",
        )
        parser.add_argument(
            '--detailed',
            action='store_true',
            help="Show detailed query analysis",
        )
        parser.add_argument(
            '--skip-steps',
            nargs='*',
            choices=['forecast', 'replenishment', 'production', 'aggregated', 'cache'],
            help="Skip specific steps for targeted analysis",
        )

    def handle(self, *args, **options):
        scenario_version = options['scenario']
        detailed = options['detailed']
        skip_steps = options.get('skip_steps', [])
        
        print("="*80)
        print("🚀 PERFORMANCE ANALYSIS: calculate_model")
        print("="*80)
        print(f"📊 Scenario: {scenario_version}")
        print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔧 Django DEBUG mode: {settings.DEBUG}")
        if PSUTIL_AVAILABLE:
            print(f"💻 Available memory: {psutil.virtual_memory().available / 1024 / 1024 / 1024:.1f} GB")
        
        try:
            scenario = scenarios.objects.get(version=scenario_version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario version '{scenario_version}' not found."))
            return

        total_start_time = time.time()
        step_times = {}
        
        # Step 1: Aggregated Forecast
        if 'forecast' not in skip_steps:
            with PerformanceTimer("populate_aggregated_forecast") as timer:
                AggForecastCommand().handle(version=scenario_version)
            step_times['forecast'] = time.time() - timer.start_time

        # Step 2: Calculated Replenishment
        if 'replenishment' not in skip_steps:
            with PerformanceTimer("populate_calculated_replenishment_v2") as timer:
                ReplenishmentCommand().handle(version=scenario_version)
            step_times['replenishment'] = time.time() - timer.start_time

        # Step 3: Calculated Production
        if 'production' not in skip_steps:
            with PerformanceTimer("populate_calculated_production") as timer:
                ProductionCommand().handle(scenario_version=scenario_version)
            step_times['production'] = time.time() - timer.start_time

        # Step 4: Aggregated Data
        if 'aggregated' not in skip_steps:
            with PerformanceTimer("populate_all_aggregated_data") as timer:
                populate_all_aggregated_data(scenario)
            step_times['aggregated'] = time.time() - timer.start_time

        # Step 5: Cache Review Data (simulate)
        if 'cache' not in skip_steps:
            with PerformanceTimer("cache_review_data (estimation)") as timer:
                from django.db import connection
                with connection.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM website_calculatedproductionmodel WHERE version_id = %s", [scenario.id])
                    result = cursor.fetchone()
                    print(f"   📈 Production records created: {result[0]:,}")
            step_times['cache'] = time.time() - timer.start_time

        total_duration = time.time() - total_start_time
        
        print("\n" + "="*80)
        print("📊 PERFORMANCE SUMMARY")
        print("="*80)
        print(f"🕐 Total execution time: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
        
        # Show step breakdown
        print("\n📊 STEP BREAKDOWN:")
        print("-" * 40)
        for step, duration in sorted(step_times.items(), key=lambda x: x[1], reverse=True):
            percentage = (duration / total_duration) * 100
            print(f"   {step:20}: {duration:6.2f}s ({percentage:5.1f}%)")
        
        # Database analysis
        self.analyze_database_performance(scenario, detailed)
        
        # Memory analysis
        if PSUTIL_AVAILABLE:
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            print(f"💾 Current memory usage: {current_memory:.1f} MB")
        
        # Recommendations
        self.provide_recommendations(total_duration, step_times)

    def analyze_database_performance(self, scenario, detailed=False):
        print("\n📊 DATABASE ANALYSIS")
        print("-" * 40)
        
        # Count records in each table
        from website.models import (
            SMART_Forecast_Model, AggregatedForecast, 
            CalcualtedReplenishmentModel, CalculatedProductionModel
        )
        
        smart_count = SMART_Forecast_Model.objects.filter(version=scenario).count()
        agg_count = AggregatedForecast.objects.filter(version=scenario).count()
        replen_count = CalcualtedReplenishmentModel.objects.filter(version=scenario).count()
        prod_count = CalculatedProductionModel.objects.filter(version=scenario).count()
        
        print(f"📋 SMART Forecast records: {smart_count:,}")
        print(f"📋 Aggregated Forecast records: {agg_count:,}")
        print(f"📋 Replenishment records: {replen_count:,}")
        print(f"📋 Production records: {prod_count:,}")
        
        # Calculate data ratios
        if smart_count > 0:
            agg_ratio = (agg_count / smart_count) * 100
            replen_ratio = (replen_count / smart_count) * 100
            prod_ratio = (prod_count / smart_count) * 100
            print(f"\n📊 Data processing ratios:")
            print(f"   Aggregated/SMART: {agg_ratio:.1f}%")
            print(f"   Replenishment/SMART: {replen_ratio:.1f}%")
            print(f"   Production/SMART: {prod_ratio:.1f}%")

    def provide_recommendations(self, total_duration, step_times):
        print("\n🎯 PERFORMANCE RECOMMENDATIONS")
        print("-" * 40)
        
        # Identify bottleneck step
        if step_times:
            slowest_step = max(step_times.items(), key=lambda x: x[1])
            print(f"🐌 Slowest step: {slowest_step[0]} ({slowest_step[1]:.2f}s)")
        
        if total_duration > 300:  # 5 minutes
            print("⚠️  CRITICAL: Process is very slow (>5 minutes)")
            print("   💡 Consider implementing:")
            print("      1. Parallel processing for independent operations")
            print("      2. Database connection pooling")
            print("      3. Batch size optimization")
            print("      4. Memory-mapped file operations")
            
        elif total_duration > 120:  # 2 minutes
            print("⚠️  WARNING: Process is slow (>2 minutes)")
            
        print("\n🔧 SPECIFIC OPTIMIZATIONS:")
        
        # Database optimization recommendations
        print("\n1. DATABASE OPTIMIZATIONS:")
        print("   📊 Ensure these indexes exist:")
        print("      CREATE INDEX idx_smart_forecast_version_data_source ON website_smart_forecast_model(version_id, Data_Source);")
        print("      CREATE INDEX idx_inventory_version_product_site ON website_masterdatainventory(version_id, product, site_id);")
        print("      CREATE INDEX idx_replenishment_version_product ON website_calcualtedreplenishmentmodel(version_id, Product_id);")
        print("      CREATE INDEX idx_production_version_product ON website_calculatedproductionmodel(version_id, product_id);")
        
        print("\n2. POLARS OPTIMIZATIONS:")
        print("   🚀 Already implemented:")
        print("      ✅ DataFrame operations using polars")
        print("      ✅ Bulk database operations")
        print("      ✅ Memory-efficient data loading")
        
        print("\n3. ALGORITHM OPTIMIZATIONS:")
        print("   ⚡ For further improvement:")
        print("      • Implement lazy loading with streaming")
        print("      • Use polars lazy evaluation (.lazy())")
        print("      • Consider parallel processing with multiprocessing")
        print("      • Cache frequently accessed lookup data")
        
        # Step-specific recommendations
        if step_times:
            print("\n4. STEP-SPECIFIC RECOMMENDATIONS:")
            for step, duration in step_times.items():
                if duration > 60:  # More than 1 minute
                    if step == 'forecast':
                        print(f"   📊 {step}: Consider caching product master data")
                    elif step == 'replenishment':
                        print(f"   📊 {step}: Optimize site extraction and validation")
                    elif step == 'production':
                        print(f"   📊 {step}: Vectorize inventory calculations")
                    elif step == 'aggregated':
                        print(f"   📊 {step}: Consider pre-computed aggregations")

        print("\n5. MONITORING:")
        print("   📈 Run this analysis regularly to track improvements:")
        print(f"      python manage.py performance_analysis --scenario '{total_duration}'")
        print("   📈 Use --skip-steps to focus on specific bottlenecks")
        print("   📈 Use --detailed for query analysis when DEBUG=True")
