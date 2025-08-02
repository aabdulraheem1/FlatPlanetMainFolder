#!/usr/bin/env python
"""
Performance Analysis Script for calculate_model
This script measures the execution time of each step in the calculate_model process
and provides recommendations for performance improvements.
"""

import os
import sys
import time
import psutil
import gc
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connection, reset_queries
from django.conf import settings

# Add Django project to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')

import django
django.setup()

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
        self.start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        self.start_queries = len(connection.queries) if settings.DEBUG else 0
        reset_queries()
        print(f"\n🔄 Starting: {self.name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        end_queries = len(connection.queries) if settings.DEBUG else 0
        
        duration = end_time - self.start_time
        memory_used = end_memory - self.start_memory
        queries_count = end_queries
        
        print(f"✅ Completed: {self.name}")
        print(f"   ⏱️  Duration: {duration:.2f} seconds")
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

    def handle(self, *args, **options):
        scenario_version = options['scenario']
        detailed = options['detailed']
        
        print("="*80)
        print("🚀 PERFORMANCE ANALYSIS: calculate_model")
        print("="*80)
        print(f"📊 Scenario: {scenario_version}")
        print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔧 Django DEBUG mode: {settings.DEBUG}")
        print(f"💻 Available memory: {psutil.virtual_memory().available / 1024 / 1024 / 1024:.1f} GB")
        
        try:
            scenario = scenarios.objects.get(version=scenario_version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario version '{scenario_version}' not found."))
            return

        total_start_time = time.time()
        
        # Step 1: Aggregated Forecast
        with PerformanceTimer("populate_aggregated_forecast"):
            AggForecastCommand().handle(version=scenario_version)

        # Step 2: Calculated Replenishment
        with PerformanceTimer("populate_calculated_replenishment_v2"):
            ReplenishmentCommand().handle(version=scenario_version)

        # Step 3: Calculated Production
        with PerformanceTimer("populate_calculated_production"):
            ProductionCommand().handle(scenario_version=scenario_version)

        # Step 4: Aggregated Data
        with PerformanceTimer("populate_all_aggregated_data"):
            populate_all_aggregated_data(scenario)

        # Step 5: Cache Review Data (simulate subprocess call timing)
        with PerformanceTimer("cache_review_data (simulation)"):
            # We'll just measure a sample query to simulate this step
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM website_calculatedproductionmodel WHERE version_id = %s", [scenario.id])
                result = cursor.fetchone()
                print(f"   📈 Production records created: {result[0]:,}")

        total_duration = time.time() - total_start_time
        
        print("\n" + "="*80)
        print("📊 PERFORMANCE SUMMARY")
        print("="*80)
        print(f"🕐 Total execution time: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
        
        # Database analysis
        self.analyze_database_performance(scenario, detailed)
        
        # Memory analysis
        current_memory = psutil.Process().memory_info().rss / 1024 / 1024
        print(f"💾 Current memory usage: {current_memory:.1f} MB")
        
        # Recommendations
        self.provide_recommendations(total_duration)

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
        
        # Analyze query patterns if detailed mode
        if detailed and settings.DEBUG:
            print("\n🔍 QUERY ANALYSIS")
            print("-" * 40)
            
            # Group queries by type
            query_patterns = {}
            for query in connection.queries[-50:]:  # Last 50 queries
                sql = query['sql']
                if 'SELECT' in sql:
                    if 'JOIN' in sql:
                        pattern = 'SELECT with JOIN'
                    elif 'WHERE' in sql:
                        pattern = 'SELECT with WHERE'
                    else:
                        pattern = 'Simple SELECT'
                elif 'INSERT' in sql:
                    pattern = 'INSERT'
                elif 'UPDATE' in sql:
                    pattern = 'UPDATE'
                elif 'DELETE' in sql:
                    pattern = 'DELETE'
                else:
                    pattern = 'Other'
                
                query_patterns[pattern] = query_patterns.get(pattern, 0) + 1
            
            for pattern, count in sorted(query_patterns.items(), key=lambda x: x[1], reverse=True):
                print(f"   {pattern}: {count} queries")

    def provide_recommendations(self, total_duration):
        print("\n🎯 PERFORMANCE RECOMMENDATIONS")
        print("-" * 40)
        
        if total_duration > 300:  # 5 minutes
            print("⚠️  CRITICAL: Process is very slow (>5 minutes)")
            print("   💡 Consider implementing:")
            print("      1. Parallel processing for independent operations")
            print("      2. Database connection pooling")
            print("      3. Batch size optimization")
            print("      4. Memory-mapped file operations")
            
        elif total_duration > 120:  # 2 minutes
            print("⚠️  WARNING: Process is slow (>2 minutes)")
            print("   💡 Recommended optimizations:")
            
        print("\n🔧 SPECIFIC OPTIMIZATIONS:")
        
        # Database optimization recommendations
        print("\n1. DATABASE OPTIMIZATIONS:")
        print("   📊 Verify indexes are created:")
        print("      CREATE INDEX idx_smart_forecast_version_data_source ON website_smart_forecast_model(version_id, Data_Source);")
        print("      CREATE INDEX idx_inventory_version_product_site ON website_masterdatainventory(version_id, product, site_id);")
        print("      CREATE INDEX idx_replenishment_version_product ON website_calcualtedreplenishmentmodel(version_id, Product_id);")
        
        print("\n2. POLARS OPTIMIZATIONS:")
        print("   🚀 Already implemented:")
        print("      ✅ DataFrame operations using polars")
        print("      ✅ Bulk database operations")
        print("      ✅ Memory-efficient data loading")
        
        print("\n3. ALGORITHM OPTIMIZATIONS:")
        print("   ⚡ Consider implementing:")
        print("      • Vectorized calculations instead of row-by-row processing")
        print("      • Lazy loading for large datasets")
        print("      • Chunked processing for memory efficiency")
        
        print("\n4. INFRASTRUCTURE OPTIMIZATIONS:")
        print("   🖥️  Consider:")
        print("      • Increase database connection pool size")
        print("      • Use SSD storage for database")
        print("      • Increase available RAM")
        print("      • Consider database query caching")

if __name__ == '__main__':
    import django
    from django.core.management import execute_from_command_line
    
    # Run the performance analysis
    execute_from_command_line(['performance_analysis.py', 'run_performance_analysis', '--detailed'])
