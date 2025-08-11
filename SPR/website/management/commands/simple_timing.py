"""
Simple Performance Timing Script for calculate_model
Measures execution time without external dependencies
"""

import time
from django.core.management.base import BaseCommand
from django.db import connection
from datetime import datetime

from website.models import scenarios
from website.management.commands.populate_aggregated_forecast import Command as AggForecastCommand
from website.management.commands.populate_calculated_replenishment_v2 import Command as ReplenishmentCommand
from website.management.commands.populate_calculated_production import Command as ProductionCommand
from website.customized_function import populate_all_aggregated_data

class Command(BaseCommand):
    help = 'Simple performance timing for calculate_model process'

    def add_arguments(self, parser):
        parser.add_argument(
            '--scenario',
            type=str,
            default='Jul 25 SPR',
            help="The scenario version to analyze",
        )

    def handle(self, *args, **options):
        scenario_version = options['scenario']
        
        print("="*60)
        print("⏱️  SIMPLE PERFORMANCE TIMING")
        print("="*60)
        print(f"📊 Scenario: {scenario_version}")
        print(f"🕐 Started: {datetime.now().strftime('%H:%M:%S')}")
        
        try:
            scenario = scenarios.objects.get(version=scenario_version)
        except scenarios.DoesNotExist:
            print(f"❌ Scenario '{scenario_version}' not found")
            return

        times = {}
        total_start = time.time()

        # Step 1: Aggregated Forecast
        print(f"\n1️⃣  populate_aggregated_forecast...")
        start = time.time()
        AggForecastCommand().handle(version=scenario_version)
        times['aggregated_forecast'] = time.time() - start
        print(f"    ✅ Completed in {times['aggregated_forecast']:.1f}s")

        # Step 2: Calculated Replenishment
        print(f"\n2️⃣  populate_calculated_replenishment_v2...")
        start = time.time()
        ReplenishmentCommand().handle(version=scenario_version)
        times['replenishment'] = time.time() - start
        print(f"    ✅ Completed in {times['replenishment']:.1f}s")

        # Step 3: Calculated Production
        print(f"\n3️⃣  populate_calculated_production...")
        start = time.time()
        ProductionCommand().handle(scenario_version=scenario_version)
        times['production'] = time.time() - start
        print(f"    ✅ Completed in {times['production']:.1f}s")

        # Step 4: Aggregated Data
        print(f"\n4️⃣  populate_all_aggregated_data...")
        start = time.time()
        populate_all_aggregated_data(scenario)
        times['aggregated_data'] = time.time() - start
        print(f"    ✅ Completed in {times['aggregated_data']:.1f}s")

        total_time = time.time() - total_start

        # Results summary
        print("\n" + "="*60)
        print("📊 TIMING RESULTS")
        print("="*60)
        
        # Sort by duration
        sorted_times = sorted(times.items(), key=lambda x: x[1], reverse=True)
        
        for step, duration in sorted_times:
            percentage = (duration / total_time) * 100
            status = "🐌" if duration > 30 else "⚡" if duration < 10 else "👍"
            print(f"{status} {step:25}: {duration:6.1f}s ({percentage:5.1f}%)")
        
        print(f"\n🕐 Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        
        # Quick analysis
        slowest = sorted_times[0]
        print(f"\n🎯 ANALYSIS:")
        print(f"   Slowest step: {slowest[0]} ({slowest[1]:.1f}s)")
        
        if total_time > 120:
            print(f"   ⚠️  Process is slow - consider optimizations")
        else:
            print(f"   ✅ Performance looks good!")
            
        # Data volumes
        self.show_data_volumes(scenario)
        
        # Recommendations
        self.show_recommendations(times, total_time)

    def show_data_volumes(self, scenario):
        print(f"\n📊 DATA VOLUMES:")
        print("-" * 30)
        
        from website.models import (
            SMART_Forecast_Model, AggregatedForecast, 
            CalcualtedReplenishmentModel, CalculatedProductionModel
        )
        
        smart_count = SMART_Forecast_Model.objects.filter(version=scenario).count()
        agg_count = AggregatedForecast.objects.filter(version=scenario).count()
        replen_count = CalcualtedReplenishmentModel.objects.filter(version=scenario).count()
        prod_count = CalculatedProductionModel.objects.filter(version=scenario).count()
        
        print(f"   SMART Forecast: {smart_count:,} records")
        print(f"   Aggregated: {agg_count:,} records")
        print(f"   Replenishment: {replen_count:,} records")
        print(f"   Production: {prod_count:,} records")

    def show_recommendations(self, times, total_time):
        print(f"\n💡 RECOMMENDATIONS:")
        print("-" * 30)
        
        # Find the bottleneck
        slowest = max(times.items(), key=lambda x: x[1])
        
        if slowest[1] > 60:  # More than 1 minute
            print(f"🎯 Focus on optimizing: {slowest[0]}")
            
            if slowest[0] == 'aggregated_forecast':
                print("   • Check database indexes on SMART_Forecast_Model")
                print("   • Consider caching product master data")
                
            elif slowest[0] == 'replenishment':
                print("   • Optimize site code extraction logic")
                print("   • Check indexes on forecast and product tables")
                
            elif slowest[0] == 'production':
                print("   • Vectorize inventory calculations")
                print("   • Optimize cost lookup operations")
                
            elif slowest[0] == 'aggregated_data':
                print("   • Consider pre-computed aggregations")
                print("   • Check if data can be processed in chunks")
        
        print(f"\n🔧 GENERAL OPTIMIZATIONS:")
        print("   • Ensure database indexes are created")
        print("   • Monitor database query patterns")
        print("   • Consider parallel processing for independent steps")
        
        if total_time < 60:
            print(f"\n🎉 Good performance! Current optimizations are working.")
        elif total_time < 180:
            print(f"\n👍 Acceptable performance, minor optimizations possible.")
        else:
            print(f"\n⚠️  Consider significant optimizations needed.")
