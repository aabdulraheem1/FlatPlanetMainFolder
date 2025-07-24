"""
Performance Analysis Script for Django Review Scenario View
This script will time different components of the review_scenario view to identify bottlenecks.
"""

import time
import cProfile
import pstats
from io import StringIO
from django.core.management.base import BaseCommand
from website.models import scenarios
from website.customized_function import (
    calculate_control_tower_data,
    get_foundry_chart_data,
    get_forecast_data_by_parent_product_group,
    get_forecast_data_by_product_group,
    get_forecast_data_by_region,
    get_forecast_data_by_customer,
    get_forecast_data_by_data_source,
    get_inventory_data_with_start_date,
    get_production_data_by_group,
    get_top_products_per_month_by_group,
    detailed_view_scenario_inventory
)

class Command(BaseCommand):
    help = 'Analyze performance of review scenario view components'

    def add_arguments(self, parser):
        parser.add_argument('version', type=str, help='Scenario version to analyze')

    def handle(self, *args, **options):
        version = options['version']
        
        try:
            scenario = scenarios.objects.get(version=version)
            print(f"🔍 Performance Analysis for Scenario: {version}")
            print("=" * 60)
            
            total_start = time.time()
            
            # 1. Control Tower Data
            print("⏱️  Testing Control Tower Data...")
            start = time.time()
            control_tower_data = calculate_control_tower_data(scenario)
            control_tower_time = time.time() - start
            print(f"   ✅ Control Tower: {control_tower_time:.2f} seconds")
            
            # 2. Foundry Data
            print("⏱️  Testing Foundry Data...")
            start = time.time()
            foundry_data = get_foundry_chart_data(scenario)
            foundry_time = time.time() - start
            print(f"   ✅ Foundry Data: {foundry_time:.2f} seconds")
            
            # 3. Forecast Data (multiple components)
            print("⏱️  Testing Forecast Data...")
            forecast_start = time.time()
            
            start = time.time()
            forecast_parent = get_forecast_data_by_parent_product_group(scenario)
            forecast_parent_time = time.time() - start
            
            start = time.time()
            forecast_product = get_forecast_data_by_product_group(scenario)
            forecast_product_time = time.time() - start
            
            start = time.time()
            forecast_region = get_forecast_data_by_region(scenario)
            forecast_region_time = time.time() - start
            
            start = time.time()
            forecast_customer = get_forecast_data_by_customer(scenario)
            forecast_customer_time = time.time() - start
            
            start = time.time()
            forecast_data_source = get_forecast_data_by_data_source(scenario)
            forecast_data_source_time = time.time() - start
            
            total_forecast_time = time.time() - forecast_start
            print(f"   ✅ Forecast by Parent Group: {forecast_parent_time:.2f} seconds")
            print(f"   ✅ Forecast by Product Group: {forecast_product_time:.2f} seconds")
            print(f"   ✅ Forecast by Region: {forecast_region_time:.2f} seconds")
            print(f"   ✅ Forecast by Customer: {forecast_customer_time:.2f} seconds")
            print(f"   ✅ Forecast by Data Source: {forecast_data_source_time:.2f} seconds")
            print(f"   📊 Total Forecast: {total_forecast_time:.2f} seconds")
            
            # 4. Inventory Data
            print("⏱️  Testing Inventory Data...")
            start = time.time()
            inventory_data = get_inventory_data_with_start_date(scenario)
            inventory_time = time.time() - start
            print(f"   ✅ Inventory Data: {inventory_time:.2f} seconds")
            
            # 5. Supplier Data
            print("⏱️  Testing Supplier Data...")
            start = time.time()
            supplier_chart_data = get_production_data_by_group('HBZJBF02', scenario)
            supplier_chart_time = time.time() - start
            
            start = time.time()
            supplier_top_products = get_top_products_per_month_by_group('HBZJBF02', scenario)
            supplier_products_time = time.time() - start
            
            total_supplier_time = supplier_chart_time + supplier_products_time
            print(f"   ✅ Supplier Chart Data: {supplier_chart_time:.2f} seconds")
            print(f"   ✅ Supplier Top Products: {supplier_products_time:.2f} seconds")
            print(f"   📊 Total Supplier: {total_supplier_time:.2f} seconds")
            
            # 6. Detailed Inventory Data
            print("⏱️  Testing Detailed Inventory Data...")
            start = time.time()
            detailed_inventory = detailed_view_scenario_inventory(scenario)
            detailed_inventory_time = time.time() - start
            print(f"   ✅ Detailed Inventory: {detailed_inventory_time:.2f} seconds")
            
            total_time = time.time() - total_start
            
            # Summary
            print("\n" + "=" * 60)
            print("📈 PERFORMANCE SUMMARY:")
            print("=" * 60)
            print(f"🎯 Control Tower Data:     {control_tower_time:>8.2f}s ({control_tower_time/total_time*100:>5.1f}%)")
            print(f"🏭 Foundry Data:          {foundry_time:>8.2f}s ({foundry_time/total_time*100:>5.1f}%)")
            print(f"📊 Forecast Data:         {total_forecast_time:>8.2f}s ({total_forecast_time/total_time*100:>5.1f}%)")
            print(f"📦 Inventory Data:        {inventory_time:>8.2f}s ({inventory_time/total_time*100:>5.1f}%)")
            print(f"🚚 Supplier Data:         {total_supplier_time:>8.2f}s ({total_supplier_time/total_time*100:>5.1f}%)")
            print(f"🔍 Detailed Inventory:    {detailed_inventory_time:>8.2f}s ({detailed_inventory_time/total_time*100:>5.1f}%)")
            print("-" * 60)
            print(f"⏱️  TOTAL TIME:            {total_time:>8.2f}s")
            print("=" * 60)
            
            # Recommendations
            print("\n🚀 OPTIMIZATION RECOMMENDATIONS:")
            components = [
                ("Control Tower", control_tower_time),
                ("Foundry", foundry_time),
                ("Forecast", total_forecast_time),
                ("Inventory", inventory_time),
                ("Supplier", total_supplier_time),
                ("Detailed Inventory", detailed_inventory_time)
            ]
            
            components.sort(key=lambda x: x[1], reverse=True)
            
            print("📋 Components by loading time (slowest first):")
            for i, (name, time_taken) in enumerate(components, 1):
                if time_taken > 2:
                    status = "🔴 CRITICAL"
                elif time_taken > 1:
                    status = "🟡 SLOW"
                else:
                    status = "🟢 GOOD"
                print(f"   {i}. {name:<18} {time_taken:>6.2f}s {status}")
            
            print("\n💡 SUGGESTIONS:")
            if total_time > 10:
                print("   • Consider implementing lazy loading for tabs")
                print("   • Cache frequently accessed data")
                print("   • Load only essential data initially")
            if max([t[1] for t in components]) > 3:
                print("   • Optimize database queries in the slowest component")
                print("   • Consider pagination for large datasets")
            
        except scenarios.DoesNotExist:
            print(f"❌ Scenario '{version}' not found!")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
