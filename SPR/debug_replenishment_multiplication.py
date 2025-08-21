#!/usr/bin/env python
"""
Debug Z14EP Replenishment Multiplication Analysis
Investigates why Z14EP forecast (2,921) becomes replenishment (24,075) - 8.2x multiplication
"""

import os
import sys
import django
from decimal import Decimal

# Add the SPR project root to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
spr_root = current_dir  # We're already in the SPR directory
if spr_root not in sys.path:
    sys.path.insert(0, spr_root)

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')

try:
    django.setup()
except Exception as e:
    print(f"Django setup error: {e}")
    # Fallback: try adding parent directory
    parent_dir = os.path.dirname(spr_root)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    django.setup()

from website.models import (
    SMART_Forecast_Model, CalcualtedReplenishmentModel, CalculatedProductionModel,
    MasterDataSafetyStocks
)

def analyze_z14ep_replenishment_multiplication():
    """Analyze why Z14EP replenishment is 8.2x higher than forecast"""
    
    product_code = "Z14EP"
    scenario = "Aug 25 SPR"
    
    print(f"🔍 Z14EP Replenishment Multiplication Analysis")
    print(f"Product: {product_code}, Scenario: {scenario}")
    print("=" * 80)
    
    # 1. Check forecast data
    print("\n📊 1. SMART FORECAST ANALYSIS:")
    forecast_records = SMART_Forecast_Model.objects.filter(
        Product=product_code,
        version__version=scenario
    )
    
    total_forecast = sum(float(record.Qty) for record in forecast_records)
    print(f"Total forecast quantity: {total_forecast:,.0f} units")
    print(f"Number of forecast periods: {forecast_records.count()}")
    
    if forecast_records.exists():
        for record in forecast_records[:3]:  # Show first 3 records
            print(f"  Period {record.Period_AU}: {float(record.Qty):,.0f} units at {record.Location}")
    
    # 2. Check replenishment data 
    print("\n📦 2. REPLENISHMENT ANALYSIS:")
    replenishment_records = CalcualtedReplenishmentModel.objects.filter(
        Product=product_code,
        version__version=scenario
    )
    
    total_replenishment = sum(float(record.ReplenishmentQty) for record in replenishment_records)
    print(f"Total replenishment quantity: {total_replenishment:,.0f} units")
    print(f"Number of replenishment records: {replenishment_records.count()}")
    print(f"Multiplication factor: {total_replenishment / total_forecast:.2f}x")
    
    # 3. Analyze safety stock requirements
    print("\n🛡️ 3. SAFETY STOCK ANALYSIS:")
    
    # Get all delivery locations for Z14EP forecast
    delivery_locations = set(record.Location for record in forecast_records)
    print(f"Delivery locations requiring Z14EP: {sorted(delivery_locations)}")
    
    total_safety_stock = 0
    for location in sorted(delivery_locations):
        safety_stock_records = MasterDataSafetyStocks.objects.filter(
            PartNum=product_code,
            Plant=location,
            version__version=scenario
        )
        
        if safety_stock_records.exists():
            for ss in safety_stock_records:
                safety_level = float(ss.SafetyQty or 0)
                total_safety_stock += safety_level
                print(f"  {location}: Safety stock level = {safety_level:,.0f} units")
        else:
            print(f"  {location}: No safety stock data found")
    
    print(f"Total safety stock requirements: {total_safety_stock:,.0f} units")
    
    # 4. Calculate expected replenishment
    print("\n🧮 4. REPLENISHMENT CALCULATION BREAKDOWN:")
    expected_base = total_forecast + total_safety_stock
    print(f"Expected base replenishment: {total_forecast:,.0f} (forecast) + {total_safety_stock:,.0f} (safety stock) = {expected_base:,.0f}")
    print(f"Actual replenishment: {total_replenishment:,.0f}")
    print(f"Difference: {total_replenishment - expected_base:,.0f} units")
    
    if expected_base > 0:
        print(f"Base calculation factor: {expected_base / total_forecast:.2f}x")
        print(f"Additional multiplication: {total_replenishment / expected_base:.2f}x")
    
    # 5. Detailed replenishment record analysis
    print("\n📋 5. DETAILED REPLENISHMENT RECORDS:")
    site_summary = {}
    
    for record in replenishment_records:
        site = record.Site
        qty = float(record.ReplenishmentQty)
        
        if site not in site_summary:
            site_summary[site] = {"count": 0, "total_qty": 0, "periods": []}
        
        site_summary[site]["count"] += 1
        site_summary[site]["total_qty"] += qty
        site_summary[site]["periods"].append(record.ShippingDate)
    
    for site, data in site_summary.items():
        print(f"  {site}: {data['total_qty']:,.0f} units across {data['count']} periods")
        unique_periods = len(set(data['periods']))
        print(f"    Unique periods: {unique_periods}, Average per period: {data['total_qty'] / data['count']:,.1f}")
    
    # 6. Check for 24-month planning horizon effect
    print("\n📅 6. PLANNING HORIZON ANALYSIS:")
    
    # Group forecast by period to see distribution
    forecast_by_period = {}
    for record in forecast_records:
        period = record.Period_AU
        if period not in forecast_by_period:
            forecast_by_period[period] = 0
        forecast_by_period[period] += float(record.Qty)
    
    print(f"Forecast spans {len(forecast_by_period)} periods:")
    sorted_periods = sorted(forecast_by_period.keys())
    for i, period in enumerate(sorted_periods[:6]):  # Show first 6 periods
        qty = forecast_by_period[period]
        print(f"  {period}: {qty:,.0f} units")
    
    if len(sorted_periods) > 6:
        print(f"  ... ({len(sorted_periods) - 6} more periods)")
    
    # 7. Check replenishment periods vs forecast periods
    print("\n🔄 7. REPLENISHMENT VS FORECAST PERIOD COMPARISON:")
    
    replenishment_periods = set()
    for record in replenishment_records:
        replenishment_periods.add(record.ShippingDate)
    
    forecast_periods = set(forecast_by_period.keys())
    
    print(f"Forecast periods: {len(forecast_periods)}")
    print(f"Replenishment periods: {len(replenishment_periods)}")
    print(f"Period multiplication factor: {len(replenishment_periods) / len(forecast_periods):.2f}x")
    
    # Check if replenishment is creating multiple shipments per forecast period
    extra_periods = replenishment_periods - forecast_periods
    if extra_periods:
        print(f"Additional replenishment periods not in forecast: {len(extra_periods)}")
        print(f"Sample additional periods: {sorted(list(extra_periods))[:5]}")

if __name__ == "__main__":
    analyze_z14ep_replenishment_multiplication()
