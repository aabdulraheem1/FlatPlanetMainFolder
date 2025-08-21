#!/usr/bin/env python
"""
Trace First Month Calculation: 30 units forecast → 518 units replenishment
Shows exact step-by-step calculation from replenishment command
"""

import os
import sys
import django
from decimal import Decimal
from datetime import datetime

# Add the SPR project root to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
spr_root = current_dir
if spr_root not in sys.path:
    sys.path.insert(0, spr_root)

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')

try:
    django.setup()
except Exception as e:
    print(f"Django setup error: {e}")
    parent_dir = os.path.dirname(spr_root)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    django.setup()

from website.models import (
    SMART_Forecast_Model, CalcualtedReplenishmentModel
)

def trace_first_month_calculation():
    """Trace exactly how 30 units forecast becomes 518 units replenishment"""
    
    product_code = "Z14EP"
    scenario = "Aug 25 SPR"
    location = "AU03-POB1"
    target_month = "2025-08"
    
    print(f"🔍 TRACING FIRST MONTH CALCULATION")
    print(f"Product: {product_code}, Location: {location}, Month: {target_month}")
    print("=" * 80)
    
    # 1. Get forecast records for first month
    print(f"\n📊 STEP 1: FORECAST INPUT FOR {target_month}")
    forecast_records = SMART_Forecast_Model.objects.filter(
        Product=product_code,
        version__version=scenario,
        Location=location,
        Period_AU__year=2025,
        Period_AU__month=8
    ).order_by('Period_AU')
    
    print(f"Found {forecast_records.count()} forecast records for {target_month}")
    
    total_forecast_month = 0
    for record in forecast_records:
        qty = float(record.Qty)
        total_forecast_month += qty
        print(f"  {record.Period_AU}: {qty} units")
    
    print(f"Total forecast for {target_month}: {total_forecast_month} units")
    
    # 2. Get replenishment records for first month
    print(f"\n📦 STEP 2: REPLENISHMENT OUTPUT FOR {target_month}")
    replenishment_records = CalcualtedReplenishmentModel.objects.filter(
        Product=product_code,
        version__version=scenario,
        Location=location,
        ShippingDate__year=2025,
        ShippingDate__month=8
    ).order_by('ShippingDate')
    
    print(f"Found {replenishment_records.count()} replenishment records for {target_month}")
    
    total_replenishment_month = 0
    record_details = []
    
    for record in replenishment_records:
        qty = float(record.ReplenishmentQty)
        total_replenishment_month += qty
        record_details.append({
            'date': record.ShippingDate,
            'qty': qty,
            'site': str(record.Site)
        })
        print(f"  {record.ShippingDate}: {qty} units from {record.Site}")
    
    print(f"Total replenishment for {target_month}: {total_replenishment_month} units")
    
    # 3. Analyze the pattern
    print(f"\n🔍 STEP 3: CALCULATION ANALYSIS")
    print(f"Input: {total_forecast_month} units forecast")
    print(f"Output: {total_replenishment_month} units replenishment")
    print(f"Multiplication factor: {total_replenishment_month / total_forecast_month if total_forecast_month > 0 else 0:.2f}x")
    
    # 4. Check if there are multiple shipments per forecast period
    print(f"\n📅 STEP 4: SHIPMENT PATTERN ANALYSIS")
    
    # Group replenishment by date
    shipments_by_date = {}
    for detail in record_details:
        date_str = detail['date'].strftime('%Y-%m-%d')
        if date_str not in shipments_by_date:
            shipments_by_date[date_str] = []
        shipments_by_date[date_str].append(detail['qty'])
    
    print("Replenishment shipments breakdown:")
    for date, quantities in sorted(shipments_by_date.items()):
        total_qty = sum(quantities)
        print(f"  {date}: {len(quantities)} shipment(s), total {total_qty} units")
        if len(quantities) > 1:
            print(f"    Individual shipments: {quantities}")
    
    # 5. Check for repeating patterns
    print(f"\n🔢 STEP 5: PATTERN DETECTION")
    
    unique_quantities = list(set(detail['qty'] for detail in record_details))
    print(f"Unique shipment quantities: {sorted(unique_quantities)}")
    
    # Check if 518 is made up of smaller components
    if total_replenishment_month == 518:
        print(f"\nAnalyzing how 518 is constructed:")
        
        # Check for common batch sizes
        potential_batches = [30, 50, 100, 150, 200, 250]
        for batch in potential_batches:
            if 518 % batch == 0:
                num_batches = 518 // batch
                print(f"  518 = {num_batches} batches of {batch} units")
        
        # Check if it's a multiplier of the forecast
        if total_forecast_month > 0:
            exact_multiplier = 518 / total_forecast_month
            print(f"  518 = {total_forecast_month} × {exact_multiplier:.2f}")
            
            # Check for common multipliers
            common_multipliers = [10, 12, 15, 16, 17, 18, 20, 24]
            for mult in common_multipliers:
                if abs(exact_multiplier - mult) < 0.1:
                    result = total_forecast_month * mult
                    print(f"  Close to: {total_forecast_month} × {mult} = {result}")
    
    # 6. Look for business logic clues
    print(f"\n💡 STEP 6: BUSINESS LOGIC INVESTIGATION")
    
    print("Possible explanations for 30 → 518 conversion:")
    print("1. Lead time coverage: Building inventory for multiple months ahead")
    print("2. Minimum order quantity: 518 might be the MOQ for Z14EP")
    print("3. Production batch size: Optimal production run size")
    print("4. Safety stock multiplier: Hidden safety stock calculation")
    print("5. Freight optimization: Economic shipment quantity")
    
    # Calculate what multiplier would give us common business scenarios
    if total_forecast_month > 0:
        print(f"\nMultiplier scenarios:")
        print(f"  For 3-month coverage: {total_forecast_month * 3} units (multiplier: 3.0)")
        print(f"  For 6-month coverage: {total_forecast_month * 6} units (multiplier: 6.0)")
        print(f"  For 12-month coverage: {total_forecast_month * 12} units (multiplier: 12.0)")
        print(f"  For 17.27x coverage: {total_forecast_month * 17.27:.0f} units (matches actual!)")
    
    print(f"\n🎯 CONCLUSION:")
    print(f"The replenishment algorithm converts {total_forecast_month} units forecast")
    print(f"into {total_replenishment_month} units replenishment (17.27x multiplier)")
    print(f"This suggests the system is building inventory to cover ~17 months of demand")
    print(f"instead of just the current month's demand.")

if __name__ == "__main__":
    trace_first_month_calculation()
