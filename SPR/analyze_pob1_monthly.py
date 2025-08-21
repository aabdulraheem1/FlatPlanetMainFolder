#!/usr/bin/env python
"""
Direct Month-by-Month Analysis for AU03-POB1 Location
Shows exact forecast vs replenishment breakdown to explain the 13,683 units
"""

import os
import sys
import django
from decimal import Decimal
from datetime import datetime

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
    SMART_Forecast_Model, CalcualtedReplenishmentModel
)

def analyze_pob1_monthly():
    """Direct analysis of AU03-POB1 forecast vs replenishment by month"""
    
    product_code = "Z14EP"
    scenario = "Aug 25 SPR"
    location = "AU03-POB1"
    
    print(f"📊 DIRECT MONTHLY ANALYSIS: {location}")
    print(f"Product: {product_code}, Scenario: {scenario}")
    print("=" * 100)
    
    # 1. Get forecast data for POB1
    print(f"\n📈 STEP 1: FORECAST DATA FOR {location}")
    forecast_records = SMART_Forecast_Model.objects.filter(
        Product=product_code,
        version__version=scenario,
        Location=location
    ).order_by('Period_AU')
    
    forecast_by_month = {}
    total_forecast = 0
    
    for record in forecast_records:
        month = record.Period_AU.strftime('%Y-%m')
        qty = float(record.Qty)
        
        if month not in forecast_by_month:
            forecast_by_month[month] = 0
        forecast_by_month[month] += qty
        total_forecast += qty
    
    print(f"Total forecast for {location}: {total_forecast:,.0f} units")
    print("Monthly forecast breakdown:")
    for month in sorted(forecast_by_month.keys()):
        qty = forecast_by_month[month]
        print(f"  {month}: {qty:,.0f} units")
    
    # 2. Get replenishment data for POB1
    print(f"\n📦 STEP 2: REPLENISHMENT DATA FOR {location}")
    replenishment_records = CalcualtedReplenishmentModel.objects.filter(
        Product=product_code,
        version__version=scenario,
        Location=location
    ).order_by('ShippingDate')
    
    replenishment_by_month = {}
    total_replenishment = 0
    
    for record in replenishment_records:
        month = record.ShippingDate.strftime('%Y-%m')
        qty = float(record.ReplenishmentQty)
        
        if month not in replenishment_by_month:
            replenishment_by_month[month] = 0
        replenishment_by_month[month] += qty
        total_replenishment += qty
    
    print(f"Total replenishment for {location}: {total_replenishment:,.0f} units")
    print("Monthly replenishment breakdown:")
    for month in sorted(replenishment_by_month.keys()):
        qty = replenishment_by_month[month]
        print(f"  {month}: {qty:,.0f} units")
    
    # 3. Direct comparison
    print(f"\n🔍 STEP 3: MONTH-BY-MONTH COMPARISON FOR {location}")
    print(f"{'Month':<10} {'Forecast':<12} {'Replenishment':<15} {'Multiplier':<12} {'Difference':<12}")
    print("-" * 70)
    
    all_months = sorted(set(list(forecast_by_month.keys()) + list(replenishment_by_month.keys())))
    
    total_forecast_check = 0
    total_replenishment_check = 0
    
    for month in all_months:
        forecast_qty = forecast_by_month.get(month, 0)
        replenishment_qty = replenishment_by_month.get(month, 0)
        
        total_forecast_check += forecast_qty
        total_replenishment_check += replenishment_qty
        
        if forecast_qty > 0:
            multiplier = replenishment_qty / forecast_qty
        else:
            multiplier = float('inf') if replenishment_qty > 0 else 0
        
        difference = replenishment_qty - forecast_qty
        
        print(f"{month:<10} {forecast_qty:<12.0f} {replenishment_qty:<15.0f} {multiplier:<12.2f} {difference:<12.0f}")
    
    print("-" * 70)
    print(f"{'TOTAL':<10} {total_forecast_check:<12.0f} {total_replenishment_check:<15.0f} {total_replenishment_check/total_forecast_check if total_forecast_check > 0 else 0:<12.2f} {total_replenishment_check - total_forecast_check:<12.0f}")
    
    # 4. Analyze specific replenishment records to understand the logic
    print(f"\n🔬 STEP 4: DETAILED REPLENISHMENT RECORDS ANALYSIS FOR {location}")
    print("First 10 replenishment records:")
    print(f"{'Date':<12} {'Qty':<8} {'Site':<6} {'Details'}")
    print("-" * 50)
    
    for i, record in enumerate(replenishment_records[:10]):
        print(f"{record.ShippingDate.strftime('%Y-%m-%d'):<12} {float(record.ReplenishmentQty):<8.0f} {str(record.Site):<6}")
    
    if replenishment_records.count() > 10:
        print(f"... and {replenishment_records.count() - 10} more records")
    
    # 5. Show why replenishment is higher
    print(f"\n💡 STEP 5: WHY REPLENISHMENT IS HIGHER")
    multiplication_factor = total_replenishment / total_forecast if total_forecast > 0 else 0
    
    print(f"Forecast total: {total_forecast:,.0f} units")
    print(f"Replenishment total: {total_replenishment:,.0f} units")
    print(f"Multiplication factor: {multiplication_factor:.2f}x")
    print(f"Additional units created: {total_replenishment - total_forecast:,.0f}")
    
    print("\nPossible reasons for multiplication:")
    print("1. Lead time coverage - system creating inventory ahead of demand")
    print("2. Minimum order quantities - rounding up to MOQ")
    print("3. Safety stock buffer (even though master data shows 0)")
    print("4. Freight optimization - consolidating shipments")
    print("5. Production planning horizon - building ahead of actual demand dates")
    
    # 6. Check if there are any patterns in the multiplication
    print(f"\n📊 STEP 6: MULTIPLICATION PATTERN ANALYSIS")
    
    months_with_multiplication = []
    for month in all_months:
        forecast_qty = forecast_by_month.get(month, 0)
        replenishment_qty = replenishment_by_month.get(month, 0)
        
        if forecast_qty > 0 and replenishment_qty > forecast_qty:
            factor = replenishment_qty / forecast_qty
            months_with_multiplication.append((month, factor, replenishment_qty - forecast_qty))
    
    if months_with_multiplication:
        print("Months with multiplication > 1.0:")
        for month, factor, extra in months_with_multiplication:
            print(f"  {month}: {factor:.2f}x (+{extra:.0f} units)")
    
    print("\n" + "=" * 100)
    print(f"🎯 CONCLUSION: {location} receives {total_replenishment:,.0f} units")
    print(f"   This is {multiplication_factor:.2f}x the forecast of {total_forecast:,.0f} units")
    print(f"   The system adds {total_replenishment - total_forecast:,.0f} extra units due to supply planning logic")

if __name__ == "__main__":
    analyze_pob1_monthly()
