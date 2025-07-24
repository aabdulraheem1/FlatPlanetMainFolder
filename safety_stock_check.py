#!/usr/bin/env python
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import scenarios, CalcualtedReplenishmentModel, MasterDataSafetyStocks, MasterDataInventory, SMART_Forecast_Model
from datetime import datetime, date
from collections import defaultdict

scenario = scenarios.objects.get(version='Jul 25 SPR Inv')

# Get DEWB135-1 data
product = 'DEWB135-1'

# Get safety stock requirements by plant
safety_stocks = {}
for ss in MasterDataSafetyStocks.objects.filter(version=scenario, PartNum=product):
    safety_stocks[ss.Plant] = float(ss.MinimumQty or 0) + float(ss.SafetyQty or 0)

print('Safety Stock Requirements by Plant:')
for plant, required in safety_stocks.items():
    print(f'  {plant}: {required} units')

# Get opening inventory by site
opening_inventory = {}
for inv in MasterDataInventory.objects.filter(version=scenario, product=product):
    site_key = inv.site.SiteName
    total_stock = float(inv.onhandstock_qty or 0) + float(inv.intransitstock_qty or 0) + float(inv.wip_stock_qty or 0)
    opening_inventory[site_key] = total_stock

print('\nOpening Inventory by Site:')
for site, stock in opening_inventory.items():
    print(f'  {site}: {stock} units')

# Get total monthly demand from forecast
monthly_demand = defaultdict(float)
for f in SMART_Forecast_Model.objects.filter(version=scenario, Product=product):
    month_key = f.Period_AU.replace(day=1)
    monthly_demand[month_key] += float(f.Qty or 0)

print(f'\nTotal Monthly Demand:')
for month in sorted(monthly_demand.keys())[:6]:  # Show first 6 months
    print(f'  {month.strftime("%Y-%m")}: {monthly_demand[month]} units')

# Get replenishment by month
monthly_replenishment = defaultdict(float)
for r in CalcualtedReplenishmentModel.objects.filter(version=scenario, Product__Product=product):
    month_key = r.ShippingDate.replace(day=1)
    monthly_replenishment[month_key] += float(r.ReplenishmentQty or 0)

print(f'\nMonthly Replenishment:')
for month in sorted(monthly_replenishment.keys())[:6]:  # Show first 6 months
    print(f'  {month.strftime("%Y-%m")}: {monthly_replenishment[month]} units')

# Now let's simulate inventory flow for key locations
print('\n=== SAFETY STOCK VALIDATION ===')

# Focus on locations with high safety stock requirements
high_safety_locations = ['POB1', 'TOW1', 'WAT1']

for location in high_safety_locations:
    print(f'\n--- Location: {location} ---')
    
    # Get safety stock requirement for this location
    required_safety = safety_stocks.get(location, 0)
    print(f'Required safety stock: {required_safety} units')
    
    # Get opening inventory
    opening = opening_inventory.get(location, 0)
    print(f'Opening inventory: {opening} units')
    
    # Simulate monthly flow
    running_inventory = opening
    
    for month in sorted(monthly_demand.keys())[:3]:  # Show first 3 months
        month_demand = 0
        month_replenishment = 0
        
        # Get demand for this location for this month (approximate from proportions)
        total_month_demand = monthly_demand[month]
        # Assume location gets proportional share based on safety stock ratio
        if sum(safety_stocks.values()) > 0:
            location_demand_ratio = required_safety / sum(safety_stocks.values())
            month_demand = total_month_demand * location_demand_ratio
        
        # Get replenishment for this month
        month_replenishment = monthly_replenishment.get(month, 0)
        
        # Calculate end-of-month inventory
        end_inventory = running_inventory + month_replenishment - month_demand
        
        print(f'  {month.strftime("%Y-%m")}: Start={running_inventory:.1f}, Demand={month_demand:.1f}, Replen={month_replenishment:.1f}, End={end_inventory:.1f}')
        
        # Check if safety stock is maintained
        if end_inventory < required_safety:
            print(f'    ⚠️ WARNING: End inventory ({end_inventory:.1f}) below safety stock ({required_safety})')
        else:
            print(f'    ✓ Safety stock maintained')
        
        running_inventory = end_inventory
