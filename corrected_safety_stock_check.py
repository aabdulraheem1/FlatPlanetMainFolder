import os
import django
from django.conf import settings

# Initialize Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import scenarios, CalcualtedReplenishmentModel, MasterDataSafetyStocks, MasterDataInventory, SMART_Forecast_Model
from datetime import datetime, date
from collections import defaultdict

scenario = scenarios.objects.get(version='Jul 25 SPR Inv')

# Get DEWB135-1 data
product = 'DEWB135-1'

# Compound locations from forecast that map to plants
location_to_plant = {
    'AU03-POB1': 'POB1',
    'AU03-TOW1': 'TOW1', 
    'AU03-WAT1': 'WAT1',
    'CL01-MAI1': 'MAI1',
    'US01-PHO2': 'PHO2',
    'ZA01-JOH1': 'JOH1'
}

# Get safety stock requirements by plant
safety_stocks = {}
for ss in MasterDataSafetyStocks.objects.filter(version=scenario, PartNum=product):
    if float(ss.MinimumQty or 0) > 0 or float(ss.SafetyQty or 0) > 0:
        safety_stocks[ss.Plant] = float(ss.MinimumQty or 0) + float(ss.SafetyQty or 0)

print('Safety Stock Requirements by Plant:')
for plant, required in sorted(safety_stocks.items()):
    print(f'  {plant}: {required} units')

# Get opening inventory by site
opening_inventory = {}
for inv in MasterDataInventory.objects.filter(version=scenario, product=product):
    site_key = inv.site.SiteName
    total_stock = float(inv.onhandstock_qty or 0) + float(inv.intransitstock_qty or 0) + float(inv.wip_stock_qty or 0)
    opening_inventory[site_key] = total_stock

print('\nOpening Inventory by Site:')
for site, stock in sorted(opening_inventory.items()):
    print(f'  {site}: {stock} units')

# Get monthly demand from forecast using compound location keys
monthly_demand = defaultdict(lambda: defaultdict(float))
for forecast in SMART_Forecast_Model.objects.filter(version=scenario, Product=product, Qty__gt=0).order_by('Period_AU'):
    period = forecast.Period_AU
    if isinstance(period, str):
        # Convert string like "2025-07" to date
        year, month = period.split('-')
        period_date = date(int(year), int(month), 1)
    else:
        period_date = period
    
    location = forecast.Location
    plant = location_to_plant.get(location)
    if plant:
        monthly_demand[period_date][plant] += float(forecast.Qty)

print('\nMonthly Demand by Plant:')
for period_date in sorted(monthly_demand.keys()):
    period_str = period_date.strftime('%Y-%m')
    print(f'  {period_str}:')
    total_period_demand = 0
    for plant, demand in sorted(monthly_demand[period_date].items()):
        print(f'    {plant}: {demand} units')
        total_period_demand += demand
    print(f'    Total: {total_period_demand} units')

# Get monthly replenishment directly using plant codes
monthly_replenishment = defaultdict(lambda: defaultdict(float))
for rep in CalcualtedReplenishmentModel.objects.filter(version=scenario, Product=product, ReplenishmentQty__gt=0).order_by('ShippingDate'):
    shipping_date = rep.ShippingDate
    period_date = date(shipping_date.year, shipping_date.month, 1)
    
    # Location is already a plant code (POB1, TOW1, WAT1, etc.)
    plant = rep.Location
    monthly_replenishment[period_date][plant] += float(rep.ReplenishmentQty)

print('\nMonthly Replenishment by Plant:')
for period_date in sorted(monthly_replenishment.keys()):
    period_str = period_date.strftime('%Y-%m')
    print(f'  {period_str}:')
    total_period_replen = 0
    for plant, replen in sorted(monthly_replenishment[period_date].items()):
        print(f'    {plant}: {replen} units')
        total_period_replen += replen
    print(f'    Total: {total_period_replen} units')

print('\n=== SAFETY STOCK VALIDATION ===')

# Track inventory by plant
running_inventory = {}
for plant in safety_stocks.keys():
    running_inventory[plant] = opening_inventory.get(plant, 0)

# Process months in chronological order
all_periods = set(monthly_demand.keys()) | set(monthly_replenishment.keys())
for period_date in sorted(all_periods):
    period_str = period_date.strftime('%Y-%m')
    print(f'\n--- Month: {period_str} ---')
    
    # For each plant with safety stock requirements
    for plant in sorted(safety_stocks.keys()):
        required_safety = safety_stocks[plant]
        start_inventory = running_inventory[plant]
        
        # Get demand and replenishment for this month/plant
        demand = monthly_demand.get(period_date, {}).get(plant, 0)
        replenishment = monthly_replenishment.get(period_date, {}).get(plant, 0)
        
        # Calculate ending inventory
        end_inventory = start_inventory + replenishment - demand
        running_inventory[plant] = end_inventory
        
        if demand > 0 or replenishment > 0 or end_inventory < required_safety:
            print(f'  {plant}: Start={start_inventory:.1f}, Demand={demand:.1f}, Replen={replenishment:.1f}, End={end_inventory:.1f}')
            if end_inventory < required_safety:
                shortfall = required_safety - end_inventory
                print(f'    ⚠️ WARNING: End inventory ({end_inventory:.1f}) below safety stock ({required_safety})! Shortfall: {shortfall:.1f}')
            else:
                print(f'    ✓ Safety stock maintained (required: {required_safety})')
