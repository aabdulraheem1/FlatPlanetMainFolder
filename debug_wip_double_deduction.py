import os
import sys
import django

# Add the project path
sys.path.append(r'c:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'spr.settings')

# Initialize Django
django.setup()

from website.models import *
from django.db.models import Sum
import pandas as pd

def analyze_wip_double_deduction():
    """Analyze potential WIP double deduction in the quantity flow"""
    
    scenario = scenarios.objects.get(version='Jul 25 SPR')
    product = '1990-208-01B'

    print('🔍 TRACING QUANTITY FLOW - INVESTIGATING POTENTIAL WIP DOUBLE DEDUCTION')
    print('=' * 80)

    print('\n1️⃣  SMART FORECAST DEMAND')
    print('-' * 40)
    smart_forecast = SMART_Forecast_Model.objects.filter(
        version=scenario, 
        Product=product
    ).aggregate(total_qty=Sum('Qty'))
    forecast_qty = smart_forecast['total_qty'] or 0
    print(f'Total forecast demand: {forecast_qty} units')

    print('\n2️⃣  INVENTORY SNAPSHOT')
    print('-' * 40)
    inventory = MasterDataInventory.objects.filter(
        version=scenario,
        product=product
    )
    total_onhand = inventory.aggregate(total=Sum('onhandstock_qty'))['total'] or 0
    total_intransit = inventory.aggregate(total=Sum('intransitstock_qty'))['total'] or 0  
    total_wip = inventory.aggregate(total=Sum('wip_stock_qty'))['total'] or 0

    print(f'Total On Hand: {total_onhand}')
    print(f'Total In Transit: {total_intransit}')
    print(f'Total WIP: {total_wip}')
    print(f'Total Available Stock: {total_onhand + total_intransit + total_wip}')

    print('\n3️⃣  REPLENISHMENT CALCULATION')
    print('-' * 40)
    replenishment = CalcualtedReplenishmentModel.objects.filter(
        version=scenario,
        Product__Product=product
    ).aggregate(total_qty=Sum('ReplenishmentQty'))
    replenishment_qty = replenishment['total_qty'] or 0
    print(f'Total replenishment needed: {replenishment_qty} units')

    print('\n4️⃣  PRODUCTION CALCULATION') 
    print('-' * 40)
    production = CalculatedProductionModel.objects.filter(
        version=scenario,
        product_id=product
    ).aggregate(total_qty=Sum('production_quantity'))
    production_qty = production['total_qty'] or 0
    print(f'Total production required: {production_qty} units')

    print('\n5️⃣  DETAILED SITE-BY-SITE BREAKDOWN')
    print('-' * 40)
    
    # Get all sites with inventory for this product
    sites_with_inventory = inventory.values('site_id').annotate(
        onhand=Sum('onhandstock_qty'),
        intransit=Sum('intransitstock_qty'), 
        wip=Sum('wip_stock_qty')
    ).order_by('site_id')

    for site_inv in sites_with_inventory:
        site = site_inv['site_id']
        onhand = site_inv['onhand'] or 0
        intransit = site_inv['intransit'] or 0 
        wip = site_inv['wip'] or 0
        total_available = onhand + intransit + wip
        
        # Check replenishment for this site
        site_replenishment = CalcualtedReplenishmentModel.objects.filter(
            version=scenario,
            Product__Product=product,
            Site__SiteName=site
        ).aggregate(total=Sum('ReplenishmentQty'))['total'] or 0
        
        # Check production for this site  
        site_production = CalculatedProductionModel.objects.filter(
            version=scenario,
            product_id=product,
            site_id=site
        ).aggregate(total=Sum('production_quantity'))['total'] or 0
        
        print(f'\n📍 Site: {site}')
        print(f'   Inventory: OnHand={onhand}, InTransit={intransit}, WIP={wip} | Total={total_available}')
        print(f'   Replenishment: {site_replenishment}')  
        print(f'   Production: {site_production}')
        
        # Calculate expected production based on production logic
        expected_production = max(0, site_replenishment - total_available)
        print(f'   Expected Production Logic: max(0, {site_replenishment} - {total_available}) = {expected_production}')
        
        # Check for discrepancies
        if site_production != expected_production:
            print(f'   ⚠️  DISCREPANCY: Actual production ({site_production}) != Expected ({expected_production})')
        else:
            print(f'   ✅ Production matches expected logic')

    print('\n6️⃣  OVERALL QUANTITY FLOW ANALYSIS')
    print('-' * 40)
    print(f'Forecast Demand: {forecast_qty}')
    print(f'Available Stock: {total_onhand + total_intransit + total_wip}')
    print(f'Net Requirement: {max(0, forecast_qty - (total_onhand + total_intransit + total_wip))}')
    print(f'Actual Replenishment: {replenishment_qty}')
    print(f'Actual Production: {production_qty}')
    
    # Analysis summary
    print('\n7️⃣  WIP DOUBLE DEDUCTION ANALYSIS')
    print('-' * 40)
    
    # Check if WIP is being deducted in replenishment AND production
    print('Checking replenishment logic...')
    # In replenishment v2, WIP is included in available stock calculation
    print('✓ Replenishment command includes WIP in available stock (on_hand + in_transit + wip - safety_stock)')
    
    print('\nChecking production logic...')
    # In production command, WIP is also included in opening inventory
    print('✓ Production command includes WIP in opening inventory (onhand + intransit + wip)')
    
    print('\n📊 CONCLUSION:')
    if forecast_qty > 0 and production_qty < (forecast_qty * 0.1):  # If production is less than 10% of forecast
        print('⚠️  WARNING: Production quantity seems unusually small relative to forecast')
        print('   This could indicate potential double deduction of WIP inventory')
        print('   WIP may be deducted once in replenishment calculation and again in production calculation')
    else:
        print('✅ Production quantities appear reasonable relative to forecast demand')

if __name__ == "__main__":
    analyze_wip_double_deduction()
