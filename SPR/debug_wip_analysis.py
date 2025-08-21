# Simple debug script to check WIP double deduction
# Run this in Django shell

from website.models import *
from django.db.models import Sum

def check_wip_double_deduction():
    scenario = scenarios.objects.get(version='Jul 25 SPR')
    product = '1990-208-01B'
    
    print('=== WIP DOUBLE DEDUCTION ANALYSIS ===')
    print(f'Product: {product}')
    print(f'Scenario: {scenario.version}')
    print()
    
    # 1. Get forecast demand
    forecast_demand = SMART_Forecast_Model.objects.filter(
        version=scenario, Product=product
    ).aggregate(total=Sum('Qty'))['total'] or 0
    print(f'1. Total Forecast Demand: {forecast_demand}')
    
    # 2. Get inventory totals
    inventory_data = MasterDataInventory.objects.filter(
        version=scenario, product=product
    ).aggregate(
        onhand=Sum('onhandstock_qty'),
        intransit=Sum('intransitstock_qty'),
        wip=Sum('wip_stock_qty')
    )
    
    onhand = inventory_data['onhand'] or 0
    intransit = inventory_data['intransit'] or 0
    wip = inventory_data['wip'] or 0
    total_inventory = onhand + intransit + wip
    
    print(f'2. Inventory Breakdown:')
    print(f'   On Hand: {onhand}')
    print(f'   In Transit: {intransit}')
    print(f'   WIP: {wip}')
    print(f'   Total Available: {total_inventory}')
    
    # 3. Get replenishment
    replenishment_total = CalcualtedReplenishmentModel.objects.filter(
        version=scenario, Product__Product=product
    ).aggregate(total=Sum('ReplenishmentQty'))['total'] or 0
    print(f'3. Total Replenishment Required: {replenishment_total}')
    
    # 4. Get production
    production_total = CalculatedProductionModel.objects.filter(
        version=scenario, product_id=product
    ).aggregate(total=Sum('production_quantity'))['total'] or 0
    print(f'4. Total Production Required: {production_total}')
    
    # 5. Analysis
    print()
    print('=== FLOW ANALYSIS ===')
    expected_net_demand = max(0, forecast_demand - total_inventory)
    print(f'Expected Net Demand: max(0, {forecast_demand} - {total_inventory}) = {expected_net_demand}')
    print(f'Actual Replenishment: {replenishment_total}')
    print(f'Difference: {abs(expected_net_demand - replenishment_total)}')
    
    # Check production logic
    expected_production = max(0, replenishment_total - total_inventory)
    print(f'Expected Production: max(0, {replenishment_total} - {total_inventory}) = {expected_production}')
    print(f'Actual Production: {production_total}')
    print(f'Production Difference: {abs(expected_production - production_total)}')
    
    # Check for double deduction
    print()
    print('=== DOUBLE DEDUCTION CHECK ===')
    if production_total < expected_production:
        reduction = expected_production - production_total
        if abs(reduction - wip) < 10:  # Within 10 units (accounting for rounding)
            print(f'⚠️  POTENTIAL WIP DOUBLE DEDUCTION DETECTED!')
            print(f'   Production shortfall: {reduction}')
            print(f'   WIP quantity: {wip}')
            print(f'   This suggests WIP may be deducted twice')
        else:
            print(f'✅ No clear WIP double deduction pattern')
    else:
        print(f'✅ Production meets or exceeds expected requirements')

# Run the analysis
check_wip_double_deduction()
