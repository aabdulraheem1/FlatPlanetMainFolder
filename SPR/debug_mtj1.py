#!/usr/bin/env python
import os
import sys
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import *
from django.db.models import Sum

print("=== MTJ1 AUTO-LEVELING DIAGNOSTIC ===")

try:
    # Get MTJ1 July 2025 data for scenario "Jul 25 SPR"
    scenario = scenarios.objects.get(version="Jul 25 SPR")
    print(f"Scenario: {scenario.version}")

    # Check pour plan capacity - Look for capacity of 1912/1913
    pour_plan = MasterDataPlan.objects.filter(
        version=scenario,
        Foundry__SiteName='MTJ1',
        Month__year=2025,
        Month__month=7
    ).first()

    if pour_plan:
        capacity = pour_plan.PlanDressMass
        print(f'✅ MTJ1 July 2025 Pour Plan Capacity: {capacity} tonnes')
    else:
        print('❌ No pour plan found for MTJ1 July 2025')
        capacity = 0

    # Check current production demand after auto-leveling
    current_demand = CalculatedProductionModel.objects.filter(
        version=scenario,
        site__SiteName='MTJ1',
        pouring_date__year=2025,
        pouring_date__month=7
    ).aggregate(total=Sum('tonnes'))['total'] or 0

    gap = capacity - current_demand
    print(f'📊 MTJ1 July 2025 Current Demand: {current_demand:.2f} tonnes')
    print(f'📊 Gap remaining: {gap:.2f} tonnes')

    if gap > 400:  # More than 400 tonnes gap
        print(f'🚨 LARGE GAP DETECTED: {gap:.2f} tonnes unfilled!')
        
        # Check what production is available in future months
        print(f'\n=== CHECKING FUTURE MONTHS FOR AVAILABLE PRODUCTION ===')
        
        total_available = 0
        for month in range(8, 13):  # Aug-Dec 2025
            future_demand = CalculatedProductionModel.objects.filter(
                version=scenario,
                site__SiteName='MTJ1',
                pouring_date__year=2025,
                pouring_date__month=month
            ).aggregate(total=Sum('tonnes'))['total'] or 0
            
            record_count = CalculatedProductionModel.objects.filter(
                version=scenario,
                site__SiteName='MTJ1',
                pouring_date__year=2025,
                pouring_date__month=month
            ).count()
            
            total_available += future_demand
            print(f'  📅 2025-{month:02d}: {future_demand:.2f} tonnes ({record_count} records)')
        
        print(f'\n📈 Total future production available (Aug-Dec): {total_available:.2f} tonnes')
        
        if total_available > gap:
            print(f'✅ Sufficient production exists ({total_available:.2f}t) to fill gap ({gap:.2f}t)')
            print(f'🔍 LIKELY CAUSES:')
            print(f'   1. 90-day constraint prevented moving from later months')
            print(f'   2. Algorithm filled other months first (sequential processing)')
            print(f'   3. Small production records were filtered out')
        else:
            print(f'⚠️  Insufficient production available to fill complete gap')

    # Check auto-leveling status
    opt_state = ScenarioOptimizationState.objects.filter(version=scenario).first()
    print(f'\n=== AUTO-LEVELING STATUS ===')
    print(f'🔧 Auto-leveling applied: {opt_state.auto_optimization_applied if opt_state else "No record"}')
    if opt_state and opt_state.last_optimization_date:
        print(f'🕐 Last optimization: {opt_state.last_optimization_date}')

except Exception as e:
    print(f'❌ Error: {e}')
    import traceback
    traceback.print_exc()
