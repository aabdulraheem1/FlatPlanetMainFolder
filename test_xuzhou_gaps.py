#!/usr/bin/env python
"""
Test if XUZ1 has gaps that can be filled by sequential optimization
"""
import os
import sys
import django
from datetime import date

# Setup Django environment
sys.path.append('.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import scenarios, MasterDataPlan, CalculatedProductionModel, MasterDataPlantModel
from django.db.models import Sum

def test_xuzhou_gaps():
    print('=== Testing XUZ1 Gaps for Sequential Auto-Level Optimization ===')
    
    # Get the scenario and site
    scenario = scenarios.objects.get(version='Jul 25 SPR Inv')
    site = MasterDataPlantModel.objects.get(SiteName='XUZ1')
    
    test_months = [
        (date(2025, 7, 1), 'Jul 2025'),
        (date(2025, 8, 1), 'Aug 2025'),
        (date(2025, 9, 1), 'Sep 2025'),
        (date(2025, 10, 1), 'Oct 2025')
    ]
    
    total_gaps = 0
    gaps_found = []
    
    for test_date, month_name in test_months:
        print(f'\n📅 {month_name}:')
        
        # Get Pour Plan capacity (search by year and month, not exact date)
        try:
            plan = MasterDataPlan.objects.filter(
                version=scenario, 
                Foundry=site, 
                Month__year=test_date.year,
                Month__month=test_date.month
            ).first()
            if plan:
                capacity = plan.PlanDressMass or 0
                print(f'  Pour Plan Capacity: {capacity:.1f}t')
                print(f'    (Plan date: {plan.Month})')
            else:
                print(f'  ❌ No Pour Plan found for {month_name}')
                continue
        except Exception as e:
            print(f'  ❌ Error getting Pour Plan: {e}')
            continue
            
        # Get current demand
        demand = CalculatedProductionModel.objects.filter(
            version=scenario.version,
            site=site,
            pouring_date__year=test_date.year,
            pouring_date__month=test_date.month
        ).aggregate(total=Sum('tonnes'))['total'] or 0
        
        print(f'  Current Demand: {demand:.1f}t')
        
        # Calculate gap
        gap = capacity - demand
        print(f'  Gap: {gap:.1f}t')
        
        if gap > 1:  # Only significant gaps
            print(f'  ✅ GAP FOUND! Can be filled by auto-level optimization')
            total_gaps += gap
            gaps_found.append((month_name, gap))
        elif gap < -1:
            print(f'  ⚠️  OVER CAPACITY by {abs(gap):.1f}t')
        else:
            print(f'  ✅ Balanced (no significant gap)')
    
    print(f'\n=== SUMMARY ===')
    if gaps_found:
        print(f'✅ READY FOR AUTO-LEVEL OPTIMIZATION!')
        print(f'Total gaps: {total_gaps:.1f}t across {len(gaps_found)} months')
        for month, gap in gaps_found:
            print(f'  - {month}: {gap:.1f}t gap to fill')
        
        print(f'\n🚀 To test sequential month-by-month optimization:')
        print(f'   1. Go to scenario review page for "Jul 25 SPR Inv"')
        print(f'   2. Click "Auto Level Optimization" button')
        print(f'   3. Select XUZ1 site')
        print(f'   4. Choose product groups to prioritize')
        print(f'   5. Run optimization')
        print(f'   6. Check if gaps are filled sequentially: Jul→Aug→Sep→Oct')
    else:
        print(f'ℹ️  No significant gaps found - optimization may not show visible changes')
    
if __name__ == "__main__":
    test_xuzhou_gaps()
