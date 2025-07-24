#!/usr/bin/env python
"""
Test sequential optimization for XUZ1 site in Jul 25 SPR Inv scenario
"""
import os
import sys
import django
from datetime import date

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import scenarios, MasterDataPlan, CalculatedProductionModel, MasterDataPlantModel
from django.db.models import Sum

def test_xuzhou_optimization():
    print('=== Testing Sequential Optimization: Jul 25 SPR Inv - XUZ1 ===')
    
    # Get the scenario
    scenario = 'Jul 25 SPR Inv'
    try:
        scenario_obj = scenarios.objects.get(version=scenario)
        print(f'✅ Found scenario: {scenario}')
    except scenarios.DoesNotExist:
        print(f'❌ Scenario not found: {scenario}')
        return
    
    # Get XUZ1 site
    try:
        site = MasterDataPlantModel.objects.get(SiteName='XUZ1')
        print(f'✅ Found site: {site.SiteName}')
    except MasterDataPlantModel.DoesNotExist:
        print('❌ XUZ1 site not found')
        return
    
    print(f'\n--- Testing XUZ1 Sequential Optimization Logic ---')
    
    # Test months for optimization
    test_months = [
        date(2025, 7, 1),
        date(2025, 8, 1), 
        date(2025, 9, 1),
        date(2025, 10, 1)
    ]
    
    total_gaps = 0
    months_with_gaps = []
    
    for test_month in test_months:
        print(f'\n📅 {test_month.strftime("%b %Y")}:')
        
        # Get Pour Plan capacity (PlanDressMass) for XUZ1
        try:
            plan = MasterDataPlan.objects.get(
                version=scenario_obj, 
                Foundry=site, 
                Month=test_month
            )
            capacity = plan.PlanDressMass or 0
            print(f'  Pour Plan (PlanDressMass): {capacity:.1f}t')
            
            # Show calculation details
            print(f'    AvailableDays: {plan.AvailableDays}')
            print(f'    heatsperdays: {plan.heatsperdays}')
            print(f'    TonsPerHeat: {plan.TonsPerHeat}')
            print(f'    Yield: {plan.Yield}')
            print(f'    WasterPercentage: {plan.WasterPercentage}%')
            
        except MasterDataPlan.DoesNotExist:
            capacity = 0
            print(f'  No Pour Plan for {test_month.strftime("%b %Y")}')
            continue
        
        # Get current production demand for XUZ1
        demand = CalculatedProductionModel.objects.filter(
            version=scenario_obj, 
            site=site, 
            pouring_date__year=test_month.year, 
            pouring_date__month=test_month.month
        ).aggregate(total=Sum('tonnes'))['total'] or 0
        
        print(f'  Current Demand: {demand:.1f}t')
        
        # Calculate gap
        gap = capacity - demand
        print(f'  Gap: {gap:.1f}t')
        
        if gap > 1:
            print(f'  ✅ GAP FOUND! {gap:.1f}t can be filled by sequential optimization')
            total_gaps += gap
            months_with_gaps.append((test_month, gap))
        elif capacity > 0:
            print(f'  ✅ No gap - capacity is fully utilized')
        
        # Show product breakdown if there is production
        if demand > 0:
            products = CalculatedProductionModel.objects.filter(
                version=scenario_obj, 
                site=site, 
                pouring_date__year=test_month.year, 
                pouring_date__month=test_month.month
            ).values('product_group').annotate(total=Sum('tonnes')).order_by('-total')[:3]
            
            if products:
                print(f'    Top Products:')
                for prod in products:
                    print(f'      {prod["product_group"] or "Unknown"}: {prod["total"]:.1f}t')
    
    # Check future production available for optimization
    print(f'\n--- Future Production Available for Optimization ---')
    future_production = CalculatedProductionModel.objects.filter(
        version=scenario_obj,
        site=site,
        pouring_date__gte=date(2025, 8, 1),
        pouring_date__lte=date(2025, 12, 31)
    ).aggregate(total=Sum('tonnes'))['total'] or 0
    
    print(f'Future production (Aug-Dec 2025): {future_production:.1f}t')
    
    # Summary
    print(f'\n=== OPTIMIZATION SUMMARY ===')
    print(f'Total gaps found: {total_gaps:.1f}t across {len(months_with_gaps)} months')
    print(f'Future production available: {future_production:.1f}t')
    
    if total_gaps > 0 and future_production > total_gaps:
        print(f'✅ READY FOR SEQUENTIAL OPTIMIZATION!')
        print(f'   Can fill {total_gaps:.1f}t gaps from {future_production:.1f}t future production')
        for month, gap in months_with_gaps:
            print(f'   - {month.strftime("%b %Y")}: {gap:.1f}t gap to fill')
    elif total_gaps > 0:
        print(f'⚠️  Insufficient future production to fill all gaps')
    else:
        print(f'ℹ️  No gaps found requiring optimization')

if __name__ == "__main__":
    test_xuzhou_optimization()
