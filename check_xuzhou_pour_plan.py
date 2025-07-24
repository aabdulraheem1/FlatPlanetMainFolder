#!/usr/bin/env python
"""
Check Pour Plan data for XUZ1 site in Jul 25 SPR Inv scenario
"""
import os
import sys
import django
from datetime import date

# Setup Django environment
sys.path.append('.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import scenarios, MasterDataPlan, MasterDataPlantModel

def check_xuzhou_pour_plan():
    print('=== Checking XUZ1 Pour Plan Data in Jul 25 SPR Inv ===')
    
    # Get the scenario
    try:
        scenario = scenarios.objects.get(version='Jul 25 SPR Inv')
        print(f'✅ Found scenario: {scenario.version}')
    except scenarios.DoesNotExist:
        print('❌ Scenario not found: Jul 25 SPR Inv')
        return
    
    # Get XUZ1 site
    try:
        site = MasterDataPlantModel.objects.get(SiteName='XUZ1')
        print(f'✅ Found site: {site.SiteName}')
    except MasterDataPlantModel.DoesNotExist:
        print('❌ XUZ1 site not found')
        return
    
    # Get all Pour Plans for XUZ1 in this scenario
    plans = MasterDataPlan.objects.filter(version=scenario, Foundry=site)
    print(f'\n📊 XUZ1 Pour Plans in Jul 25 SPR Inv:')
    print(f'Total plans found: {plans.count()}')
    
    if plans.count() == 0:
        print('❌ NO POUR PLANS FOUND! This is why the red line is not showing.')
        print('\n🔍 Checking if Pour Plans exist for other scenarios:')
        
        # Check other scenarios
        other_scenarios = scenarios.objects.exclude(version='Jul 25 SPR Inv')[:5]
        for other_scenario in other_scenarios:
            other_plans = MasterDataPlan.objects.filter(version=other_scenario, Foundry=site)
            if other_plans.exists():
                print(f'  ✅ {other_scenario.version}: {other_plans.count()} plans')
                # Show first plan as example
                first_plan = other_plans.first()
                if first_plan.Month:
                    print(f'    Example: {first_plan.Month.strftime("%b %Y")}: PlanDressMass = {first_plan.PlanDressMass}')
            else:
                print(f'  ❌ {other_scenario.version}: No plans')
    else:
        for plan in plans:
            if plan.Month:
                print(f'  📅 {plan.Month.strftime("%b %Y")}:')
                print(f'    PlanDressMass = {plan.PlanDressMass:.1f}t')
                print(f'    Details:')
                print(f'      AvailableDays: {plan.AvailableDays}')
                print(f'      heatsperdays: {plan.heatsperdays}')
                print(f'      TonsPerHeat: {plan.TonsPerHeat}')
                print(f'      Yield: {plan.Yield}')
                print(f'      WasterPercentage: {plan.WasterPercentage}%')
            else:
                print(f'  ❌ Month=None: PlanDressMass = {plan.PlanDressMass}')
    
    print(f'\n🎯 SOLUTION: To get the red line in Xuzhou chart, you need to:')
    print(f'   1. Upload Master Data Plan for XUZ1 site')
    print(f'   2. Or copy Master Data Plan from another scenario')
    print(f'   3. The red line comes from MasterDataPlan.PlanDressMass calculation')

if __name__ == "__main__":
    check_xuzhou_pour_plan()
