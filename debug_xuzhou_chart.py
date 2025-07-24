#!/usr/bin/env python
"""
Debug chart generation for XUZ1 site in Jul 25 SPR Inv scenario
"""
import os
import sys
import django
from datetime import date

# Setup Django environment
sys.path.append('.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.customized_function import get_monthly_pour_plan_for_site, get_production_data_by_group

def debug_xuzhou_chart():
    print('=== Debugging XUZ1 Chart Generation in Jul 25 SPR Inv ===')
    
    scenario_version = 'Jul 25 SPR Inv'
    site_name = 'XUZ1'
    
    print(f'\n1. Getting chart data for {site_name}...')
    try:
        chart_data = get_production_data_by_group(site_name, scenario_version)
        print(f'✅ Chart data generated successfully')
        print(f'   Chart labels: {chart_data.get("labels", [])}')
        print(f'   Number of datasets: {len(chart_data.get("datasets", []))}')
        
        if chart_data.get('labels'):
            print(f'   First few labels: {chart_data["labels"][:5]}')
            print(f'   Last few labels: {chart_data["labels"][-5:]}')
    except Exception as e:
        print(f'❌ Error generating chart data: {e}')
        return
    
    print(f'\n2. Getting monthly pour plan for {site_name}...')
    try:
        monthly_pour_plan = get_monthly_pour_plan_for_site(site_name, scenario_version, chart_data['labels'])
        print(f'✅ Monthly pour plan generated successfully')
        print(f'   Pour plan data: {monthly_pour_plan}')
        
        # Match chart labels with pour plan values
        print(f'\n3. Chart Labels vs Pour Plan Values:')
        for i, (label, value) in enumerate(zip(chart_data['labels'], monthly_pour_plan)):
            if i < 10:  # Show first 10 months
                print(f'   {label}: {value}t')
            elif i == 10:
                print(f'   ... (showing first 10 months)')
                break
                
    except Exception as e:
        print(f'❌ Error generating pour plan: {e}')
        return
    
    print(f'\n4. Checking for zero values (which would make red line invisible):')
    zero_months = [i for i, val in enumerate(monthly_pour_plan) if val == 0]
    if zero_months:
        print(f'   ⚠️  Found {len(zero_months)} months with zero pour plan values')
        for idx in zero_months[:5]:  # Show first 5 zero months
            if idx < len(chart_data['labels']):
                print(f'      {chart_data["labels"][idx]}: 0t')
    else:
        print(f'   ✅ No zero values found in pour plan')
    
    # Check for July-October 2025 specifically
    print(f'\n5. Checking specific months Jul-Oct 2025:')
    target_months = ['2025-07', '2025-08', '2025-09', '2025-10']
    for target in target_months:
        if target in chart_data['labels']:
            idx = chart_data['labels'].index(target)
            pour_value = monthly_pour_plan[idx] if idx < len(monthly_pour_plan) else 0
            print(f'   {target}: {pour_value}t')
        else:
            print(f'   {target}: NOT FOUND in chart labels')

if __name__ == "__main__":
    debug_xuzhou_chart()
