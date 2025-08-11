#!/usr/bin/env python
"""
Debug script to check MonthlyPouredDataModel data
"""

import os
import django

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MonthlyPouredDataModel, scenarios

def debug_monthly_poured_data():
    """Debug the MonthlyPouredDataModel data"""
    print("=== MonthlyPouredDataModel Debug Report ===")
    
    # Check total records
    total_count = MonthlyPouredDataModel.objects.count()
    print(f"Total MonthlyPouredDataModel records: {total_count}")
    
    if total_count == 0:
        print("❌ No MonthlyPouredDataModel records found!")
        print("💡 You need to upload inventory data via upload_on_hand_stock to populate this model")
        return
    
    # Get scenarios that have poured data
    scenarios_with_data = MonthlyPouredDataModel.objects.values_list('version__version', flat=True).distinct()
    print(f"Scenarios with poured data: {list(scenarios_with_data)}")
    
    # Get available sites
    sites_with_data = MonthlyPouredDataModel.objects.values_list('site_name', flat=True).distinct()
    print(f"Sites with poured data: {list(sites_with_data)}")
    
    # Get available fiscal years
    fys_with_data = MonthlyPouredDataModel.objects.values_list('fiscal_year', flat=True).distinct()
    print(f"Fiscal years with poured data: {list(fys_with_data)}")
    
    # Show sample data for FY25 MTJ1
    print("\n=== Sample Data: FY25 MTJ1 ===")
    sample_records = MonthlyPouredDataModel.objects.filter(
        site_name='MTJ1',
        fiscal_year='FY25'
    ).order_by('version__version', 'year', 'month')[:10]
    
    for record in sample_records:
        print(f"  {record.version.version} - {record.month_year_display}: {record.monthly_tonnes} tonnes")
    
    print("\n=== Testing get_monthly_data_for_site_and_fy ===")
    # Test the function that's failing
    from website.customized_function import get_monthly_poured_data_for_site_and_fy
    
    # Get first scenario with data
    if scenarios_with_data:
        test_scenario = scenarios_with_data[0]
        print(f"Testing with scenario: {test_scenario}")
        
        result = get_monthly_poured_data_for_site_and_fy('MTJ1', 'FY25', test_scenario)
        print(f"Result: {result}")
        
        if result:
            print("✅ Function returned data successfully")
        else:
            print("❌ Function returned empty data")
    
    print("\n=== Debug Complete ===")

if __name__ == "__main__":
    debug_monthly_poured_data()
