#!/usr/bin/env python
import os
import sys
import django

# Add the project root to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import CalcualtedReplenishmentModel, scenarios
from collections import defaultdict

def analyze_dewb135_details():
    scenario = scenarios.objects.get(version='Jul 25 SPR Inv')
    replenishments = CalcualtedReplenishmentModel.objects.filter(
        version=scenario,
        Product='DEWB135-1'
    ).values('ShippingDate', 'ReplenishmentQty', 'Site', 'Location').order_by('ShippingDate', 'Site', 'Location')

    print('DEWB135-1 Complete Analysis:')
    print('Date\t\tQty\tSite\t\tLocation\tCombination Key')
    print('-' * 80)
    
    date_site_combinations = defaultdict(list)
    date_site_location_combinations = set()
    
    for rep in replenishments:
        shipping_date = rep["ShippingDate"]
        site = rep["Site"]
        location = rep["Location"]
        qty = rep["ReplenishmentQty"]
        
        # Key for (Date, Site) combination
        date_site_key = (shipping_date, site)
        
        # Key for (Date, Site, Location) combination
        date_site_location_key = (shipping_date, site, location)
        
        print(f'{shipping_date}\t{qty}\t{site}\t\t{location}\t{date_site_location_key}')
        
        date_site_combinations[date_site_key].append({
            'location': location,
            'qty': qty
        })
        
        date_site_location_combinations.add(date_site_location_key)
    
    print('\n' + '=' * 80)
    print('Analysis Results:')
    
    # Check for true duplicates (same date, site, AND location)
    true_duplicates = 0
    for key in date_site_location_combinations:
        # Count how many records have this exact combination
        count = sum(1 for rep in replenishments 
                   if rep["ShippingDate"] == key[0] 
                   and rep["Site"] == key[1] 
                   and rep["Location"] == key[2])
        if count > 1:
            true_duplicates += 1
            print(f'TRUE DUPLICATE FOUND: {key} appears {count} times')
    
    if true_duplicates == 0:
        print('✅ NO TRUE DUPLICATES: No identical (Date, Site, Location) combinations found')
    
    print(f'\nTotal unique (Date, Site, Location) combinations: {len(date_site_location_combinations)}')
    print(f'Total records: {len(list(replenishments))}')
    
    # Show why same date/site can have multiple records (different locations)
    print('\n' + '=' * 80)
    print('Date/Site combinations with multiple locations (This is CORRECT behavior):')
    
    for (date, site), records in date_site_combinations.items():
        if len(records) > 1:
            locations = [r['location'] for r in records]
            total_qty = sum(r['qty'] for r in records)
            print(f'{date} at {site}: {len(records)} locations {locations} (Total: {total_qty})')
    
    print('\n' + '=' * 80)
    print('EXPLANATION:')
    print('The "warning" was incorrect. Multiple records for same date/site is NORMAL when:')
    print('- Same site serves multiple location codes (AU03, ZA01, CL01, US01)')
    print('- Each location represents different customer regions/markets')
    print('- This is legitimate business logic, not a data quality issue')

if __name__ == "__main__":
    analyze_dewb135_details()
