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

def analyze_dewb135_replenishment():
    scenario = scenarios.objects.get(version='Jul 25 SPR Inv')
    replenishments = CalcualtedReplenishmentModel.objects.filter(
        version=scenario,
        Product='DEWB135-1'
    ).values('ShippingDate', 'ReplenishmentQty', 'Site', 'Location').order_by('ShippingDate', 'Site', 'Location')

    print('DEWB135-1 Detailed Replenishment Analysis:')
    print('Date\t\tQty\tSite\t\tLocation')
    print('-' * 70)
    
    date_site_totals = defaultdict(lambda: defaultdict(float))
    location_counts = defaultdict(int)
    
    for rep in replenishments:
        print(f'{rep["ShippingDate"]}\t{rep["ReplenishmentQty"]}\t{rep["Site"]}\t\t{rep["Location"]}')
        date_site_totals[rep["ShippingDate"]][rep["Site"]] += rep["ReplenishmentQty"] or 0
        location_counts[rep["Location"]] += 1

    print('\n' + '=' * 70)
    print('Location Distribution:')
    for location, count in sorted(location_counts.items()):
        print(f'{location}: {count} records')
    
    print('\n' + '=' * 70)
    print('Summary by Date and Site:')
    print('Date\t\tSite\t\tTotal Qty')
    print('-' * 50)
    grand_total = 0
    unique_combinations = set()
    
    for date in sorted(date_site_totals.keys()):
        for site, qty in date_site_totals[date].items():
            print(f'{date}\t{site}\t\t{qty}')
            grand_total += qty
            unique_combinations.add((date, site))

    print('-' * 50)
    print(f'GRAND TOTAL: {grand_total}')
    print(f'UNIQUE (DATE, SITE) COMBINATIONS: {len(unique_combinations)}')
    print(f'TOTAL RECORDS: {len(list(replenishments))}')
    
    # Check if there are multiple records for same date/site
    if len(list(replenishments)) > len(unique_combinations):
        print('\n⚠️  WARNING: Multiple records exist for same date/site combinations!')
        print('This suggests the aggregation is not working properly.')

if __name__ == "__main__":
    analyze_dewb135_replenishment()
