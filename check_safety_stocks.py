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

from website.models import MasterDataSafetyStocks, scenarios

def check_safety_stocks():
    scenario = scenarios.objects.get(version='Jul 25 SPR Inv')
    safety_stocks = MasterDataSafetyStocks.objects.filter(
        version=scenario,
        PartNum='DEWB135-1'
    ).values('Plant', 'PartNum', 'MinimumQty', 'SafetyQty')

    print('DEWB135-1 Safety Stock Settings:')
    found = False
    for ss in safety_stocks:
        print(f'Plant: {ss["Plant"]}, MinQty: {ss["MinimumQty"]}, SafetyQty: {ss["SafetyQty"]}')
        found = True

    if not found:
        print('No safety stock settings found for DEWB135-1')
        
    # Also check how many safety stocks are defined in total
    total_safety_stocks = MasterDataSafetyStocks.objects.filter(version=scenario).count()
    print(f'\nTotal safety stock records in scenario: {total_safety_stocks}')
    
    # Check a few examples
    examples = MasterDataSafetyStocks.objects.filter(version=scenario)[:5]
    print('\nExample safety stock records:')
    for ex in examples:
        print(f'Plant: {ex.Plant}, Part: {ex.PartNum}, MinQty: {ex.MinimumQty}, SafetyQty: {ex.SafetyQty}')

if __name__ == "__main__":
    check_safety_stocks()
