#!/usr/bin/env python
import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MasterDataInventory, scenarios

print('=== TESTING inventory_snapshot_date for Jul 25 SPR ===')

# Get the scenario
try:
    scenario = scenarios.objects.get(version='Jul 25 SPR')
    print(f'✅ Scenario found: {scenario.version}')
except scenarios.DoesNotExist:
    print('❌ Scenario "Jul 25 SPR" not found')
    exit()

# Test the exact logic from the view
inventory_snapshot_date = None
try:
    inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
    print(f'Query: MasterDataInventory.objects.filter(version={scenario})')
    print(f'Result: {inventory_snapshot}')
    
    if inventory_snapshot:
        print(f'✅ MasterDataInventory record found')
        print(f'Raw date_of_snapshot: {inventory_snapshot.date_of_snapshot}')
        print(f'Type: {type(inventory_snapshot.date_of_snapshot)}')
        
        inventory_snapshot_date = inventory_snapshot.date_of_snapshot
        print(f'inventory_snapshot_date assigned: {inventory_snapshot_date}')
        
        # Test template formatting
        formatted_date = inventory_snapshot_date.strftime('%d %B %Y')
        print(f'Formatted for template: {formatted_date}')
    else:
        print('❌ No MasterDataInventory record found for this scenario')
        
        # Check total count
        total_count = MasterDataInventory.objects.filter(version=scenario).count()
        print(f'Total MasterDataInventory records for this scenario: {total_count}')
        
        # Check if any MasterDataInventory exists at all
        all_count = MasterDataInventory.objects.count()
        print(f'Total MasterDataInventory records in database: {all_count}')
        
        # Check what scenarios exist in MasterDataInventory
        unique_versions = MasterDataInventory.objects.values_list('version__version', flat=True).distinct()
        print(f'Available scenarios in MasterDataInventory: {list(unique_versions)}')
        
except Exception as e:
    print(f'❌ Exception occurred: {e}')
    import traceback
    traceback.print_exc()

print(f'Final inventory_snapshot_date value: {inventory_snapshot_date}')
print(f'Boolean evaluation: {bool(inventory_snapshot_date)}')

# Also test Django template evaluation
if inventory_snapshot_date:
    from django.template import Template, Context
    template_code = '{{ inventory_snapshot_date|date:"d F Y" }}'
    template = Template(template_code)
    context = Context({'inventory_snapshot_date': inventory_snapshot_date})
    result = template.render(context)
    print(f'Django template result: "{result}"')
