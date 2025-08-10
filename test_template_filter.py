#!/usr/bin/env python
import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from django.template import Template, Context
from website.models import MasterDataInventory, scenarios

# Get the data exactly like the view does
scenario = scenarios.objects.get(version='Jul 25 SPR')
inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
inventory_snapshot_date = inventory_snapshot.date_of_snapshot if inventory_snapshot else None

print(f'inventory_snapshot_date from view: {inventory_snapshot_date}')
print(f'Type: {type(inventory_snapshot_date)}')

# Test the template filter exactly
template_code = '{{ inventory_snapshot_date|date:"Y-m-d"|default:"" }}'
template = Template(template_code)
context = Context({'inventory_snapshot_date': inventory_snapshot_date})
result = template.render(context)

print(f'Template result: "{result}"')
print(f'Length: {len(result)}')
print(f'Is empty: {result == ""}')

# Also test truthiness
print(f'inventory_snapshot_date is truthy: {bool(inventory_snapshot_date)}')
if inventory_snapshot_date:
    print("Date is truthy in Python")
else:
    print("Date is falsy in Python - THIS IS THE PROBLEM!")
