#!/usr/bin/env python3
"""Fix duplicate inventory snapshot records"""

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import OpeningInventorySnapshot
from datetime import datetime

print("=== Fixing Duplicate Inventory Records ===")

june_date = datetime(2025, 6, 30).date()
july_date = datetime(2025, 7, 31).date()

# Check current state
june_records = OpeningInventorySnapshot.objects.filter(snapshot_date=june_date)
july_records = OpeningInventorySnapshot.objects.filter(snapshot_date=july_date)

print(f"Current state:")
print(f"  June 30 records: {june_records.count()}")
print(f"  July 31 records: {july_records.count()}")

# The correct approach: Delete June 30 records and keep July 31 records
print(f"\nRemoving duplicate June 30 records...")

june_product_groups = list(june_records.values_list('parent_product_group', flat=True))
print(f"June 30 product groups to delete: {june_product_groups}")

for record in june_records:
    print(f"  Deleting: {record.parent_product_group} - ${record.inventory_value_aud:,.2f} (ID: {record.id})")

deleted_count = june_records.delete()
print(f"✅ Deleted {deleted_count[0]} June 30 records")

# Verify final state
remaining_records = OpeningInventorySnapshot.objects.all()
print(f"\nFinal verification:")
print(f"  Total remaining records: {remaining_records.count()}")

july_final = OpeningInventorySnapshot.objects.filter(snapshot_date=july_date)
print(f"  July 31 records: {july_final.count()}")

if july_final.count() > 0:
    print(f"\n📦 Final inventory by product group (July 31, 2025):")
    product_totals = {}
    for record in july_final:
        product_totals[record.parent_product_group] = record.inventory_value_aud
    
    for product, value in sorted(product_totals.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {product}: ${value:,.2f} AUD")
    
    total_value = sum(product_totals.values())
    print(f"\n💰 Total Inventory Value: ${total_value:,.2f} AUD")

print(f"\n✅ Duplicate records cleaned up! Now only July 31 records remain.")
