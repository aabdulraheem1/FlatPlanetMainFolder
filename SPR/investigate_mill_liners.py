#!/usr/bin/env python3
"""Investigate Mill Liners value discrepancy"""

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import OpeningInventorySnapshot
from datetime import datetime

print("=== Investigating Mill Liners Value Discrepancy ===")

# Check all Mill Liners records across all dates
print("1. Checking ALL Mill Liners records in database:")
all_mill_liners = OpeningInventorySnapshot.objects.filter(parent_product_group="Mill Liners")
print(f"Total Mill Liners records: {all_mill_liners.count()}")

for record in all_mill_liners:
    print(f"  Date: {record.snapshot_date}, Value: ${record.inventory_value_aud:,.2f}, ID: {record.id}")

# Check all records for July 31
print(f"\n2. All records for 2025-07-31:")
july_records = OpeningInventorySnapshot.objects.filter(snapshot_date=datetime(2025, 7, 31).date())
print(f"Total records: {july_records.count()}")

product_totals = {}
for record in july_records:
    if record.parent_product_group not in product_totals:
        product_totals[record.parent_product_group] = []
    product_totals[record.parent_product_group].append({
        'id': record.id, 
        'value': record.inventory_value_aud,
        'created_at': record.created_at
    })

for product, records_list in product_totals.items():
    total_value = sum(r['value'] for r in records_list)
    print(f"  {product}: ${total_value:,.2f} ({len(records_list)} records)")
    if len(records_list) > 1:
        print(f"    ⚠️  Multiple records detected:")
        for r in records_list:
            print(f"      ID {r['id']}: ${r['value']:,.2f} (created {r['created_at']})")

# Check if there are any June 30 records left
print(f"\n3. Checking for any remaining June 30 records:")
june_records = OpeningInventorySnapshot.objects.filter(snapshot_date=datetime(2025, 6, 30).date())
print(f"June 30 records: {june_records.count()}")

if june_records.count() > 0:
    for record in june_records:
        print(f"  {record.parent_product_group}: ${record.inventory_value_aud:,.2f}")

print(f"\n4. Total inventory across all July 31 records:")
total_july = sum(record.inventory_value_aud for record in july_records)
print(f"${total_july:,.2f} AUD")

print(f"\nExpected Mill Liners value was around $16M, but seeing $32M - investigating...")
