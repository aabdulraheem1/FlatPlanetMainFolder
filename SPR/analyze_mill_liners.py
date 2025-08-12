#!/usr/bin/env python3
"""Check OpeningInventorySnapshot data for Mill Liners in Aug 25 SP scenario"""

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import OpeningInventorySnapshot
from datetime import datetime

print("=== OpeningInventorySnapshot Data Analysis ===")

# Get the snapshot date for Aug 25 SP scenario
snapshot_date = datetime(2025, 7, 31).date()
print(f"Analyzing data for snapshot_date: {snapshot_date}")

# Query for Mill Liners records
mill_liners_records = OpeningInventorySnapshot.objects.filter(
    snapshot_date=snapshot_date,
    parent_product_group="Mill Liners"
)

print(f"\nMill Liners records found: {mill_liners_records.count()}")

if mill_liners_records.count() > 0:
    print("\n=== Mill Liners Record Details ===")
    for i, record in enumerate(mill_liners_records, 1):
        print(f"\nRecord {i}:")
        print(f"  ID: {record.id}")
        print(f"  Parent Product Group: {record.parent_product_group}")
        print(f"  Snapshot Date: {record.snapshot_date}")
        print(f"  Inventory Value AUD: ${record.inventory_value_aud:,.2f}")
        print(f"  Source System: {record.source_system}")
        print(f"  Data Freshness Hours: {record.data_freshness_hours}")
        print(f"  Created At: {record.created_at}")
        print(f"  Created By User: {record.created_by_user}")
        print(f"  Updated At: {record.updated_at}")
        print(f"  Refresh Reason: {record.refresh_reason}")
        print(f"  Scenarios Using This Snapshot: {record.scenarios_using_this_snapshot}")

    # Show total for Mill Liners
    total_mill_liners = sum(record.inventory_value_aud for record in mill_liners_records)
    print(f"\n💰 Total Mill Liners Inventory: ${total_mill_liners:,.2f} AUD")
    
    # Show comparison with other product groups
    print(f"\n=== Comparison with Other Product Groups ===")
    all_records = OpeningInventorySnapshot.objects.filter(snapshot_date=snapshot_date)
    
    product_totals = {}
    for record in all_records:
        if record.parent_product_group not in product_totals:
            product_totals[record.parent_product_group] = 0
        product_totals[record.parent_product_group] += record.inventory_value_aud
    
    # Sort by value descending
    sorted_products = sorted(product_totals.items(), key=lambda x: x[1], reverse=True)
    
    for product, value in sorted_products:
        percentage = (value / sum(product_totals.values())) * 100
        highlight = " ← Mill Liners" if product == "Mill Liners" else ""
        print(f"  {product}: ${value:,.2f} AUD ({percentage:.1f}%){highlight}")
        
else:
    print("❌ No Mill Liners records found for this snapshot date")
    
    # Show what product groups do exist
    all_records = OpeningInventorySnapshot.objects.filter(snapshot_date=snapshot_date)
    if all_records.count() > 0:
        product_groups = list(all_records.values_list('parent_product_group', flat=True).distinct())
        print(f"\nAvailable product groups: {product_groups}")

print(f"\n=== Field Information ===")
print("Fields available in OpeningInventorySnapshot model:")
field_names = [field.name for field in OpeningInventorySnapshot._meta.fields]
for field_name in field_names:
    print(f"  - {field_name}")
