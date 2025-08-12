#!/usr/bin/env python3
"""Analyze what went wrong with the inventory snapshot data"""

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import OpeningInventorySnapshot, MasterDataInventory
from datetime import datetime

print("=== Analyzing Inventory Snapshot Data Issues ===")

july_date = datetime(2025, 7, 31).date()
snapshot_records = OpeningInventorySnapshot.objects.filter(snapshot_date=july_date)

print(f"Current OpeningInventorySnapshot records (July 31): {snapshot_records.count()}")
print(f"Expected: 11 product groups from your original verification")
print(f"Actual: {snapshot_records.count()} product groups")

print(f"\n=== Current Snapshot Data ===")
current_total = 0
for record in snapshot_records:
    current_total += record.inventory_value_aud
    print(f"  {record.parent_product_group}: ${record.inventory_value_aud:,.2f}")

print(f"\nCurrent Total: ${current_total:,.2f}")
print(f"Expected Total: $197,259,603.27 (from your original verification)")
print(f"Difference: ${abs(current_total - 197259603.27):,.2f}")

# Check MasterDataInventory to see what the upload process should have created
print(f"\n=== MasterDataInventory Analysis ===")
master_records = MasterDataInventory.objects.filter(version="Aug 25 SP")
print(f"MasterDataInventory records: {master_records.count()}")

if master_records.count() > 0:
    # Group by product groups
    product_groups = {}
    for record in master_records:
        group = record.parent_product_group
        if group not in product_groups:
            product_groups[group] = {'count': 0, 'total_value': 0}
        product_groups[group]['count'] += 1
        if hasattr(record, 'inventory_value_aud') and record.inventory_value_aud:
            product_groups[group]['total_value'] += float(record.inventory_value_aud)
    
    print(f"\nMasterDataInventory by Product Group:")
    for group, data in sorted(product_groups.items(), key=lambda x: x[1]['total_value'], reverse=True):
        print(f"  {group}: {data['count']} records, ${data['total_value']:,.2f}")

print(f"\n=== Recommendations ===")
print("1. The current snapshot data appears to be incomplete (only 7 vs expected 11 product groups)")
print("2. The Mill Liners value of $32M seems high - should be around $16M")
print("3. Missing product groups: Maintenance Spares, Raw Materials, Sugar, Wear Pipe")
print("\n🔧 Suggested fixes:")
print("   A. Re-upload the inventory data from the original source")
print("   B. Check if the PowerBI data source is correct")
print("   C. Verify the upload process didn't skip some product groups")
print("   D. Check if there are filters/conditions affecting the data")
