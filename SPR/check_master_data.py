#!/usr/bin/env python3
"""Check the MasterDataInventory source data"""

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MasterDataInventory
from decimal import Decimal

print("=== Checking MasterDataInventory Source Data ===")

# Check what product groups exist in MasterDataInventory
master_records = MasterDataInventory.objects.filter(version="Aug 25 SP")
print(f"Total MasterDataInventory records: {master_records.count()}")

if master_records.count() > 0:
    # Group by parent_product_group
    product_groups = {}
    
    for record in master_records:
        group = record.parent_product_group
        if group not in product_groups:
            product_groups[group] = {
                'count': 0, 
                'total_value': Decimal('0'),
                'sites': set(),
                'sample_site': None
            }
        product_groups[group]['count'] += 1
        product_groups[group]['sites'].add(record.site_name)
        product_groups[group]['sample_site'] = record.site_name
        
        # Sum up inventory values
        if hasattr(record, 'inventory_value_aud') and record.inventory_value_aud:
            product_groups[group]['total_value'] += Decimal(str(record.inventory_value_aud))
    
    print(f"\nMasterDataInventory by Product Group:")
    print("=" * 80)
    master_total = Decimal('0')
    
    for group, data in sorted(product_groups.items(), key=lambda x: float(x[1]['total_value']), reverse=True):
        master_total += data['total_value']
        sites_list = sorted(list(data['sites']))
        print(f"{group}:")
        print(f"  Records: {data['count']}")
        print(f"  Value: ${data['total_value']:,.2f}")
        print(f"  Sites: {', '.join(sites_list[:5])}")  # Show first 5 sites
        if len(sites_list) > 5:
            print(f"         ... and {len(sites_list) - 5} more sites")
        print()
    
    print(f"Total from MasterDataInventory: ${master_total:,.2f}")
    
    print(f"\n=== Missing Product Groups Analysis ===")
    print("Expected groups from your verification:")
    expected_groups = [
        'Crawler Systems', 'Fixed Plant', 'GET', 'Maintenance Spares', 
        'Mill Liners', 'Mining Fabrication', 'Mining Other', 'Rail', 
        'Raw Materials', 'Sugar', 'Wear Pipe'
    ]
    
    master_groups = set(product_groups.keys())
    expected_set = set(expected_groups)
    
    missing_groups = expected_set - master_groups
    extra_groups = master_groups - expected_set
    
    print(f"Missing from MasterDataInventory: {sorted(missing_groups) if missing_groups else 'None'}")
    print(f"Extra in MasterDataInventory: {sorted(extra_groups) if extra_groups else 'None'}")
    
else:
    print("❌ No MasterDataInventory records found!")

print(f"\n=== Investigation Summary ===")
print("The snapshot creation process uses MasterDataInventory as the source.")
print("If product groups are missing from the snapshot, they're likely missing from MasterDataInventory too.")
print("This suggests the original data upload/import process had issues.")
