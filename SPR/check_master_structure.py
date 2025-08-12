#!/usr/bin/env python3
"""Check MasterDataInventory structure and data"""

import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MasterDataInventory
from decimal import Decimal

print("=== Checking MasterDataInventory Structure ===")

# Get a sample record to see field structure
sample_record = MasterDataInventory.objects.filter(version="Aug 25 SP").first()

if sample_record:
    print("Available fields in MasterDataInventory:")
    for field in sample_record._meta.fields:
        field_value = getattr(sample_record, field.name, 'N/A')
        print(f"  {field.name}: {field_value}")
    
    print(f"\n=== MasterDataInventory Analysis ===")
    master_records = MasterDataInventory.objects.filter(version="Aug 25 SP")
    print(f"Total records: {master_records.count()}")
    
    # Group by product to understand the structure
    products = {}
    sites = set()
    
    for record in master_records[:100]:  # Sample first 100 records
        product = record.product
        site_name = record.site.SiteName if record.site else 'Unknown'
        sites.add(site_name)
        
        if product not in products:
            products[product] = {
                'count': 0,
                'total_cost': Decimal('0'),
                'sites': set()
            }
        
        products[product]['count'] += 1
        products[product]['sites'].add(site_name)
        
        if record.cost_aud:
            products[product]['total_cost'] += Decimal(str(record.cost_aud))
    
    print(f"\nUnique sites (from sample): {sorted(list(sites))}")
    print(f"Number of unique products (from sample): {len(products)}")
    
    # Show top products by cost
    print(f"\nTop products by cost (from sample):")
    sorted_products = sorted(products.items(), key=lambda x: float(x[1]['total_cost']), reverse=True)
    for product, data in sorted_products[:10]:
        print(f"  {product[:50]}...: ${data['total_cost']:,.2f} ({data['count']} records)")
    
    # Check how product groups should be determined
    print(f"\n=== Product Grouping Analysis ===")
    print("MasterDataInventory doesn't have 'parent_product_group' field")
    print("The product grouping likely happens during snapshot creation")
    print("Need to check the upload_on_hand_stock function in views.py")

else:
    print("❌ No MasterDataInventory records found for Aug 25 SP")
