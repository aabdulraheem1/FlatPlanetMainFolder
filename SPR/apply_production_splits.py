"""
Apply production allocation splits to CalculatedProductionModel records.

This script will:
1. Find existing CalculatedProductionModel records for a product
2. Check if allocation splits exist for that product  
3. Delete old records and create new split records with allocated quantities
4. Preserve all other field values (dates, scenarios, etc.)
"""

import os
import sys
import django
from django.db import transaction
from collections import defaultdict
from decimal import Decimal

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import *

def apply_production_splits(product_code, dry_run=True):
    """
    Apply allocation splits to existing CalculatedProductionModel records.
    
    Args:
        product_code (str): Product code to apply splits for
        dry_run (bool): If True, shows what would be done without making changes
    """
    
    print(f"=== APPLYING PRODUCTION SPLITS FOR {product_code} ===")
    
    # 1. Get existing production records
    existing_records = CalculatedProductionModel.objects.filter(product__Product=product_code)
    print(f"Found {existing_records.count()} existing production records")
    
    if existing_records.count() == 0:
        print("No existing records to split")
        return
    
    # 2. Get allocation percentages
    allocations = ProductionAllocationModel.objects.filter(product__Product=product_code)
    print(f"Found {allocations.count()} allocation records")
    
    if allocations.count() == 0:
        print("No allocation splits defined - nothing to do")
        return
    
    # 3. Group allocations by month
    month_allocations = defaultdict(list)
    for alloc in allocations:
        month_allocations[alloc.month_year].append({
            'site': alloc.site,
            'percentage': alloc.allocation_percentage
        })
    
    print(f"Allocation splits by month:")
    for month, splits in month_allocations.items():
        total_pct = sum(s['percentage'] for s in splits)
        site_list = [f"{s['site'].SiteName}({s['percentage']}%)" for s in splits]
        print(f"  {month}: {', '.join(site_list)} (Total: {total_pct}%)")
    
    # 4. Group existing records by month (using pouring_date)
    month_records = defaultdict(list)
    for record in existing_records:
        month_key = record.pouring_date.strftime('%Y-%m') if record.pouring_date else 'unknown'
        month_records[month_key].append(record)
    
    print(f"\nExisting records by month:")
    for month, records in month_records.items():
        total_qty = sum(r.production_quantity or 0 for r in records)
        total_tonnes = sum(r.tonnes or 0 for r in records)
        sites = set(r.site.SiteName for r in records)
        print(f"  {month}: {len(records)} records, {total_qty} qty, {total_tonnes:.2f} tonnes, sites: {sorted(sites)}")
    
    # 5. Apply splits for each month that has both records and allocations
    records_to_delete = []
    records_to_create = []
    
    for month, records in month_records.items():
        if month not in month_allocations:
            print(f"\nNo allocations for {month} - keeping existing records")
            continue
            
        splits = month_allocations[month]
        total_pct = sum(s['percentage'] for s in splits)
        
        if abs(total_pct - 100.0) > 0.01:  # Allow small rounding errors
            print(f"\nWARNING: {month} allocations don't sum to 100% ({total_pct}%) - skipping")
            continue
            
        print(f"\nProcessing {month}:")
        
        # Calculate totals to split
        total_qty = sum(r.production_quantity or 0 for r in records)
        total_tonnes = sum(r.tonnes or 0 for r in records)
        total_price_aud = sum(r.price_aud or 0 for r in records)  
        total_cost_aud = sum(r.cost_aud or 0 for r in records)
        total_production_aud = sum(r.production_aud or 0 for r in records)
        total_revenue_aud = sum(r.revenue_aud or 0 for r in records)
        
        print(f"  Totals to split: {total_qty} qty, {total_tonnes:.2f} tonnes")
        
        # Mark existing records for deletion
        records_to_delete.extend(records)
        
        # Create new split records  
        for split in splits:
            pct = split['percentage'] / 100.0
            site = split['site']
            
            # Use first record as template for non-quantity fields
            template = records[0]
            
            new_record_data = {
                'version': template.version,
                'product': template.product,
                'site': site,
                'pouring_date': template.pouring_date,
                'production_quantity': total_qty * pct,
                'tonnes': total_tonnes * pct,
                'product_group': template.product_group,
                'parent_product_group': template.parent_product_group,
                'price_aud': total_price_aud * pct,
                'cost_aud': total_cost_aud * pct,
                'production_aud': total_production_aud * pct,
                'revenue_aud': total_revenue_aud * pct,
                'latest_customer_invoice': template.latest_customer_invoice,
                'latest_customer_invoice_date': template.latest_customer_invoice_date,
                'is_outsourced': template.is_outsourced,
            }
            
            records_to_create.append(new_record_data)
            
            print(f"    -> {site.SiteName}: {total_qty * pct:.1f} qty ({split['percentage']}%)")
    
    print(f"\nSUMMARY:")
    print(f"  Records to delete: {len(records_to_delete)}")
    print(f"  Records to create: {len(records_to_create)}")
    
    if dry_run:
        print(f"\nDRY RUN - No changes made. Set dry_run=False to apply changes.")
        return
        
    # 6. Apply changes in a transaction
    try:
        with transaction.atomic():
            # Delete old records
            deleted_count = len(records_to_delete)
            for record in records_to_delete:
                record.delete()
            print(f"Deleted {deleted_count} existing records")
            
            # Create new records
            created_records = []
            for record_data in records_to_create:
                new_record = CalculatedProductionModel.objects.create(**record_data)
                created_records.append(new_record)
            
            print(f"Created {len(created_records)} new split records")
            print("SUCCESS: Production splits applied!")
            
    except Exception as e:
        print(f"ERROR applying splits: {e}")
        raise

if __name__ == "__main__":
    # Test with P2P22XAH
    product_code = "P2P22XAH"
    
    print("=== DRY RUN ===")
    apply_production_splits(product_code, dry_run=True)
    
    print("\n" + "="*50)
    user_input = input("Apply these changes? (y/N): ")
    
    if user_input.lower() == 'y':
        print("\n=== APPLYING CHANGES ===")
        apply_production_splits(product_code, dry_run=False)
    else:
        print("Changes cancelled.")
