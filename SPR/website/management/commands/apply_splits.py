from django.core.management.base import BaseCommand
from django.db import transaction
from website.models import *
from collections import defaultdict
from datetime import datetime

class Command(BaseCommand):
    help = 'Apply production allocation splits to CalculatedProductionModel records'

    def add_arguments(self, parser):
        parser.add_argument('product_code', type=str, help='Product code to apply splits for')
        parser.add_argument('--scenario', type=str, required=True, help='Scenario/version to apply splits for')
        parser.add_argument('--apply', action='store_true', help='Apply changes (default is dry-run)')

    def handle(self, *args, **options):
        product_code = options['product_code']
        scenario = options['scenario']
        apply_changes = options['apply']
        
        self.stdout.write(f"=== {'APPLYING' if apply_changes else 'ANALYZING'} PRODUCTION SPLITS FOR {product_code} IN {scenario} ===")
        
        # 1. Get existing production records FOR THE SPECIFIC SCENARIO
        existing_records = CalculatedProductionModel.objects.filter(
            product__Product=product_code,
            version__version=scenario
        )
        self.stdout.write(f"Found {existing_records.count()} existing production records in {scenario}")
        
        if existing_records.count() == 0:
            self.stdout.write(f"No existing records to split in {scenario}. Run calculate_model first?")
            return
        
        # 2. Get allocation percentages FOR THE SPECIFIC SCENARIO
        allocations = ProductionAllocationModel.objects.filter(
            product__Product=product_code,
            version__version=scenario
        )
        self.stdout.write(f"Found {allocations.count()} allocation records in {scenario}")
        
        if allocations.count() == 0:
            self.stdout.write("No allocation splits defined - nothing to do")
            return
        
        # 3. Group allocations by month
        month_allocations = defaultdict(list)
        for alloc in allocations:
            month_allocations[alloc.month_year].append({
                'site': alloc.site,
                'percentage': alloc.allocation_percentage
            })
        
        self.stdout.write("\nAllocation splits by month:")
        for month, splits in month_allocations.items():
            total_pct = sum(s['percentage'] for s in splits)
            site_list = [f"{s['site'].SiteName}({s['percentage']}%)" for s in splits]
            self.stdout.write(f"  {month}: {', '.join(site_list)} (Total: {total_pct}%)")
        
        # 4. Group existing records by month (using pouring_date)
        month_records = defaultdict(list)
        for record in existing_records:
            if record.pouring_date:
                month_key = record.pouring_date.strftime('%Y-%m')
            else:
                month_key = 'unknown'
            month_records[month_key].append(record)
        
        self.stdout.write("\nExisting records by month:")
        for month, records in month_records.items():
            total_qty = sum(r.production_quantity or 0 for r in records)
            total_tonnes = sum(r.tonnes or 0 for r in records)
            sites = set(r.site.SiteName for r in records)
            self.stdout.write(f"  {month}: {len(records)} records, {total_qty} qty, {total_tonnes:.2f} tonnes, sites: {sorted(sites)}")
        
        # 5. Create month mapping function
        def map_allocation_month_to_production(alloc_month):
            """Convert allocation format 'Aug-25' to production format '2025-08'"""
            try:
                # Parse "Aug-25" format
                date_obj = datetime.strptime(alloc_month, '%b-%y')
                # Convert to "2025-08" format  
                return date_obj.strftime('%Y-%m')
            except:
                return None
        
        # 6. Apply splits for matching months
        records_to_delete = []
        records_to_create = []
        
        for alloc_month, splits in month_allocations.items():
            prod_month = map_allocation_month_to_production(alloc_month)
            
            if not prod_month or prod_month not in month_records:
                self.stdout.write(f"\nNo production records found for allocation month {alloc_month} (mapped to {prod_month})")
                continue
                
            records = month_records[prod_month]
            total_pct = sum(s['percentage'] for s in splits)
            
            if abs(total_pct - 100.0) > 0.01:  # Allow small rounding errors
                self.stdout.write(f"\nWARNING: {alloc_month} allocations don't sum to 100% ({total_pct}%) - skipping")
                continue
                
            self.stdout.write(f"\nProcessing {alloc_month} -> {prod_month}:")
            
            # Calculate totals to split
            total_qty = sum(r.production_quantity or 0 for r in records)
            total_tonnes = sum(r.tonnes or 0 for r in records)
            total_price_aud = sum(r.price_aud or 0 for r in records)  
            total_cost_aud = sum(r.cost_aud or 0 for r in records)
            total_production_aud = sum(r.production_aud or 0 for r in records)
            total_revenue_aud = sum(r.revenue_aud or 0 for r in records)
            
            self.stdout.write(f"  Totals to split: {total_qty} qty, {total_tonnes:.2f} tonnes")
            
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
                
                self.stdout.write(f"    -> {site.SiteName}: {total_qty * pct:.1f} qty ({split['percentage']}%)")
        
        self.stdout.write(f"\nSUMMARY:")
        self.stdout.write(f"  Records to delete: {len(records_to_delete)}")
        self.stdout.write(f"  Records to create: {len(records_to_create)}")
        
        if not apply_changes:
            self.stdout.write(f"\nDRY RUN - No changes made. Use --apply to apply changes.")
            return
            
        # 7. Apply changes in a transaction
        try:
            with transaction.atomic():
                # Delete old records
                deleted_count = len(records_to_delete)
                for record in records_to_delete:
                    record.delete()
                self.stdout.write(self.style.SUCCESS(f"Deleted {deleted_count} existing records"))
                
                # Create new records
                created_records = []
                for record_data in records_to_create:
                    new_record = CalculatedProductionModel.objects.create(**record_data)
                    created_records.append(new_record)
                
                self.stdout.write(self.style.SUCCESS(f"Created {len(created_records)} new split records"))
                self.stdout.write(self.style.SUCCESS("SUCCESS: Production splits applied!"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"ERROR applying splits: {e}"))
            raise
