from django.core.management.base import BaseCommand
from django.db import transaction
from website.models import *

class Command(BaseCommand):
    help = 'Copy production records from one scenario to another'

    def add_arguments(self, parser):
        parser.add_argument('product_code', type=str, help='Product code to copy')
        parser.add_argument('--from_scenario', type=str, required=True, help='Source scenario')
        parser.add_argument('--to_scenario', type=str, required=True, help='Target scenario')
        parser.add_argument('--apply', action='store_true', help='Apply changes (default is dry-run)')

    def handle(self, *args, **options):
        product_code = options['product_code']
        from_scenario = options['from_scenario']
        to_scenario = options['to_scenario']
        apply_changes = options['apply']
        
        self.stdout.write(f"=== {'COPYING' if apply_changes else 'ANALYZING'} {product_code} FROM {from_scenario} TO {to_scenario} ===")
        
        # Get source records
        source_records = CalculatedProductionModel.objects.filter(
            product__Product=product_code,
            version__version=from_scenario
        )
        self.stdout.write(f"Found {source_records.count()} records in {from_scenario}")
        
        if source_records.count() == 0:
            self.stdout.write(f"No records found in {from_scenario}")
            return
            
        # Get target scenario
        try:
            target_scenario = scenarios.objects.get(version=to_scenario)
        except scenarios.DoesNotExist:
            self.stdout.write(f"Target scenario '{to_scenario}' not found!")
            return
            
        # Check existing records in target
        existing_records = CalculatedProductionModel.objects.filter(
            product__Product=product_code,
            version__version=to_scenario
        )
        self.stdout.write(f"Target scenario has {existing_records.count()} existing records")
        
        if not apply_changes:
            self.stdout.write(f"DRY RUN - Would copy {source_records.count()} records to {to_scenario}")
            return
            
        # Copy records
        try:
            with transaction.atomic():
                copied_count = 0
                for record in source_records:
                    # Create new record with same data but different version
                    new_record = CalculatedProductionModel.objects.create(
                        version=target_scenario,
                        product=record.product,
                        site=record.site,
                        pouring_date=record.pouring_date,
                        production_quantity=record.production_quantity,
                        tonnes=record.tonnes,
                        product_group=record.product_group,
                        parent_product_group=record.parent_product_group,
                        price_aud=record.price_aud,
                        cost_aud=record.cost_aud,
                        production_aud=record.production_aud,
                        revenue_aud=record.revenue_aud,
                        latest_customer_invoice=record.latest_customer_invoice,
                        latest_customer_invoice_date=record.latest_customer_invoice_date,
                        is_outsourced=record.is_outsourced,
                    )
                    copied_count += 1
                    
                self.stdout.write(self.style.SUCCESS(f"Successfully copied {copied_count} records to {to_scenario}"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error copying records: {e}"))
            raise
