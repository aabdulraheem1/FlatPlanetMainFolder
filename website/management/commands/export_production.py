from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.db.models.functions import TruncMonth
from website.models import scenarios, CalculatedProductionModel
import csv
import os
from datetime import datetime


class Command(BaseCommand):
    help = 'Export production data by product to CSV file'

    def add_arguments(self, parser):
        parser.add_argument('version', type=str, help='Scenario version (e.g., Jul_25_SPR_Inv)')
        parser.add_argument(
            '--output', 
            type=str, 
            help='Output file path (optional, defaults to current directory)',
            default=None
        )
        parser.add_argument(
            '--group', 
            type=str, 
            help='Filter by parent product group (optional)',
            default=None
        )

    def handle(self, *args, **options):
        version = options['version']
        output_path = options['output']
        parent_group = options['group']
        
        try:
            # Get the scenario
            scenario = scenarios.objects.get(version=version)
            self.stdout.write(f"Found scenario: {version}")
            
            # Query production data
            queryset = CalculatedProductionModel.objects.filter(version=scenario)
            
            # Filter by parent group if specified
            if parent_group:
                queryset = queryset.filter(parent_product_group=parent_group)
                self.stdout.write(f"Filtering by parent group: {parent_group}")
            
            # Group by parent product group, product, and month, then sum production costs
            production_data = (
                queryset
                .annotate(month=TruncMonth('pouring_date'))
                .values('parent_product_group', 'product', 'month')
                .annotate(total_production_aud=Sum('cogs_aud'))
                .order_by('parent_product_group', 'product', 'month')
            )
            
            # Convert to list for CSV export
            export_data = []
            for item in production_data:
                export_data.append({
                    'ParentProductGroup': item['parent_product_group'],
                    'Product': item['product'],
                    'Date': item['month'].strftime('%Y-%m-%d'),
                    'ProductionAUD': round(float(item['total_production_aud'] or 0), 2)
                })
            
            if not export_data:
                self.stdout.write(
                    self.style.WARNING('No production data found for the specified criteria')
                )
                return
            
            # Generate filename if not provided
            if not output_path:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                group_suffix = f'_{parent_group.replace(" ", "_")}' if parent_group else '_All_Groups'
                filename = f'Production_by_Product_{version}{group_suffix}_{timestamp}.csv'
                output_path = os.path.join(os.getcwd(), filename)
            
            # Write CSV file
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['ParentProductGroup', 'Product', 'Date', 'ProductionAUD']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(export_data)
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully exported {len(export_data)} records to: {output_path}')
            )
            
        except scenarios.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f'Scenario not found: {version}')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Export failed: {str(e)}')
            )
