from django.core.management.base import BaseCommand
from website.customized_function import build_detailed_monthly_table

class Command(BaseCommand):
    help = 'Test monthly breakdown for specific scenarios'

    def handle(self, *args, **options):
        scenario = "Aug 25 SP"
        
        print("🧪 Testing monthly breakdown for FY25 MTJ1...")
        
        # Test FY25 (should show proper snapshot-based logic)
        table_data = build_detailed_monthly_table("FY25", "MTJ1", scenario)
        
        print(f"\n=== FY25 MTJ1 Monthly Breakdown ===")
        print(f"Total rows: {len(table_data)}")
        
        if isinstance(table_data, list) and len(table_data) > 0:
            print(f"First row type: {type(table_data[0])}")
            if isinstance(table_data[0], dict):
                for i, row in enumerate(table_data[:3]):  # Show first 3 rows
                    print(f"\nRow {i+1}:")
                    for key, value in row.items():
                        print(f"  {key}: {value}")
            else:
                print(f"First few rows: {table_data[:3]}")
        else:
            print(f"Data: {table_data}")
