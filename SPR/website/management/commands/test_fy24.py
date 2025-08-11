from django.core.management.base import BaseCommand
from website.customized_function import build_detailed_monthly_table
from website.models import scenarios

class Command(BaseCommand):
    help = 'Test FY24 modal functionality'

    def handle(self, *args, **options):
        try:
            # Quick test of FY24 modal
            scenario = scenarios.objects.get(version='Dec 24 SPR to Test Inventory Model')
            
            # Test the fy_ranges dictionary access
            fy_ranges = {
                "FY24": ("2024-04-01", "2025-03-31"),
                "FY25": ("2025-04-01", "2026-03-31"),
                "FY26": ("2026-04-01", "2027-03-31"),
                "FY27": ("2027-04-01", "2028-03-31"),
            }
            
            if 'FY24' in fy_ranges:
                self.stdout.write("✅ FY24 found in test fy_ranges")
            else:
                self.stdout.write("❌ FY24 missing from test fy_ranges")
            
            # Test the actual function call
            self.stdout.write("🔍 Testing build_detailed_monthly_table for FY24...")
            result = build_detailed_monthly_table('FY24', 'MTJ1', scenario, 'pour')
            
            if result and len(str(result)) > 100:
                self.stdout.write("✅ FY24 modal test successful!")
                self.stdout.write(f"   Result type: {type(result)}")
                self.stdout.write(f"   Result length: {len(str(result))} characters")
                
                if 'table' in str(result).lower():
                    self.stdout.write("✅ Result contains HTML table")
                else:
                    self.stdout.write("⚠️  Result might not be HTML table")
            else:
                self.stdout.write("❌ FY24 modal returned empty or invalid result")
                self.stdout.write(f"   Result: {result}")
                
        except KeyError as e:
            self.stdout.write(f"❌ KeyError (FY24 missing from fy_ranges): {e}")
        except Exception as e:
            self.stdout.write(f"❌ Error: {e}")
            import traceback
            self.stdout.write(traceback.format_exc())
