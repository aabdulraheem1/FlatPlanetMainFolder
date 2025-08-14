from django.core.management.base import BaseCommand
from website.customized_function import get_combined_demand_and_poured_data
from website.models import scenarios

class Command(BaseCommand):
    help = "Test the fixed demand plan calculation with snapshot filtering"

    def handle(self, *args, **kwargs):
        try:
            scenario = scenarios.objects.get(version='AUG 25 SP')
            
            self.stdout.write('🧪 Testing fixed demand plan calculation...')
            
            demand_data, poured_data = get_combined_demand_and_poured_data(scenario)
            
            self.stdout.write('\n=== FIXED DEMAND PLAN RESULTS ===')
            self.stdout.write(f'MTJ1 FY25 Demand Plan: {demand_data["FY25"]["MTJ1"]:,} tonnes')
            self.stdout.write(f'MTJ1 FY25 Poured Data: {poured_data["FY25"]["MTJ1"]:,} tonnes')
            
            # Show other sites for comparison
            self.stdout.write('\n=== ALL SITES FY25 ===')
            for site in ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WUN1', 'WOD1', 'CHI1']:
                demand = demand_data["FY25"].get(site, 0)
                poured = poured_data["FY25"].get(site, 0)
                self.stdout.write(f'{site}: Demand={demand:,}t, Poured={poured:,}t')
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {e}"))
