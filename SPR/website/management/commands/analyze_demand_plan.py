from django.core.management.base import BaseCommand
from django.db.models import Sum
from datetime import date
from website.models import scenarios, CalculatedProductionModel

class Command(BaseCommand):
    help = "Analyze demand plan breakdown for MTJ1 FY25 in AUG 25 SP scenario"

    def handle(self, *args, **kwargs):
        try:
            scenario = scenarios.objects.get(version='AUG 25 SP')
            self.stdout.write(f'✅ Found scenario: {scenario.version}')
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR("❌ Scenario 'AUG 25 SP' not found"))
            return

        # Define FY25 range
        fy25_start = date(2025, 4, 1)
        fy25_end = date(2026, 3, 31)

        self.stdout.write('\n📅 FY25 DEMAND PLAN BREAKDOWN FOR MTJ1 (AUG 25 SP)')
        self.stdout.write('=' * 70)

        # Get all records for the year
        all_records = CalculatedProductionModel.objects.filter(
            version=scenario,
            site__SiteName='MTJ1',
            pouring_date__gte=fy25_start,
            pouring_date__lte=fy25_end
        )

        total_tonnes = all_records.aggregate(total=Sum('tonnes'))['total'] or 0
        total_count = all_records.count()

        self.stdout.write(f'\n🎯 OVERALL SUMMARY:')
        self.stdout.write(f'Total FY25 Demand: {total_tonnes:,.1f} tonnes ({total_count:,} records)')

        # Breakdown by production vs revenue
        production_records = all_records.filter(production_quantity__gt=0)
        revenue_records = all_records.filter(production_quantity=0, revenue_aud__gt=0)

        prod_tonnes = production_records.aggregate(total=Sum('tonnes'))['total'] or 0
        rev_tonnes = revenue_records.aggregate(total=Sum('tonnes'))['total'] or 0

        self.stdout.write(f'\n📊 SOURCE BREAKDOWN:')
        self.stdout.write(f'├─ Production Demand: {prod_tonnes:,.1f} tonnes ({production_records.count():,} records)')
        self.stdout.write(f'└─ Revenue/Fixed Plant: {rev_tonnes:,.1f} tonnes ({revenue_records.count():,} records)')

        # Monthly breakdown
        self.stdout.write(f'\n📅 MONTHLY BREAKDOWN:')
        self.stdout.write('-' * 50)

        months = [
            ('Jul 2025', date(2025, 7, 1), date(2025, 8, 1)),
            ('Aug 2025', date(2025, 8, 1), date(2025, 9, 1)),
            ('Sep 2025', date(2025, 9, 1), date(2025, 10, 1)),
            ('Oct 2025', date(2025, 10, 1), date(2025, 11, 1)),
            ('Nov 2025', date(2025, 11, 1), date(2025, 12, 1)),
            ('Dec 2025', date(2025, 12, 1), date(2026, 1, 1)),
            ('Jan 2026', date(2026, 1, 1), date(2026, 2, 1)),
            ('Feb 2026', date(2026, 2, 1), date(2026, 3, 1)),
            ('Mar 2026', date(2026, 3, 1), date(2026, 4, 1)),
        ]

        for month_name, start, end in months:
            month_records = all_records.filter(pouring_date__gte=start, pouring_date__lt=end)
            month_total = month_records.aggregate(total=Sum('tonnes'))['total'] or 0
            
            if month_total > 0:
                month_prod = month_records.filter(production_quantity__gt=0).aggregate(total=Sum('tonnes'))['total'] or 0
                month_rev = month_records.filter(production_quantity=0, revenue_aud__gt=0).aggregate(total=Sum('tonnes'))['total'] or 0
                
                self.stdout.write(f'{month_name}: {month_total:,.1f} tonnes')
                self.stdout.write(f'  ├─ Production: {month_prod:,.1f} tonnes')
                self.stdout.write(f'  └─ Revenue/Fixed: {month_rev:,.1f} tonnes')

        # Focus on July 2025 (largest month)
        self.stdout.write(f'\n🔍 JULY 2025 DETAILED ANALYSIS:')
        self.stdout.write('-' * 40)
        
        july_records = all_records.filter(
            pouring_date__year=2025,
            pouring_date__month=7
        )
        
        july_total = july_records.aggregate(total=Sum('tonnes'))['total'] or 0
        self.stdout.write(f'July 2025 Total: {july_total:,.1f} tonnes ({july_records.count():,} records)')

        # Top products in July
        top_july_products = july_records.values('product__Product').annotate(
            tonnes=Sum('tonnes')
        ).order_by('-tonnes')[:10]
        
        if top_july_products:
            self.stdout.write(f'\nTop 10 products in July 2025:')
            for i, prod in enumerate(top_july_products, 1):
                prod_name = prod['product__Product'] or 'Unknown'
                self.stdout.write(f'{i:2d}. {prod_name}: {prod["tonnes"]:.1f} tonnes')

        # Explain the data composition
        self.stdout.write(f'\n💡 WHAT THIS DATA REPRESENTS:')
        self.stdout.write('=' * 50)
        self.stdout.write('The demand_plan.FY25.MTJ1 value in the Control Tower combines:')
        self.stdout.write('')
        self.stdout.write('1. 📦 PRODUCTION DEMAND (production_quantity > 0):')
        self.stdout.write('   • Calculated from replenishment requirements')
        self.stdout.write('   • Based on inventory shortfalls and forecasted shipments')
        self.stdout.write('   • Represents actual casting/pouring demand')
        self.stdout.write('')
        self.stdout.write('2. 💰 REVENUE/FIXED PLANT FORECAST (production_quantity = 0):')
        self.stdout.write('   • External demand forecasts from Fixed Plant data')
        self.stdout.write('   • Revenue-based projections converted to tonnes')
        self.stdout.write('   • Strategic capacity allocations')
        self.stdout.write('')
        self.stdout.write('3. ⏰ TIME-MIXED DATA:')
        self.stdout.write('   • Historical months: May show actual performance')
        self.stdout.write('   • Future months: Show projected/planned demand')
        self.stdout.write('   • Snapshot date (July 31, 2025) determines split')
        self.stdout.write('')
        self.stdout.write('This gives you both actual demand and future projections in one view!')
