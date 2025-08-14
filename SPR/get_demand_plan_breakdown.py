import os
import sys
import django

# Django setup
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import scenarios, CalculatedProductionModel
from django.db.models import Sum
from django.db.models.functions import TruncMonth
from datetime import date

# Get the scenario
try:
    scenario = scenarios.objects.get(version='AUG 25 SP')
    print(f'✅ Found scenario: {scenario.version}')
except:
    print('❌ Scenario AUG 25 SP not found')
    sys.exit(1)

# Define FY25 range (Apr 2025 to Mar 2026)
fy25_start = date(2025, 4, 1)
fy25_end = date(2026, 3, 31)

print(f'\n📅 FY25 DEMAND PLAN BREAKDOWN FOR MTJ1 (AUG 25 SP)')
print('=' * 60)

# Get monthly demand data from CalculatedProductionModel (same as build_detailed_monthly_table function)
demand_monthly = (
    CalculatedProductionModel.objects
    .filter(
        version=scenario,
        site__SiteName='MTJ1',
        pouring_date__gte=fy25_start,
        pouring_date__lte=fy25_end
    )
    .annotate(month=TruncMonth('pouring_date'))
    .values('month')
    .annotate(total=Sum('tonnes'))
    .order_by('month')
)

total_demand = 0
for row in demand_monthly:
    month_str = row['month'].strftime('%b %Y')
    tonnes = row['total'] or 0
    total_demand += tonnes
    print(f'{month_str}: {tonnes:,.1f} tonnes')

print(f'\n📊 TOTAL FY25 DEMAND PLAN: {total_demand:,.1f} tonnes')

# Also show the source breakdown (what types of production)
print(f'\n🔍 SOURCE BREAKDOWN BY MONTH:')
print('-' * 40)

current_month = fy25_start
while current_month <= fy25_end:
    month_start = current_month.replace(day=1)
    next_month = month_start.replace(month=month_start.month + 1) if month_start.month < 12 else month_start.replace(year=month_start.year + 1, month=1)
    
    records = CalculatedProductionModel.objects.filter(
        version=scenario,
        site__SiteName='MTJ1',
        pouring_date__gte=month_start,
        pouring_date__lt=next_month
    )
    
    month_total = records.aggregate(total=Sum('tonnes'))['total'] or 0
    record_count = records.count()
    
    if month_total > 0:
        print(f'\n{month_start.strftime("%b %Y")}: {month_total:,.1f} tonnes ({record_count} records)')
        
        # Show production vs revenue breakdown
        production_qty = sum([r.production_quantity or 0 for r in records])
        total_revenue = records.aggregate(total=Sum('revenue_aud'))['total'] or 0
        
        print(f'  - Production Quantity: {production_qty:,.0f} units')
        print(f'  - Revenue AUD: ${total_revenue:,.0f}')
        
        # Sample a few records to see what types
        sample_records = records[:3]
        for i, record in enumerate(sample_records):
            product_name = record.product.Product if record.product else "N/A"
            print(f'  - Record {i+1}: Product={product_name}, Qty={record.production_quantity or 0}, Tonnes={record.tonnes or 0:.1f}, Revenue=${record.revenue_aud or 0:.0f}')
    
    # Move to next month
    current_month = next_month

print(f'\n🔍 DATA SOURCE EXPLANATION:')
print('=' * 40)
print('The demand plan data comes from CalculatedProductionModel which includes:')
print('1. Regular Production Demand (from replenishment calculations)')
print('2. Fixed Plant Forecast (external forecasted demand)')
print('3. Revenue Forecast (demand projected from revenue targets)')
print('4. These are combined and show both actual historical data and future projections')
