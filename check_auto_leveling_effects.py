#!/usr/bin/env python3
"""
Verify if auto-leveling moved production TO July 2025 for Crawler Systems
"""
import os
import sys
import django
from datetime import datetime

# Setup Django environment
if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_dir)
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
    django.setup()

from website.models import CalculatedProductionModel, scenarios
from django.db.models import Sum, Count

def check_auto_leveling_effects():
    print("🔍 Checking Auto-Leveling Effects on Crawler Systems Production")
    print("=" * 65)
    
    version = "Jul 25 SPR"
    scenario = scenarios.objects.get(version=version)
    
    # Check production distribution across months for Crawler Systems
    monthly_production = CalculatedProductionModel.objects.filter(
        version=scenario,
        parent_product_group="Crawler Systems",
        pouring_date__year__gte=2025
    ).extra(
        select={'month': "FORMAT(pouring_date, 'yyyy-MM')"}
    ).values('month').annotate(
        total_cogs=Sum('cogs_aud'),
        total_tonnes=Sum('tonnes'),
        record_count=Count('id')
    ).order_by('month')
    
    print("📊 Crawler Systems Production by Month (Jul 25 SPR):")
    print("-" * 65)
    
    total_cogs = 0
    total_tonnes = 0
    total_records = 0
    
    for month_data in monthly_production:
        month = month_data['month']
        cogs = month_data['total_cogs'] or 0
        tonnes = month_data['total_tonnes'] or 0
        records = month_data['record_count']
        
        total_cogs += cogs
        total_tonnes += tonnes
        total_records += records
        
        print(f"📅 {month}: ${cogs:>12,.2f} COGS | {tonnes:>8.2f}t | {records:>3} records")
    
    print("-" * 65)
    print(f"📊 TOTAL:    ${total_cogs:>12,.2f} COGS | {total_tonnes:>8.2f}t | {total_records:>3} records")
    
    print()
    print("🧪 Analysis:")
    
    # Check if July has unusually high production (sign of auto-leveling)
    july_data = next((m for m in monthly_production if m['month'] == '2025-07'), None)
    if july_data:
        july_cogs = july_data['total_cogs'] or 0
        july_percentage = (july_cogs / total_cogs * 100) if total_cogs > 0 else 0
        print(f"📈 July 2025 represents {july_percentage:.1f}% of total Crawler Systems COGS")
        
        if july_percentage > 25:  # If July has more than 25% of production
            print("✅ HIGH July production suggests auto-leveling moved work TO July")
        elif july_percentage < 15:
            print("⚠️  LOW July production suggests auto-leveling may not have filled capacity")
        else:
            print("📊 MODERATE July production - normal distribution")
    else:
        print("❌ No July 2025 production found for Crawler Systems")
    
    print()
    print("🎯 Expected Auto-Leveling Impact:")
    print("  • MTJ1 July capacity: ~1912 tonnes")  
    print("  • If auto-leveling worked, July should have increased production")
    print("  • Production_aud in inventory projections should reflect this increase")
    print("  • Closing inventory should be affected by the production timing change")

if __name__ == "__main__":
    check_auto_leveling_effects()
