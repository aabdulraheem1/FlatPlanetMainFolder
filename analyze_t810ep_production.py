#!/usr/bin/env python3
"""
Quick script to analyze T810EP production allocation results
"""

import os
import sys
import django

# Add the project directory to Python path
sys.path.append('c:\\Users\\aali\\OneDrive - bradken.com\\Data\\Training\\SPR')

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from SPR.website.models import CalculatedProductionModel
from collections import defaultdict

def analyze_t810ep_production():
    """Analyze T810EP production allocation results"""
    
    print("🏭 T810EP PRODUCTION FLOW ANALYSIS")
    print("=" * 50)
    
    # Get T810EP production records  
    production_records = CalculatedProductionModel.objects.filter(
        version__version='Aug 25 SPR',
        product__Product='T810EP'
    )
    
    if not production_records.exists():
        print("❌ No production records found for T810EP")
        return
    
    print(f"📈 TOTAL PRODUCTION RECORDS: {production_records.count()}")
    
    # Group by production site
    site_totals = defaultdict(float)
    total_production = 0
    
    for record in production_records:
        site_name = record.site.SiteName if record.site else "Unknown"
        site_totals[site_name] += record.production_quantity
        total_production += record.production_quantity
    
    print(f"📊 TOTAL PRODUCTION QUANTITY: {total_production} units")
    print(f"🏭 PRODUCTION BY SITE:")
    
    # Sort by quantity descending
    for site, quantity in sorted(site_totals.items(), key=lambda x: x[1], reverse=True):
        percentage = (quantity / total_production * 100) if total_production > 0 else 0
        print(f"   {site}: {quantity:,.0f} units ({percentage:.1f}%)")
    
    # Monthly breakdown
    from datetime import datetime
    monthly_totals = defaultdict(float)
    
    for record in production_records:
        month_key = record.pouring_date.strftime('%Y-%m')
        monthly_totals[month_key] += record.production_quantity
    
    print(f"\n📅 PRODUCTION BY MONTH:")
    for month, quantity in sorted(monthly_totals.items()):
        percentage = (quantity / total_production * 100) if total_production > 0 else 0
        print(f"   {month}: {quantity:,.0f} units ({percentage:.1f}%)")

if __name__ == "__main__":
    analyze_t810ep_production()
