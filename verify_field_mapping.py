#!/usr/bin/env python3
"""
Verify the ACTUAL field mapping in populate_inventory_projection_model
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

from website.models import InventoryProjectionModel, AggregatedForecast, CalculatedProductionModel, scenarios
from django.db import connection
from django.db.models import Sum

def verify_field_mapping():
    print("🔍 Verifying Field Mapping in populate_inventory_projection_model")
    print("=" * 70)
    
    version = "Jul 25 SPR"
    scenario = scenarios.objects.get(version=version)
    
    # Check what data is in AggregatedForecast for Crawler Systems July 2025
    print("📊 AggregatedForecast Data (Source for revenue_aud & cogs_aud):")
    print("-" * 70)
    
    af_data = AggregatedForecast.objects.filter(
        version=scenario,
        parent_product_group_description="Crawler Systems",
        period__year=2025,
        period__month=7
    ).aggregate(
        total_revenue=Sum('revenue_aud'),
        total_cogs=Sum('cogs_aud')
    )
    
    print(f"📈 AggregatedForecast July 2025 Crawler Systems:")
    print(f"   • revenue_aud: ${af_data['total_revenue'] or 0:,.2f}")
    print(f"   • cogs_aud:    ${af_data['total_cogs'] or 0:,.2f}")
    
    # Check what data is in CalculatedProductionModel for Crawler Systems July 2025
    print()
    print("🏭 CalculatedProductionModel Data (Source for production_aud):")
    print("-" * 70)
    
    import django.db.models
    cp_data = CalculatedProductionModel.objects.filter(
        version=scenario,
        parent_product_group="Crawler Systems",
        pouring_date__year=2025,
        pouring_date__month=7
    ).aggregate(
        total_cogs_aud=Sum('cogs_aud'),
        total_revenue_aud=Sum('revenue_aud')
    )
    
    print(f"🏭 CalculatedProductionModel July 2025 Crawler Systems:")
    print(f"   • cogs_aud (maps to production_aud):    ${cp_data['total_cogs_aud'] or 0:,.2f}")
    print(f"   • revenue_aud (NOT USED in mapping):    ${cp_data['total_revenue_aud'] or 0:,.2f}")
    
    # Check what's actually in InventoryProjectionModel
    print()
    print("🎯 InventoryProjectionModel Results:")
    print("-" * 70)
    
    ip_data = InventoryProjectionModel.objects.filter(
        version=scenario,
        parent_product_group="Crawler Systems",
        month__year=2025,
        month__month=7
    ).first()
    
    if ip_data:
        print(f"📊 InventoryProjectionModel July 2025 Crawler Systems:")
        print(f"   • production_aud: ${ip_data.production_aud or 0:,.2f}  <- Should match CalculatedProduction.cogs_aud")
        print(f"   • cogs_aud:       ${ip_data.cogs_aud or 0:,.2f}        <- Should match AggregatedForecast.cogs_aud")  
        print(f"   • revenue_aud:    ${ip_data.revenue_aud or 0:,.2f}     <- Should match AggregatedForecast.revenue_aud")
        print(f"   • opening_inv:    ${ip_data.opening_inventory_aud or 0:,.2f}")
        print(f"   • closing_inv:    ${ip_data.closing_inventory_aud or 0:,.2f}")
        
        print()
        print("🔧 Field Mapping Verification:")
        print("-" * 70)
        
        production_match = abs((ip_data.production_aud or 0) - (cp_data['total_cogs_aud'] or 0)) < 0.01
        cogs_match = abs((ip_data.cogs_aud or 0) - (af_data['total_cogs'] or 0)) < 0.01
        revenue_match = abs((ip_data.revenue_aud or 0) - (af_data['total_revenue'] or 0)) < 0.01
        
        print(f"✅ production_aud matches CalculatedProduction.cogs_aud: {production_match}")
        print(f"✅ cogs_aud matches AggregatedForecast.cogs_aud: {cogs_match}")
        print(f"✅ revenue_aud matches AggregatedForecast.revenue_aud: {revenue_match}")
        
        if production_match and cogs_match and revenue_match:
            print()
            print("🎉 SUCCESS: All field mappings are CORRECT as per user specification!")
            print("   • InventoryProjectionModel.production_aud ← CalculatedProductionModel.cogs_aud")
            print("   • InventoryProjectionModel.cogs_aud ← AggregatedForecast.cogs_aud")
            print("   • InventoryProjectionModel.revenue_aud ← AggregatedForecast.revenue_aud")
        else:
            print()
            print("❌ ERROR: Field mappings are NOT correct!")
    else:
        print("❌ No inventory projection data found for Crawler Systems July 2025")
    
    print()
    print("📋 SUMMARY - Current Field Mapping Logic:")
    print("-" * 70)
    print("FROM populate_inventory_projection_model() function:")
    print("  1. revenue_aud & cogs_aud ← AggregatedForecast table")  
    print("  2. production_aud ← CalculatedProductionModel.cogs_aud")
    print("  3. Calculation: closing_inventory = opening + production - cogs")

if __name__ == "__main__":
    verify_field_mapping()
