#!/usr/bin/env python3
"""
Test Crawler Systems COGS AUD comparison between CalculatedProductionModel and InventoryProjectionModel
for Jul 25 SPR scenario, July 2025 period
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

from website.models import CalculatedProductionModel, InventoryProjectionModel, scenarios
from django.db.models import Sum

def test_crawler_systems_cogs_comparison():
    print("🔍 Testing Crawler Systems COGS AUD Comparison")
    print("Scenario: Jul 25 SPR | Period: July 2025")
    print("=" * 70)
    
    version = "Jul 25 SPR"
    try:
        scenario = scenarios.objects.get(version=version)
        print(f"✅ Found scenario: {scenario.version}")
    except scenarios.DoesNotExist:
        print(f"❌ ERROR: Scenario '{version}' not found")
        return
    
    print()
    print("📊 PART 1: CalculatedProductionModel Analysis")
    print("-" * 50)
    
    # Get Crawler Systems production data for July 2025
    cp_records = CalculatedProductionModel.objects.filter(
        version=scenario,
        parent_product_group="Crawler Systems",
        pouring_date__year=2025,
        pouring_date__month=7
    )
    
    cp_count = cp_records.count()
    cp_aggregates = cp_records.aggregate(
        total_cogs_aud=Sum('cogs_aud'),
        total_revenue_aud=Sum('revenue_aud'),
        total_tonnes=Sum('tonnes')
    )
    
    print(f"🏭 CalculatedProductionModel - Crawler Systems July 2025:")
    print(f"   • Record Count:  {cp_count}")
    print(f"   • Total COGS AUD:    ${cp_aggregates['total_cogs_aud'] or 0:,.2f}")
    print(f"   • Total Revenue AUD: ${cp_aggregates['total_revenue_aud'] or 0:,.2f}")
    print(f"   • Total Tonnes:      {cp_aggregates['total_tonnes'] or 0:,.2f}t")
    
    # Show a few sample records
    print(f"   • Sample Records:")
    sample_records = cp_records[:5]
    for record in sample_records:
        print(f"     - ID {record.id}: {record.pouring_date} | {record.tonnes or 0:,.2f}t | ${record.cogs_aud or 0:,.2f} COGS")
    
    print()
    print("📈 PART 2: InventoryProjectionModel Analysis")
    print("-" * 50)
    
    # Get inventory projection data for Crawler Systems July 2025
    ip_record = InventoryProjectionModel.objects.filter(
        version=scenario,
        parent_product_group="Crawler Systems",
        month__year=2025,
        month__month=7
    ).first()
    
    if ip_record:
        print(f"📊 InventoryProjectionModel - Crawler Systems July 2025:")
        print(f"   • Month:            {ip_record.month}")
        print(f"   • Production AUD:   ${ip_record.production_aud or 0:,.2f}")
        print(f"   • COGS AUD:         ${ip_record.cogs_aud or 0:,.2f}")
        print(f"   • Revenue AUD:      ${ip_record.revenue_aud or 0:,.2f}")
        print(f"   • Opening Inv:      ${ip_record.opening_inventory_aud or 0:,.2f}")
        print(f"   • Closing Inv:      ${ip_record.closing_inventory_aud or 0:,.2f}")
        print(f"   • Created:          {ip_record.created_at}")
        print(f"   • Updated:          {ip_record.updated_at}")
        
        print()
        print("🔧 PART 3: Field Mapping Verification")
        print("-" * 50)
        
        cp_cogs = cp_aggregates['total_cogs_aud'] or 0
        ip_production = ip_record.production_aud or 0
        
        print(f"📍 Key Comparison:")
        print(f"   • CalculatedProductionModel.cogs_aud:     ${cp_cogs:,.2f}")
        print(f"   • InventoryProjectionModel.production_aud: ${ip_production:,.2f}")
        
        # Calculate difference
        difference = abs(cp_cogs - ip_production)
        percentage_diff = (difference / cp_cogs * 100) if cp_cogs > 0 else 0
        
        print(f"   • Absolute Difference: ${difference:,.2f}")
        print(f"   • Percentage Diff:     {percentage_diff:.4f}%")
        
        # Determine match status
        if difference < 0.01:  # Within 1 cent
            print(f"   • Status: ✅ PERFECT MATCH")
            print(f"   • Field mapping is working correctly!")
        elif percentage_diff < 0.1:  # Within 0.1%
            print(f"   • Status: ✅ VERY CLOSE MATCH (within 0.1%)")
            print(f"   • Field mapping appears to be working correctly")
        else:
            print(f"   • Status: ❌ MISMATCH DETECTED")
            print(f"   • There may be an issue with field mapping or data sync")
        
        print()
        print("📋 PART 4: Expected vs Actual Mapping")
        print("-" * 50)
        print("🎯 EXPECTED FIELD MAPPING:")
        print("   • CalculatedProductionModel.cogs_aud → InventoryProjectionModel.production_aud")
        print("   • This is what the populate_inventory_projection_model() function should do")
        print()
        print("🔍 ACTUAL RESULTS:")
        print(f"   • Source (CP.cogs_aud):        ${cp_cogs:,.2f}")
        print(f"   • Destination (IP.production): ${ip_production:,.2f}")
        
        if difference < 0.01:
            print("   • ✅ Mapping is working as expected!")
        else:
            print("   • ❌ Mapping may have issues - values don't match")
            
        print()
        print("💡 ADDITIONAL INSIGHTS:")
        print(f"   • Total CP records for Crawler Systems July 2025: {cp_count}")
        print(f"   • Average COGS per record: ${(cp_cogs / cp_count) if cp_count > 0 else 0:,.2f}")
        print(f"   • Total tonnes in CP: {cp_aggregates['total_tonnes'] or 0:,.2f}t")
        
    else:
        print("❌ ERROR: No inventory projection record found for Crawler Systems July 2025")
        print("   • This suggests inventory projections may not be populated")
        print("   • Or the populate_inventory_projection_model() function hasn't run")
        
        # Show what CP data exists
        print()
        print("📊 Available CalculatedProductionModel data:")
        print(f"   • COGS AUD: ${cp_aggregates['total_cogs_aud'] or 0:,.2f}")
        print(f"   • Records:  {cp_count}")

if __name__ == "__main__":
    test_crawler_systems_cogs_comparison()
