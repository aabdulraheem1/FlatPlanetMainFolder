#!/usr/bin/env python3
"""
Test script to verify inventory projection data flow after auto-leveling
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

from website.models import (
    InventoryProjectionModel, 
    CalculatedProductionModel, 
    scenarios,
    ScenarioOptimizationState
)
from django.db.models import Sum

def test_inventory_projection_flow():
    print("🔍 Testing Inventory Projection Data Flow After Auto-Leveling")
    print("=" * 70)
    
    version = "Jul 25 SPR"
    
    try:
        scenario = scenarios.objects.get(version=version)
        print(f"✅ Found scenario: {version}")
    except scenarios.DoesNotExist:
        print(f"❌ Scenario '{version}' not found")
        return False
    
    # Check optimization state
    try:
        opt_state = ScenarioOptimizationState.objects.get(version=scenario)
        print(f"📊 Auto-optimization applied: {opt_state.auto_optimization_applied}")
        if opt_state.last_optimization_date:
            print(f"📅 Last optimization: {opt_state.last_optimization_date}")
    except ScenarioOptimizationState.DoesNotExist:
        print("⚠️  No optimization state found")
    
    print()
    
    # 1. Check CalculatedProductionModel for Crawler Systems in July 2025
    print("🧪 STEP 1: Check CalculatedProductionModel for Crawler Systems July 2025")
    production_records = CalculatedProductionModel.objects.filter(
        version=scenario,
        parent_product_group="Crawler Systems",
        pouring_date__year=2025,
        pouring_date__month=7
    )
    
    total_production_aud = production_records.aggregate(
        total_revenue=Sum('revenue_aud'),
        total_cogs=Sum('cogs_aud'),
        total_tonnes=Sum('tonnes')
    )
    
    print(f"📊 Crawler Systems July 2025 Production Records: {production_records.count()}")
    print(f"📊 Total Revenue AUD: ${total_production_aud['total_revenue']:,.2f}" if total_production_aud['total_revenue'] else "📊 Total Revenue AUD: $0.00")
    print(f"📊 Total COGS AUD: ${total_production_aud['total_cogs']:,.2f}" if total_production_aud['total_cogs'] else "📊 Total COGS AUD: $0.00")
    print(f"📊 Total Tonnes: {total_production_aud['total_tonnes']:,.2f}" if total_production_aud['total_tonnes'] else "📊 Total Tonnes: 0.00")
    
    # Show sample records
    sample_records = production_records[:3]
    for record in sample_records:
        print(f"  📄 Record {record.id}: {record.pouring_date} - Rev: ${record.revenue_aud:,.2f} - COGS: ${record.cogs_aud:,.2f} - {record.tonnes:,.2f}t")
    
    print()
    
    # 2. Check InventoryProjectionModel for Crawler Systems in July 2025
    print("🧪 STEP 2: Check InventoryProjectionModel for Crawler Systems July 2025")
    inventory_records = InventoryProjectionModel.objects.filter(
        version=scenario,
        parent_product_group="Crawler Systems",
        month__year=2025,
        month__month=7
    )
    
    print(f"📊 Crawler Systems July 2025 Inventory Records: {inventory_records.count()}")
    
    for record in inventory_records:
        print(f"  📊 Month: {record.month}")
        print(f"  💰 Production AUD: ${record.production_aud:,.2f}")
        print(f"  📦 Opening Inventory AUD: ${record.opening_inventory_aud:,.2f}")
        print(f"  📦 Closing Inventory AUD: ${record.closing_inventory_aud:,.2f}")
        print(f"  🏭 COGS AUD: ${record.cogs_aud:,.2f}")
        print(f"  💵 Revenue AUD: ${record.revenue_aud:,.2f}")
        print(f"  🕐 Created: {record.created_at}")
        print(f"  🕒 Updated: {record.updated_at}")
        print()
    
    # 3. Cross-verification: Do the numbers match?
    print("🧪 STEP 3: Cross-Verification")
    if inventory_records.exists() and total_production_aud['total_cogs']:
        inventory_production_aud = inventory_records.first().production_aud
        calculated_cogs_aud = total_production_aud['total_cogs']
        
        print(f"📊 InventoryProjectionModel Production AUD: ${inventory_production_aud:,.2f}")
        print(f"📊 CalculatedProductionModel COGS AUD: ${calculated_cogs_aud:,.2f}")
        
        if abs(inventory_production_aud - calculated_cogs_aud) < 0.01:
            print("✅ Production AUD = COGS AUD values MATCH - Data flow is correct")
        else:
            print("❌ Production AUD ≠ COGS AUD values DO NOT MATCH - Data flow may be broken")
            difference = inventory_production_aud - calculated_cogs_aud
            print(f"📊 Difference: ${difference:,.2f}")
            
        # Also show revenue for comparison
        calculated_revenue_aud = total_production_aud['total_revenue']
        print(f"📊 CalculatedProductionModel Revenue AUD: ${calculated_revenue_aud:,.2f}")
    else:
        print("⚠️  Cannot verify - missing data in one of the models")
    
    print()
    
    # 4. Check when inventory projections were last updated
    print("🧪 STEP 4: Check Inventory Projection Update Timestamps")
    latest_inventory = InventoryProjectionModel.objects.filter(
        version=scenario
    ).order_by('-updated_at').first()
    
    if latest_inventory:
        print(f"📅 Latest inventory projection update: {latest_inventory.updated_at}")
        
        # Compare with optimization timestamp
        if opt_state and opt_state.last_optimization_date:
            time_diff = latest_inventory.updated_at - opt_state.last_optimization_date
            if time_diff.total_seconds() > 0:
                print("✅ Inventory projections updated AFTER auto-leveling")
            else:
                print("❌ Inventory projections updated BEFORE auto-leveling - may be stale")
        else:
            print("⚠️  Cannot compare - no optimization timestamp available")
    else:
        print("❌ No inventory projection records found")
    
    print()
    print("🎯 SUMMARY:")
    print("=" * 70)
    
    # Check all three functions that should regenerate inventory projections
    functions_that_regenerate = [
        "calculate_model()",
        "auto_level_optimization()", 
        "reset function"
    ]
    
    print("Functions that should regenerate inventory projections:")
    for func in functions_that_regenerate:
        print(f"  📝 {func}")
    
    print()
    print("Expected data flow:")
    print("  1. Auto-leveling moves production records in CalculatedProductionModel")
    print("  2. Auto-leveling calls populate_inventory_projection_model()")  
    print("  3. populate_inventory_projection_model() reads updated CalculatedProductionModel.cogs_aud")
    print("  4. New InventoryProjectionModel records created with production_aud = sum(cogs_aud)")
    print("  5. Inventory chart shows updated production (COGS) and closing inventory values")
    print()
    print("KEY FIELD MAPPING:")
    print("  CalculatedProductionModel.cogs_aud → InventoryProjectionModel.production_aud")
    print("  CalculatedProductionModel.revenue_aud → InventoryProjectionModel.revenue_aud")
    
    return True

if __name__ == "__main__":
    test_inventory_projection_flow()
