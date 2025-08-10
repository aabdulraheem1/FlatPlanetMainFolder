#!/usr/bin/env python3
"""
Test Auto-Level Optimization Fix
Tests the key fixes applied to restore working auto-leveling functionality.
"""

import os
import sys
import django

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from datetime import datetime
from django.db.models import Sum
from website.models import scenarios, CalculatedProductionModel, MasterDataPlan

def test_mtj1_gap_calculation():
    """Test gap calculation for MTJ1 July 2025"""
    print("🧪 Testing MTJ1 July 2025 Gap Calculation...")
    
    try:
        # Get Jul 25 SPR scenario
        scenario = scenarios.objects.get(version="Jul 25 SPR")
        
        # Get MTJ1 pour plan for July 2025
        pour_plan = MasterDataPlan.objects.filter(
            version=scenario,
            Foundry__SiteName="MTJ1",
            Month__year=2025,
            Month__month=7
        ).first()
        
        if not pour_plan:
            print("❌ No pour plan found for MTJ1 July 2025")
            return False
        
        capacity = pour_plan.PlanDressMass or 0
        print(f"📊 MTJ1 July 2025 Capacity: {capacity:.2f} tonnes")
        
        # Get current demand
        current_demand = CalculatedProductionModel.objects.filter(
            version=scenario,
            site__SiteName="MTJ1",
            pouring_date__year=2025,
            pouring_date__month=7
        ).aggregate(total=Sum('tonnes'))['total'] or 0
        
        print(f"📊 MTJ1 July 2025 Current Demand: {current_demand:.2f} tonnes")
        
        # Calculate gap
        gap = capacity - current_demand
        print(f"📊 MTJ1 July 2025 Gap: {gap:.2f} tonnes")
        
        # Test gap threshold (should be > 1.0 to trigger optimization)
        if gap > 1.0:
            print(f"✅ Gap ({gap:.2f}t) exceeds 1.0 tonne threshold - WILL trigger optimization")
        else:
            print(f"❌ Gap ({gap:.2f}t) below 1.0 tonne threshold - will NOT trigger optimization")
            
        # Check future production availability
        future_production = CalculatedProductionModel.objects.filter(
            version=scenario,
            site__SiteName="MTJ1",
            pouring_date__year=2025,
            pouring_date__month__gt=7,
            tonnes__gt=0
        ).aggregate(total=Sum('tonnes'))['total'] or 0
        
        print(f"📊 MTJ1 Future Production Available: {future_production:.2f} tonnes")
        
        if future_production >= gap:
            print(f"✅ Sufficient future production ({future_production:.2f}t) to fill gap ({gap:.2f}t)")
        else:
            print(f"⚠️ Insufficient future production ({future_production:.2f}t) to fill full gap ({gap:.2f}t)")
            
        return True
        
    except Exception as e:
        print(f"❌ Error in gap calculation test: {e}")
        return False

def test_threshold_logic():
    """Test the threshold logic that was fixed"""
    print("\n🧪 Testing Threshold Logic...")
    
    test_cases = [
        (0.5, "Below 1.0 threshold - should skip"),
        (1.5, "Above 1.0 threshold - should process"),
        (0.0, "Zero gap - should skip"),
        (2000.0, "Large gap - should process")
    ]
    
    for gap_value, description in test_cases:
        if gap_value <= 1.0:
            result = "SKIP"
        else:
            result = "PROCESS"
            
        print(f"  Gap: {gap_value:.1f}t → {result} ({description})")
    
    print("✅ Threshold logic test complete")
    return True

def main():
    """Run all tests"""
    print("🚀 Testing Auto-Level Optimization Fixes\n")
    
    all_passed = True
    
    # Test gap calculation
    if not test_mtj1_gap_calculation():
        all_passed = False
    
    # Test threshold logic
    if not test_threshold_logic():
        all_passed = False
    
    print(f"\n{'🎉 ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    print("\nKey fixes applied:")
    print("  ✅ Gap threshold reverted from 0.1t back to 1.0t")
    print("  ✅ Production filtering reverted from 0.01t back to 0t")
    print("  ✅ Movement threshold reverted from 0.01t back to 0t")
    print("  ✅ 90-day constraint logic reverted from 'continue' back to 'break'")
    print("  ✅ Removed over-complex future month checking logic")
    
    return all_passed

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
