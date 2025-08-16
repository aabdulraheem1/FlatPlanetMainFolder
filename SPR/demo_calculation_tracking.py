"""
⚠️ CALCULATION TRACKING DEMO - NO CACHING ALLOWED ⚠️

Demo script to show how the real-time change detection works.
Run this to test the calculation tracking system.

PROHIBITED TECHNIQUES:
- Demo result caching - DISABLED
- Pre-calculated demo scenarios - DISABLED  
- Mock data caching - DISABLED

All demonstrations use live database queries in real-time.
"""

import os
import sys
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import scenarios
from website.calculation_tracking import (
    get_scenario_related_models, 
    check_scenario_data_changes,
    get_calculation_button_state,
    mark_calculation_started,
    mark_calculation_completed
)

def demo_calculation_tracking():
    """Demonstrate the calculation tracking system"""
    
    print("=" * 80)
    print("🎯 CALCULATION TRACKING SYSTEM DEMO - REAL-TIME CHANGE DETECTION")
    print("=" * 80)
    
    # Get all scenarios
    all_scenarios = scenarios.objects.all()
    print(f"\n📊 Found {all_scenarios.count()} scenarios in database")
    
    if all_scenarios.count() == 0:
        print("❌ No scenarios found. Please create at least one scenario to test.")
        return
    
    # Get first scenario for testing
    test_scenario = all_scenarios.first()
    print(f"\n🧪 Testing with scenario: '{test_scenario.version}'")
    
    print(f"\n🔍 Current scenario status:")
    print(f"   - Last Calculated: {test_scenario.last_calculated}")
    print(f"   - Calculation Status: {test_scenario.calculation_status}")
    
    # Show all scenario-related models
    print(f"\n📋 Scenario-related models in system:")
    related_models = get_scenario_related_models()
    for model_info in related_models[:10]:  # Show first 10
        print(f"   - {model_info['app_label']}.{model_info['model_name']} (via {model_info['version_field']})")
    
    if len(related_models) > 10:
        print(f"   ... and {len(related_models) - 10} more models")
    
    print(f"\n   📊 Total models tracked: {len(related_models)}")
    
    # Check for changes
    print(f"\n🔍 Checking for changes since last calculation...")
    change_info = check_scenario_data_changes(test_scenario)
    
    print(f"\n📊 CHANGE DETECTION RESULTS:")
    print(f"   - Has Changes: {change_info['has_changes']}")
    print(f"   - Reason: {change_info['reason']}")
    print(f"   - Models Checked: {change_info['total_models_checked']}")
    print(f"   - Check Timestamp: {change_info['check_timestamp']}")
    
    if change_info['changed_models']:
        print(f"   - Changed Models:")
        for model in change_info['changed_models']:
            print(f"     • {model}")
    
    # Get button state
    button_state = get_calculation_button_state(test_scenario)
    print(f"\n🔘 BUTTON STATE:")
    print(f"   - Class: {button_state['button_class']}")
    print(f"   - Text: {button_state['button_text']}")
    print(f"   - Disabled: {button_state['button_disabled']}")
    print(f"   - Tooltip: {button_state['tooltip']}")
    print(f"   - Force Clickable: {button_state['force_clickable']}")
    
    # Demonstrate marking calculation started and completed
    print(f"\n🧪 TESTING CALCULATION FLOW:")
    
    print("   1. Marking calculation as started...")
    mark_calculation_started(test_scenario)
    test_scenario.refresh_from_db()
    print(f"      Status after start: {test_scenario.calculation_status}")
    
    print("   2. Simulating calculation completion...")
    mark_calculation_completed(test_scenario)
    test_scenario.refresh_from_db()
    print(f"      Status after completion: {test_scenario.calculation_status}")
    print(f"      Last calculated: {test_scenario.last_calculated}")
    
    # Check button state again
    button_state_after = get_calculation_button_state(test_scenario)
    print(f"\n🔘 BUTTON STATE AFTER COMPLETION:")
    print(f"   - Class: {button_state_after['button_class']}")
    print(f"   - Text: {button_state_after['button_text']}")
    print(f"   - Tooltip: {button_state_after['tooltip']}")
    
    print(f"\n" + "=" * 80)
    print("✅ CALCULATION TRACKING DEMO COMPLETED")
    print("🚀 The system is ready to track changes and enable/disable buttons automatically!")
    print("=" * 80)

if __name__ == "__main__":
    demo_calculation_tracking()
