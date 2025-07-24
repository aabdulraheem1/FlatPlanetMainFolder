#!/usr/bin/env python
"""
Simple test script to verify inventory data structure from the review_scenario function
"""
import os
import sys
import django

# Setup Django environment
sys.path.append(r'c:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import scenarios
from website.customized_function import get_stored_inventory_data
import json

def test_inventory_data():
    """Test the inventory data structure that should reach the template"""
    print("🔥 TESTING INVENTORY DATA STRUCTURE")
    print("=" * 50)
    
    # Get a test scenario
    try:
        scenario = scenarios.objects.get(version="Jul 25 SPR Inv")
        print(f"✅ Found scenario: {scenario.version}")
    except scenarios.DoesNotExist:
        print("❌ Scenario not found")
        return
    
    # Test the stored inventory data function
    try:
        stored_inventory_data = get_stored_inventory_data(scenario)
        
        if stored_inventory_data and stored_inventory_data.get('inventory_by_group'):
            real_inventory_by_group = stored_inventory_data['inventory_by_group']
            total_inventory_value = stored_inventory_data.get('total_inventory_value', 0)
            
            print(f"✅ REAL inventory data found: ${total_inventory_value:,.2f} AUD")
            print(f"✅ Number of groups: {len(real_inventory_by_group)}")
            print(f"✅ Groups: {list(real_inventory_by_group.keys())}")
            
            # Create Cost Analysis format (same logic as in view)
            colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
            cost_analysis_labels = []
            cost_analysis_data = []
            cost_analysis_colors = []
            
            for idx, (group_name, group_value) in enumerate(real_inventory_by_group.items()):
                cost_analysis_labels.append(group_name)
                cost_analysis_data.append(group_value)
                cost_analysis_colors.append(colors[idx % len(colors)])
            
            cost_analysis_datasets = [{
                'label': 'Opening Inventory',
                'data': cost_analysis_data,
                'backgroundColor': cost_analysis_colors,
                'borderColor': cost_analysis_colors,
                'borderWidth': 1
            }]
            
            inventory_by_group_data = {
                'labels': cost_analysis_labels,
                'datasets': cost_analysis_datasets
            }
            
            print(f"\n📊 CHART.JS DATA STRUCTURE:")
            print(f"   Labels: {inventory_by_group_data['labels']}")
            print(f"   Dataset label: {inventory_by_group_data['datasets'][0]['label']}")
            print(f"   Dataset data: {inventory_by_group_data['datasets'][0]['data']}")
            print(f"   First 3 values: {[f'${v:,.0f}' for v in inventory_by_group_data['datasets'][0]['data'][:3]]}")
            
            print(f"\n🔄 JSON OUTPUT (what template receives):")
            json_output = json.dumps(inventory_by_group_data)
            print(f"   JSON length: {len(json_output)} characters")
            print(f"   JSON preview: {json_output[:200]}...")
            
            # Test parent_product_groups
            parent_product_groups = ['All Parent Product Groups'] + cost_analysis_labels
            print(f"\n📋 DROPDOWN OPTIONS:")
            for i, group in enumerate(parent_product_groups):
                print(f"   {i}: {group}")
                
            return True
            
        else:
            print("❌ No stored inventory data found")
            return False
            
    except Exception as e:
        print(f"❌ Error getting stored inventory data: {e}")
        return False

if __name__ == "__main__":
    test_inventory_data()
