#!/usr/bin/env python
"""
Test script to populate OpeningInventorySnapshot for Aug 25 SP scenario with snapshot date 2025-07-31
"""
import os
import sys
import django
from datetime import datetime

# Add the project directory to Python path
project_root = os.path.join(os.path.dirname(__file__), 'SPR')
sys.path.insert(0, project_root)

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

def test_populate_snapshot():
    """Test the populate_opening_inventory_snapshot functionality"""
    try:
        from website.models import scenarios, OpeningInventorySnapshot
        
        print("🧪 TESTING OpeningInventorySnapshot population")
        print("=" * 60)
        
        # Get the scenario
        scenario_version = "Aug 25 SP"
        scenario = scenarios.objects.get(version=scenario_version)
        print(f"✅ Found scenario: {scenario.name} ({scenario.version})")
        
        # Set snapshot date
        snapshot_date = datetime.strptime('2025-07-31', '%Y-%m-%d').date()
        print(f"📅 Snapshot date: {snapshot_date}")
        
        print("\n🚀 Starting OpeningInventorySnapshot.get_or_create_snapshot()...")
        
        # Call the populate function
        result = OpeningInventorySnapshot.get_or_create_snapshot(
            scenario=scenario,
            snapshot_date=snapshot_date,
            force_refresh=True,
            user=None,
            reason='test_populate'
        )
        
        if result:
            total_value = sum(result.values())
            group_count = len(result)
            
            print(f"\n✅ SUCCESS!")
            print(f"📊 Created {group_count} parent product groups")
            print(f"💰 Total inventory value: ${total_value:,.2f}")
            print("\n📋 Parent Product Groups:")
            
            for group, value in sorted(result.items()):
                print(f"   • {group}: ${value:,.2f}")
                
        else:
            print(f"\n❌ FAILED!")
            print(f"⚠️  No data was returned from SQL Server")
            
        print("\n" + "=" * 60)
        print("🧪 TEST COMPLETED")
        
    except Exception as e:
        print(f"\n❌ ERROR during test: {e}")
        import traceback
        print(f"\nFull traceback:")
        print(traceback.format_exc())

if __name__ == "__main__":
    test_populate_snapshot()
