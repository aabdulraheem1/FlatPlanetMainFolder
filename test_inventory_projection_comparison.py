#!/usr/bin/env python3
"""
Inventory Projection Model Comparison Test
==========================================
Tests the impact of auto-level optimization and reset on InventoryProjectionModel
for Crawler Systems parent product group in Jul/Aug/Sep 2025.

Usage:
1. Run this script initially to capture BASELINE data
2. Run AUTO-LEVEL optimization from web interface  
3. Run this script with --post-auto-level flag
4. Run RESET from web interface
5. Run this script with --post-reset flag
6. Run this script with --compare flag to see all 3 states

The key question: Does InventoryProjectionModel change when production dates 
are moved within the same month vs across month boundaries?
"""

import os
import django
import sys
import json
from datetime import date, datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import InventoryProjectionModel, CalculatedProductionModel, scenarios
from django.db import connection

class InventoryProjectionTester:
    def __init__(self):
        self.scenario = scenarios.objects.get(version='Jul 25 SPR')
        self.target_parent_group = 'Crawler Systems'
        self.target_months = [date(2025, 7, 1), date(2025, 8, 1), date(2025, 9, 1)]
        self.data_file = 'inventory_projection_test_data.json'
        
    def capture_current_state(self, state_name):
        """Capture the current state of inventory projections"""
        print(f"=== CAPTURING {state_name.upper()} STATE ===")
        
        # Get inventory projection data
        projections = InventoryProjectionModel.objects.filter(
            version=self.scenario,
            parent_product_group=self.target_parent_group,
            month__in=self.target_months
        ).order_by('month')
        
        projection_data = {}
        for proj in projections:
            month_key = proj.month.strftime('%b %Y')
            projection_data[month_key] = {
                'production_aud': float(proj.production_aud),
                'cogs_aud': float(proj.cogs_aud), 
                'revenue_aud': float(proj.revenue_aud),
                'opening_inventory_aud': float(proj.opening_inventory_aud),
                'closing_inventory_aud': float(proj.closing_inventory_aud),
                'created_at': proj.created_at.isoformat(),
                'updated_at': proj.updated_at.isoformat()
            }
        
        # Get underlying production data aggregation
        with connection.cursor() as cursor:
            cursor.execute('''
                SELECT 
                    DATEADD(MONTH, DATEDIFF(MONTH, 0, pouring_date), 0) as month_date,
                    SUM(cogs_aud) as total_cogs,
                    SUM(revenue_aud) as total_revenue,
                    COUNT(*) as record_count,
                    MIN(pouring_date) as earliest_date,
                    MAX(pouring_date) as latest_date
                FROM website_calculatedproductionmodel 
                WHERE version_id = %s 
                  AND parent_product_group = %s
                  AND YEAR(pouring_date) = 2025 
                  AND MONTH(pouring_date) IN (7, 8, 9)
                GROUP BY DATEADD(MONTH, DATEDIFF(MONTH, 0, pouring_date), 0)
                ORDER BY month_date
            ''', [self.scenario.version, self.target_parent_group])
            
            production_results = cursor.fetchall()
        
        production_data = {}
        for row in production_results:
            month_date, total_cogs, total_revenue, record_count, earliest_date, latest_date = row
            month_key = month_date.strftime('%b %Y')
            production_data[month_key] = {
                'total_cogs': float(total_cogs or 0),
                'total_revenue': float(total_revenue or 0),
                'record_count': record_count,
                'earliest_date': earliest_date.isoformat() if earliest_date else None,
                'latest_date': latest_date.isoformat() if latest_date else None
            }
        
        # Create state data
        state_data = {
            'state_name': state_name,
            'timestamp': datetime.now().isoformat(),
            'scenario': self.scenario.version,
            'parent_group': self.target_parent_group,
            'inventory_projections': projection_data,
            'production_aggregation': production_data
        }
        
        # Load existing data or create new file
        try:
            with open(self.data_file, 'r') as f:
                all_data = json.load(f)
        except FileNotFoundError:
            all_data = {}
        
        # Store this state
        all_data[state_name] = state_data
        
        # Save updated data
        with open(self.data_file, 'w') as f:
            json.dump(all_data, f, indent=2)
        
        print(f"✅ Captured {state_name} state with {len(projection_data)} projection records")
        self.print_state_summary(state_data)
        
    def print_state_summary(self, state_data):
        """Print a summary of the state data"""
        print(f"\n📊 {state_data['state_name']} Summary:")
        print(f"   Timestamp: {state_data['timestamp']}")
        
        for month_key in ['Jul 2025', 'Aug 2025', 'Sep 2025']:
            if month_key in state_data['inventory_projections']:
                proj = state_data['inventory_projections'][month_key]
                prod = state_data['production_aggregation'].get(month_key, {})
                
                print(f"\n   {month_key}:")
                print(f"     📈 Production AUD: {proj['production_aud']:>15,.2f}")
                print(f"     💰 COGS AUD:       {proj['cogs_aud']:>15,.2f}")
                print(f"     📦 Opening Inv:    {proj['opening_inventory_aud']:>15,.2f}") 
                print(f"     📦 Closing Inv:    {proj['closing_inventory_aud']:>15,.2f}")
                print(f"     🔢 Prod Records:   {prod.get('record_count', 0):>15}")
                if prod.get('earliest_date') and prod.get('latest_date'):
                    earliest = datetime.fromisoformat(prod['earliest_date']).strftime('%Y-%m-%d')
                    latest = datetime.fromisoformat(prod['latest_date']).strftime('%Y-%m-%d')
                    print(f"     📅 Date Range:     {earliest:>9} to {latest}")
        print()
        
    def compare_all_states(self):
        """Compare all captured states"""
        try:
            with open(self.data_file, 'r') as f:
                all_data = json.load(f)
        except FileNotFoundError:
            print("❌ No data file found. Run captures first.")
            return
            
        states = ['baseline', 'post_auto_level', 'post_reset']
        available_states = [s for s in states if s in all_data]
        
        if len(available_states) < 2:
            print(f"❌ Need at least 2 states. Available: {available_states}")
            return
            
        print("="*100)
        print("🔍 INVENTORY PROJECTION MODEL COMPARISON")
        print("="*100)
        
        # Compare each month across all states
        for month_key in ['Jul 2025', 'Aug 2025', 'Sep 2025']:
            print(f"\n📅 {month_key} COMPARISON:")
            print("-" * 80)
            
            # Headers
            header = f"{'State':<20} {'Production AUD':<15} {'COGS AUD':<15} {'Opening Inv':<15} {'Closing Inv':<15} {'Records':<10}"
            print(header)
            print("-" * len(header))
            
            # Data for each state
            for state_name in available_states:
                if state_name in all_data:
                    state_data = all_data[state_name]
                    if month_key in state_data['inventory_projections']:
                        proj = state_data['inventory_projections'][month_key]
                        prod = state_data['production_aggregation'].get(month_key, {})
                        
                        print(f"{state_name:<20} {proj['production_aud']:>13,.0f} {proj['cogs_aud']:>13,.0f} {proj['opening_inventory_aud']:>13,.0f} {proj['closing_inventory_aud']:>13,.0f} {prod.get('record_count', 0):>8}")
                        
            # Calculate differences if we have multiple states
            if len(available_states) >= 2:
                print("\n🔍 DIFFERENCES FROM BASELINE:")
                baseline_proj = all_data.get('baseline', {}).get('inventory_projections', {}).get(month_key, {})
                
                for state_name in available_states[1:]:  # Skip baseline
                    if state_name in all_data and month_key in all_data[state_name]['inventory_projections']:
                        current_proj = all_data[state_name]['inventory_projections'][month_key]
                        
                        prod_diff = current_proj['production_aud'] - baseline_proj.get('production_aud', 0)
                        cogs_diff = current_proj['cogs_aud'] - baseline_proj.get('cogs_aud', 0)
                        closing_diff = current_proj['closing_inventory_aud'] - baseline_proj.get('closing_inventory_aud', 0)
                        
                        print(f"  {state_name}: Prod Δ{prod_diff:+,.0f}, COGS Δ{cogs_diff:+,.0f}, Closing Δ{closing_diff:+,.0f}")
                        
                        if abs(prod_diff) < 0.01 and abs(cogs_diff) < 0.01:
                            print(f"    ✅ {state_name}: NO CHANGE (as expected for intra-month moves)")
                        else:
                            print(f"    🚨 {state_name}: CHANGE DETECTED!")
            
        print("\n" + "="*100)
        print("🎯 CONCLUSION:")
        if len(available_states) >= 3:
            print("   - If all values are identical across states, it confirms that")
            print("     auto-leveling moves dates within months (no monthly aggregation change)")  
            print("   - If values change, it means dates crossed month boundaries")
        print("="*100)

def main():
    tester = InventoryProjectionTester()
    
    if len(sys.argv) == 1:
        # Default: capture baseline
        print("📋 No arguments provided. Capturing BASELINE state...")
        tester.capture_current_state('baseline')
        print("\n✅ Next steps:")
        print("   1. Run AUTO-LEVEL optimization from web interface")
        print("   2. Run: python test_inventory_projection_comparison.py --post-auto-level")
        
    elif '--post-auto-level' in sys.argv:
        tester.capture_current_state('post_auto_level')
        print("\n✅ Next steps:")
        print("   1. Run RESET from web interface")
        print("   2. Run: python test_inventory_projection_comparison.py --post-reset")
        
    elif '--post-reset' in sys.argv:
        tester.capture_current_state('post_reset')
        print("\n✅ Next steps:")
        print("   1. Run: python test_inventory_projection_comparison.py --compare")
        
    elif '--compare' in sys.argv:
        tester.compare_all_states()
        
    else:
        print("Usage:")
        print("  python test_inventory_projection_comparison.py                    # Capture baseline")
        print("  python test_inventory_projection_comparison.py --post-auto-level  # After auto-level")
        print("  python test_inventory_projection_comparison.py --post-reset       # After reset")
        print("  python test_inventory_projection_comparison.py --compare          # Compare all")

if __name__ == '__main__':
    main()
