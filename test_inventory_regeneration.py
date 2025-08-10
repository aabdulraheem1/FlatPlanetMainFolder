#!/usr/bin/env python
"""
Test inventory projection regeneration for Jul 25 SPR scenario
"""
import os
import sys
import django

# Add the project path to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import scenarios, InventoryProjectionModel, CalculatedProductionModel
from website.customized_function import populate_inventory_projection_model

def test_inventory_regeneration():
    print("🔍 Testing inventory projection regeneration for Jul 25 SPR scenario...")
    
    # Check if Jul 25 SPR scenario exists
    scenario = scenarios.objects.filter(version='Jul 25 SPR').first()
    if not scenario:
        print("❌ Jul 25 SPR scenario not found")
        print("Available scenarios:")
        for s in scenarios.objects.all()[:10]:
            print(f"  - {s.version}")
        return
    
    print(f"✅ Found scenario: {scenario}")
    
    # Check current inventory projection count
    before_count = InventoryProjectionModel.objects.filter(version=scenario).count()
    print(f"📊 Current inventory projections: {before_count}")
    
    # Check if there's any CalculatedProductionModel data for this scenario
    production_count = CalculatedProductionModel.objects.filter(version=scenario).count()
    print(f"🏭 Production records: {production_count}")
    
    # Check specifically for Crawler Systems production data
    crawler_production = CalculatedProductionModel.objects.filter(
        version=scenario,
        parent_product_group__icontains='Crawler'
    )
    print(f"🚛 Crawler Systems production records: {crawler_production.count()}")
    
    if crawler_production.count() > 0:
        sample_crawler = crawler_production.first()
        print(f"   Sample: {sample_crawler.parent_product_group} - {sample_crawler.pouring_date} - {sample_crawler.production_aud} AUD")
    
    # Test regeneration
    print("\n🔄 Testing inventory projection regeneration...")
    try:
        result = populate_inventory_projection_model('Jul 25 SPR')
        print(f"✅ Regeneration result: {result}")
        
        # Check after count
        after_count = InventoryProjectionModel.objects.filter(version=scenario).count()
        print(f"📈 After regeneration: {after_count} records")
        
        # Check if Crawler Systems inventory projections exist
        crawler_projections = InventoryProjectionModel.objects.filter(
            version=scenario, 
            parent_product_group__icontains='Crawler'
        )
        print(f"🚛 Crawler Systems inventory projections: {crawler_projections.count()}")
        
        if crawler_projections.count() > 0:
            # Show a sample
            sample = crawler_projections.first()
            print(f"   Sample projection: {sample.parent_product_group} - {sample.month} - Opening: {sample.opening_inventory_aud:.2f}, Closing: {sample.closing_inventory_aud:.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during regeneration: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_inventory_regeneration()
