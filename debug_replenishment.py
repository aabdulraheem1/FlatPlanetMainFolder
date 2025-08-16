#!/usr/bin/env python
"""
Debug script to track why product '1979-102-01C' doesn't show up in CalcualtedReplenishmentModel
for scenario 'Aug 25 SP'
"""

import os
import django
import sys

# Add the project directory to Python path
sys.path.append('c:/Users/aali/OneDrive - bradken.com/Data/Training/SPR/SPR')

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import (
    scenarios, SMART_Forecast_Model, MasterDataProductModel, 
    MasterDataPlantModel, CalcualtedReplenishmentModel,
    MasterDataOrderBook, MasterDataHistoryOfProductionModel, 
    MasterDataEpicorSupplierMasterDataModel, 
    MasterDataManuallyAssignProductionRequirement
)

def debug_product_replenishment():
    version = 'Aug 25 SP'
    product_code = '1979-102-01C'
    
    print("=" * 80)
    print(f"🔍 DEBUGGING REPLENISHMENT FOR PRODUCT: {product_code}")
    print(f"🔍 SCENARIO VERSION: {version}")
    print("=" * 80)
    
    # Step 1: Check if scenario exists
    try:
        scenario = scenarios.objects.get(version=version)
        print(f"✅ Step 1: Scenario '{version}' found (version: {scenario.version})")
    except scenarios.DoesNotExist:
        print(f"❌ Step 1: Scenario '{version}' NOT FOUND!")
        return
    
    # Step 2: Check if product exists in master data
    try:
        product = MasterDataProductModel.objects.get(Product=product_code)
        print(f"✅ Step 2: Product '{product_code}' found in master data")
        print(f"   - Description: {product.ProductDescription}")
        print(f"   - Product Group: {product.ProductGroup}")
        print(f"   - Parent Group: {product.ParentProductGroupDescription}")
    except MasterDataProductModel.DoesNotExist:
        print(f"❌ Step 2: Product '{product_code}' NOT FOUND in master data!")
        return
    
    # Step 3: Check SMART Forecast data
    forecast_records = SMART_Forecast_Model.objects.filter(
        version=scenario,
        Product=product_code,
        Qty__gt=0
    ).exclude(
        Data_Source__in=['Fixed Plant', 'Revenue Forecast']
    )
    
    print(f"\n📊 Step 3: SMART Forecast Analysis")
    print(f"   - Total forecast records: {forecast_records.count()}")
    
    if forecast_records.exists():
        for record in forecast_records[:5]:  # Show first 5 records
            print(f"   - Date: {record.Period_AU}, Qty: {record.Qty}, "
                  f"Location: {record.Location}, Data Source: {record.Data_Source}")
    else:
        print(f"   ❌ NO FORECAST DATA found for product {product_code}")
        
        # Check if any forecast exists without filters
        all_forecast = SMART_Forecast_Model.objects.filter(
            version=scenario,
            Product=product_code
        )
        print(f"   - Total records (including all data sources): {all_forecast.count()}")
        
        if all_forecast.exists():
            print("   - Sample records found:")
            for record in all_forecast[:3]:
                print(f"     * Date: {record.Period_AU}, Qty: {record.Qty}, "
                      f"Data Source: {record.Data_Source}")
        return
    
    # Step 4: Check site extraction from Location
    from website.management.commands.populate_calculated_replenishment_v2 import extract_site_code
    
    print(f"\n🏭 Step 4: Site Code Extraction Analysis")
    
    valid_plants = set(MasterDataPlantModel.objects.values_list('SiteName', flat=True))
    print(f"   - Total valid plant sites: {len(valid_plants)}")
    
    forecast_with_sites = []
    for record in forecast_records:
        site_code = extract_site_code(record.Location)
        is_valid = site_code in valid_plants if site_code else False
        forecast_with_sites.append({
            'record': record,
            'site_code': site_code,
            'is_valid': is_valid
        })
        
        print(f"   - Location: '{record.Location}' → Site: '{site_code}' → Valid: {is_valid}")
    
    valid_forecast_count = sum(1 for f in forecast_with_sites if f['is_valid'])
    print(f"   - Records with valid sites: {valid_forecast_count}/{len(forecast_with_sites)}")
    
    if valid_forecast_count == 0:
        print(f"   ❌ NO VALID SITE CODES found in forecast locations!")
        print(f"   - Available plant sites: {sorted(list(valid_plants)[:10])}...")  # Show first 10
        return
    
    # Step 5: Check site assignment logic
    print(f"\n🎯 Step 5: Site Assignment Logic Analysis")
    
    # Manual assignments
    manual_assignments = MasterDataManuallyAssignProductionRequirement.objects.filter(
        version=scenario,
        Product__Product=product_code
    )
    print(f"   - Manual assignments: {manual_assignments.count()}")
    for assignment in manual_assignments:
        print(f"     * Assigned to: {assignment.Site.SiteName}")
    
    # Order Book
    order_book = MasterDataOrderBook.objects.filter(
        version=scenario,
        productkey=product_code
    ).exclude(site__isnull=True).exclude(site__exact='')
    print(f"   - Order Book entries: {order_book.count()}")
    for ob in order_book[:3]:
        print(f"     * Site: {ob.site}")
    
    # Production History
    production_history = MasterDataHistoryOfProductionModel.objects.filter(
        version=scenario,
        Product=product_code
    ).exclude(Foundry__isnull=True).exclude(Foundry__exact='')
    print(f"   - Production History entries: {production_history.count()}")
    for prod in production_history[:3]:
        print(f"     * Foundry: {prod.Foundry}")
    
    # Supplier
    suppliers = MasterDataEpicorSupplierMasterDataModel.objects.filter(
        version=scenario,
        PartNum=product_code
    ).exclude(VendorID__isnull=True).exclude(VendorID__exact='')
    print(f"   - Supplier entries: {suppliers.count()}")
    for sup in suppliers[:3]:
        print(f"     * Vendor: {sup.VendorID}")
    
    # Step 6: Simulate the site selection process
    print(f"\n🔄 Step 6: Site Selection Simulation")
    
    from website.management.commands.populate_calculated_replenishment_v2 import select_site
    
    # Build lookup dictionaries like in the actual command
    order_book_map = {
        (ob.version.version, ob.productkey): ob.site
        for ob in MasterDataOrderBook.objects.filter(version=scenario)
        .exclude(site__isnull=True).exclude(site__exact='')
    }
    
    production_map = {
        (prod.version.version, prod.Product): prod.Foundry
        for prod in MasterDataHistoryOfProductionModel.objects.filter(version=scenario)
        .exclude(Foundry__isnull=True).exclude(Foundry__exact='')
    }
    
    supplier_map = {
        (sup.version.version, sup.PartNum): sup.VendorID
        for sup in MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario)
        .exclude(VendorID__isnull=True).exclude(VendorID__exact='')
    }
    
    manual_assign_map = {
        (m.version.version, m.Product.Product): m.Site.SiteName
        for m in MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario)
        .select_related('Product', 'Site')
        if m.Product and m.Site
    }
    
    plant_map = {p.SiteName: p for p in MasterDataPlantModel.objects.all()}
    foundry_sites = {'XUZ1', 'MTJ1', 'COI2', 'MER1', 'WUN1', 'WOD1', 'CHI1'}
    
    def can_assign_foundry_fn(product):
        return (scenario.version, product) in production_map
    
    # Test site selection for each valid forecast record
    for forecast_data in forecast_with_sites:
        if not forecast_data['is_valid']:
            continue
            
        record = forecast_data['record']
        
        selected_site = select_site(
            product_code,
            record.Period_AU,
            record.Customer_code,
            record.Forecast_Region,
            scenario,
            order_book_map,
            production_map,
            supplier_map,
            manual_assign_map,
            plant_map,
            foundry_sites,
            can_assign_foundry_fn
        )
        
        print(f"   - For period {record.Period_AU}: Selected site = '{selected_site}'")
        
        if not selected_site:
            print(f"     ❌ NO SITE SELECTED - This explains why no replenishment record was created!")
            print(f"     - Manual assignment: {(scenario.version, product_code) in manual_assign_map}")
            print(f"     - Order book: {(scenario.version, product_code) in order_book_map}")
            print(f"     - Production history: {(scenario.version, product_code) in production_map}")
            print(f"     - Supplier: {(scenario.version, product_code) in supplier_map}")
    
    # Step 7: Check existing replenishment records
    print(f"\n📋 Step 7: Existing Replenishment Records")
    existing_records = CalcualtedReplenishmentModel.objects.filter(
        version=scenario,
        Product_id=product_code
    )
    print(f"   - Existing replenishment records: {existing_records.count()}")
    
    for record in existing_records:
        print(f"     * Site: {record.Site_id}, Date: {record.ShippingDate}, Qty: {record.ReplenishmentQty}")
    
    print("\n" + "=" * 80)
    print("🏁 DEBUG ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    debug_product_replenishment()
