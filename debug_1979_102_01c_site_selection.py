#!/usr/bin/env python3
"""
Debug the replenishment calculation for product '1979-102-01C' to understand
why no replenishment records are generated.
"""

import os
import sys
import django

# Setup Django environment
sys.path.append('C:\\Users\\aali\\OneDrive - bradken.com\\Data\\Training\\SPR\\SPR')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import (
    scenarios,
    SMART_Forecast_Model,
    MasterDataOrderBook,
    MasterDataHistoryOfProductionModel,
    MasterDataEpicorSupplierMasterDataModel,
    MasterDataManuallyAssignProductionRequirement,
    MasterDataPlantModel,
    MasterDataEpicorMethodOfManufacturingModel
)

def debug_site_selection():
    product_code = '1979-102-01C'
    scenario_version = 'Aug 25 SPR'
    
    print(f"🔍 Debugging site selection for product: {product_code}")
    print(f"📅 Scenario: {scenario_version}")
    print("=" * 80)
    
    try:
        scenario = scenarios.objects.get(version=scenario_version)
        print(f"✅ Found scenario: {scenario.version}")
    except scenarios.DoesNotExist:
        print(f"❌ Scenario '{scenario_version}' not found")
        return
    
    # Check all site assignment sources
    print("\n1️⃣  MANUAL ASSIGNMENT CHECK")
    print("=" * 50)
    manual_assigns = MasterDataManuallyAssignProductionRequirement.objects.filter(
        version=scenario,
        Product__Product=product_code
    ).select_related('Product', 'Site')
    
    print(f"Found {manual_assigns.count()} manual assignments")
    for manual in manual_assigns:
        print(f"   🎯 Manual: {manual.Product.Product} → {manual.Site.SiteName}")
    
    print("\n2️⃣  ORDER BOOK CHECK")
    print("=" * 50)
    order_books = MasterDataOrderBook.objects.filter(
        version=scenario,
        productkey=product_code
    )
    
    print(f"Found {order_books.count()} order book entries")
    for ob in order_books:
        print(f"   📋 Order Book: {ob.productkey} → {ob.site}")
    
    print("\n3️⃣  PRODUCTION HISTORY CHECK")
    print("=" * 50)
    production_history = MasterDataHistoryOfProductionModel.objects.filter(
        version=scenario,
        Product=product_code
    )
    
    print(f"Found {production_history.count()} production history records")
    for ph in production_history:
        print(f"   🏭 Production History: {ph.Product} → {ph.Foundry}")
    
    print("\n4️⃣  SUPPLIER CHECK")
    print("=" * 50)
    suppliers = MasterDataEpicorSupplierMasterDataModel.objects.filter(
        version=scenario,
        PartNum=product_code
    )
    
    print(f"Found {suppliers.count()} supplier records")
    for sup in suppliers:
        print(f"   🚚 Supplier: {sup.PartNum} → {sup.VendorID}")
    
    print("\n5️⃣  METHOD OF MANUFACTURING CHECK")
    print("=" * 50)
    mom_records = MasterDataEpicorMethodOfManufacturingModel.objects.filter(
        ProductKey=product_code
    )
    
    print(f"Found {mom_records.count()} method of manufacturing records")
    for mom in mom_records:
        print(f"   🔧 MOM: {mom.ProductKey} at {mom.SiteName} → {mom.OperationDesc}")
    
    print("\n6️⃣  FOUNDRY SITES CHECK")
    print("=" * 50)
    foundry_sites = {'XUZ1', 'MTJ1', 'COI2', 'MER1', 'WUN1', 'WOD1', 'CHI1'}
    print(f"Foundry sites: {foundry_sites}")
    
    # Check if any assigned sites are foundries
    all_assigned_sites = set()
    for ob in order_books:
        if ob.site:
            all_assigned_sites.add(ob.site)
    for ph in production_history:
        if ph.Foundry:
            all_assigned_sites.add(ph.Foundry)
    for sup in suppliers:
        if sup.VendorID:
            all_assigned_sites.add(sup.VendorID)
    for manual in manual_assigns:
        if manual.Site:
            all_assigned_sites.add(manual.Site.SiteName)
    
    print(f"All assigned sites: {all_assigned_sites}")
    foundry_assignments = all_assigned_sites.intersection(foundry_sites)
    print(f"Foundry assignments: {foundry_assignments}")
    
    print("\n7️⃣  FORECAST LOCATION ANALYSIS")
    print("=" * 50)
    forecast_records = SMART_Forecast_Model.objects.filter(
        version=scenario,
        Product=product_code
    )
    
    for record in forecast_records:
        print(f"   📍 Forecast Location: {record.Location}")
        
        # Extract site code from location
        if record.Location and '-' in record.Location:
            site_code = record.Location.split('-')[1]  # CA01-MTJ1 → MTJ1
            print(f"   🎯 Extracted site code: {site_code}")
            
            # Check if this matches any assignment
            if site_code in all_assigned_sites:
                print(f"   ✅ Site code matches assignment: {site_code}")
                print(f"   📝 This would be DIRECT PRODUCTION (no replenishment needed)")
            else:
                print(f"   ❌ Site code does not match any assignment")
                print(f"   📝 This would require REPLENISHMENT")
        else:
            print(f"   ⚠️  Cannot extract site code from location: {record.Location}")

if __name__ == '__main__':
    debug_site_selection()
