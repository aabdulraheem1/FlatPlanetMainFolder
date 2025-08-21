#!/usr/bin/env python
"""
Investigate site assignment for specific product in replenishment command
"""

import os
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import *

def investigate_product_site_assignment(product_name, scenario_version):
    """Investigate why a product was assigned to a specific site"""
    
    print(f'🔍 INVESTIGATING SITE ASSIGNMENT FOR: {product_name}')
    print(f'📋 Scenario: {scenario_version}')
    print('='*80)
    
    try:
        scenario = scenarios.objects.get(version=scenario_version)
        
        # Check if product exists in replenishment data
        replenishment = CalcualtedReplenishmentModel.objects.filter(
            version=scenario,
            Product__Product=product_name
        ).first()
        
        if replenishment:
            print(f'✅ Found replenishment record for {product_name}')
            print(f'    🏭 Assigned Site: {replenishment.Site}')
            print(f'    📅 Period: {replenishment.ShippingDate}')
            print(f'    📦 Quantity: {replenishment.ReplenishmentQty}')
            print()
        else:
            print(f'❌ No replenishment record found for {product_name}')
            return
        
        print('📊 SITE SELECTION HIERARCHY ANALYSIS:')
        print()
        
        # Step 4a: Check if product exists in MasterDataEpicorMethodOfManufacturingModel
        print('4a. Epicor Method of Manufacturing - Product Existence Check:')
        epicor_ops = MasterDataEpicorMethodOfManufacturingModel.objects.filter(
            ProductKey=product_name
        ).values('OperationDesc', 'SiteName')
        
        foundry_operations = ['pouring', 'casting', 'molding', 'moulding']
        foundry_sites = ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1', 'CHI1']
        has_foundry_ops = False
        product_exists_in_epicor = epicor_ops.exists()
        
        if product_exists_in_epicor:
            print(f'    ✅ Product FOUND in Epicor with {epicor_ops.count()} operations:')
            for op in epicor_ops:
                operation_desc = op['OperationDesc'] or ''
                is_foundry = any(fop in operation_desc.lower() for fop in foundry_operations) if operation_desc else False
                if is_foundry:
                    has_foundry_ops = True
                print(f'      - "{operation_desc}" @ {op["SiteName"]} (Foundry op: {is_foundry})')
            
            print(f'    🏭 Has foundry operations: {has_foundry_ops}')
            
            # Step 4b: Determine filtering logic
            if has_foundry_ops:
                print(f'    ✅ Product HAS foundry operations - can be assigned to foundry sites')
                foundry_filtering_applies = False
            else:
                print(f'    ⚠️  Product has NO foundry operations - foundry sites will be FILTERED OUT')
                foundry_filtering_applies = True
        else:
            print(f'    ❌ Product NOT FOUND in Epicor Method of Manufacturing')
            print(f'    ✅ Since product is NOT in Epicor, it can be assigned to ANY site (including foundry sites)')
            foundry_filtering_applies = False
            has_foundry_ops = None  # Not applicable
        
        if replenishment.Site in foundry_sites:
            if foundry_filtering_applies:
                print(f'    🚨 WARNING: Product assigned to foundry site {replenishment.Site} but should be filtered out!')
            else:
                print(f'    ✅ Product correctly assigned to foundry site {replenishment.Site}')
        else:
            print(f'    ✅ Product assigned to non-foundry site {replenishment.Site}')
        
        print()
        
        # Step 4b: Check MasterDataManuallyAssignProductionRequirement
        print('4b. Manual Assignment Check:')
        manual_assign = MasterDataManuallyAssignProductionRequirement.objects.filter(
            Product__Product=product_name
        ).values('Site__SiteName')
        
        if manual_assign.exists():
            print(f'    ✅ Found {manual_assign.count()} manual assignments for {product_name}:')
            for assign in manual_assign:
                print(f'      - Site: {assign["Site__SiteName"]}')
        else:
            print(f'    ❌ No manual assignments found for {product_name}')
        
        print()
        
        # Step 4c: Check MasterDataOrderBook  
        print('4c. Order Book Check:')
        order_book = MasterDataOrderBook.objects.filter(
            productkey=product_name
        ).values('site')
        
        if order_book.exists():
            print(f'    ✅ Found {order_book.count()} order book entries for {product_name}:')
            for order in order_book:
                print(f'      - Site: {order["site"]}')
        else:
            print(f'    ❌ No order book entries found for {product_name}')
        
        print()
        
        # Step 4d: Check MasterDataHistoryOfProductionModel
        print('4d. Production History Check:')
        prod_history = MasterDataHistoryOfProductionModel.objects.filter(
            Product=product_name
        ).values('Foundry', 'ProductionQty').distinct()
        
        if prod_history.exists():
            print(f'    ✅ Found {prod_history.count()} production history entries for {product_name}:')
            for history in prod_history:
                print(f'      - Site: {history["Foundry"]}, Qty: {history["ProductionQty"]}')
        else:
            print(f'    ❌ No production history found for {product_name}')
        
        print()
        
        # Final summary
        print('🔍 SITE SELECTION SUMMARY:')
        print(f'   Final Assignment: {replenishment.Site}')
        
        # Determine which step likely determined the site
        if manual_assign.exists():
            manual_sites = [assign["Site__SiteName"] for assign in manual_assign]
            if str(replenishment.Site) in manual_sites:
                print(f'   🎯 Likely selected via: Step 4b - Manual Assignment')
        elif order_book.exists():
            order_sites = [order["site"] for order in order_book]  
            if str(replenishment.Site) in order_sites:
                print(f'   🎯 Likely selected via: Step 4c - Order Book')
        elif prod_history.exists():
            history_sites = [history["Foundry"] for history in prod_history]
            if str(replenishment.Site) in history_sites:
                print(f'   🎯 Likely selected via: Step 4d - Production History')
        else:
            print(f'   🎯 Likely selected via: Step 4e - Supplier Priority (fallback)')
        
        print()
        
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    investigate_product_site_assignment('oh8000s1845-1a', 'Aug 25 SPR')
