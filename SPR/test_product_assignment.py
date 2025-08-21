#!/usr/bin/env python3

import os
import sys
import django

# Add the project path
sys.path.insert(0, r'C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

sys.path.append('C:/Users/aali/OneDrive - bradken.com/Data/Training/SPR/SPR/website/management/commands')

from populate_calculated_replenishment_v3_optimized import Command

# Create command instance
command = Command()

# Test specific product
product_name = 'oh8000s1845-1a'
print(f'🔍 Testing site selection for product: {product_name}')
print('')

# Get product from database
from website.models import MasterDataProductModel
try:
    product = MasterDataProductModel.objects.get(Product=product_name)
    print(f'✅ Product found: {product.Product}')
    print(f'   Description: {product.ProductDescription}')
    print('')
    
    # Test site selection
    result = command.select_site(product)
    site_result = result.SiteKey if result else "No site assigned"
    print(f'🎯 Site selection result: {site_result}')
    print('')
    
    # Check Epicor operations
    from website.models import MasterDataEpicorMethodOfManufacturingModel
    epicor_operations = list(MasterDataEpicorMethodOfManufacturingModel.objects.filter(
        ProductKey=product_name
    ).values_list('OperationDesc', flat=True))
    
    print(f'🏭 Epicor operations for {product_name}:')
    if epicor_operations:
        for i, op in enumerate(epicor_operations, 1):
            print(f'   {i}. {op}')
        
        # Check if foundry operations exist
        foundry_ops = [op for op in epicor_operations if op and op.lower() in ['pouring', 'casting', 'molding', 'moulding']]
        print('')
        print(f'🔥 Foundry operations found: {foundry_ops}')
        
        if foundry_ops:
            print('   ➡️ Product has foundry operations - can be assigned to foundry sites')
        else:
            print('   ➡️ Product has NO foundry operations - filtered from foundry sites')
    else:
        print('   ❌ No Epicor operations found')
        print('   ➡️ Product not in Epicor - can be assigned to any site')
    
    print('')
    
    # Show assignment hierarchy for debugging
    print('🔍 Assignment hierarchy check:')
    
    # 1. Manual assignment
    from website.models import MasterDataManuallyAssignProductionRequirement
    manual_site = MasterDataManuallyAssignProductionRequirement.objects.filter(
        Product=product
    ).first()
    if manual_site:
        print(f'   1. Manual assignment: {manual_site.Site.SiteKey}')
    else:
        print('   1. Manual assignment: None')
    
    # 2. Order book
    from website.models import MasterDataOrderBook
    order_sites = list(MasterDataOrderBook.objects.filter(
        productkey=product_name
    ).values_list('site', flat=True).distinct())
    if order_sites:
        print(f'   2. Order book sites: {order_sites}')
    else:
        print('   2. Order book sites: None')
    
    # 3. Production history
    from website.models import MasterDataHistoryOfProductionModel
    history_sites = list(MasterDataHistoryOfProductionModel.objects.filter(
        Product=product
    ).values_list('Foundry', flat=True).distinct())
    if history_sites:
        print(f'   3. Production history sites: {history_sites}')
    else:
        print('   3. Production history sites: None')
    
    # 4. Supplier assignment (now removed)
    print('   4. Supplier assignment: Removed (no fallback)')
        
except MasterDataProductModel.DoesNotExist:
    print(f'❌ Product {product_name} not found in database')
