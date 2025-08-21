#!/usr/bin/env python
"""
Script to investigate why BK63071B is marked as 'repeat'
"""

import os
import sys
import django

# Add the project directory to Python path
project_path = r'C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR'
if project_path not in sys.path:
    sys.path.append(project_path)

# Set Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MasterDataProductModel

def investigate_bk63071b():
    """Investigate BK63071B product data"""
    
    print("🔍 Investigating BK63071B Product Data")
    print("=" * 50)
    
    try:
        # Get the product
        product = MasterDataProductModel.objects.get(Product='BK63071B')
        
        print(f"📦 Product: {product.Product}")
        print(f"📝 Description: {product.ProductDescription}")
        print()
        
        print("🔍 INVOICE/CUSTOMER DATA:")
        print(f"   product_type: '{product.product_type}' {'❌ REPEAT!' if product.product_type == 'repeat' else '✅ NEW' if product.product_type == 'new' else '⚪ BLANK'}")
        print(f"   latest_customer_name: '{product.latest_customer_name}'")
        print(f"   latest_invoice_date: {product.latest_invoice_date}")
        print(f"   customer_data_last_updated: {product.customer_data_last_updated}")
        print()
        
        print("🔍 WHY IS IT MARKED AS REPEAT?")
        
        if product.product_type == 'repeat':
            if product.latest_invoice_date:
                print(f"   ✅ REASON: Found in PowerBI invoices with date {product.latest_invoice_date}")
                print(f"   ✅ Customer: {product.latest_customer_name}")
                print(f"   ✅ This means BK63071B HAS been invoiced before (has invoice history)")
            else:
                print(f"   ❌ INCONSISTENT: Marked as 'repeat' but no invoice date!")
                print(f"   ❌ This should have been caught by the cleanup logic")
                
        elif product.product_type == 'new':
            print(f"   ✅ REASON: Marked as 'new' - never been invoiced")
            print(f"   ✅ Invoice date should be None: {product.latest_invoice_date}")
            
        else:
            print(f"   ⚪ REASON: Not processed yet or completely blank")
        
        print()
        print("🔍 DATA SOURCE TRACKING:")
        print(f"   is_user_created: {product.is_user_created}")
        print(f"   last_imported_from_epicor: {product.last_imported_from_epicor}")
        print(f"   created_by_user: {product.created_by_user}")
        print(f"   last_modified_by_user: {product.last_modified_by_user}")
        print()
        
        print("🎯 SUMMARY:")
        if product.product_type == 'repeat' and product.latest_invoice_date:
            print(f"   ✅ BK63071B is correctly marked as 'repeat'")
            print(f"   ✅ It has invoice history from {product.latest_invoice_date}")
            print(f"   ✅ Last customer was: {product.latest_customer_name}")
        elif product.product_type == 'repeat' and not product.latest_invoice_date:
            print(f"   ❌ BK63071B is incorrectly marked as 'repeat'")
            print(f"   ❌ It has no invoice date but marked as repeat")
            print(f"   ❌ This needs cleanup - should be 'new'")
        elif product.product_type == 'new':
            print(f"   ✅ BK63071B is correctly marked as 'new' (never invoiced)")
        else:
            print(f"   ⚪ BK63071B has not been processed yet")
            
    except MasterDataProductModel.DoesNotExist:
        print("❌ BK63071B not found in database!")
        print("   This product may not exist or may be spelled differently")
        
        # Search for similar products
        similar = MasterDataProductModel.objects.filter(Product__icontains='BK63071')
        if similar.exists():
            print("\n🔍 Found similar products:")
            for p in similar[:5]:
                print(f"   - {p.Product}")
        else:
            print("\n🔍 No similar products found")

if __name__ == "__main__":
    investigate_bk63071b()
