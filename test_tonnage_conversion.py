#!/usr/bin/env python
import os
import sys
import django

# Add the current directory to Python path
sys.path.append('.')

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

# Now we can import Django models
from website.models import (
    MasterDataHistoryOfProductionModel, 
    MasterDataProductModel
)
from datetime import date

def test_tonnage_conversion():
    """Test the tonnage conversion logic"""
    print("Testing tonnage conversion...")
    
    # Get a sample production record
    production_record = MasterDataHistoryOfProductionModel.objects.first()
    if not production_record:
        print("No production records found")
        return
    
    print(f"Sample product: {production_record.Product}")
    print(f"Production Qty: {production_record.ProductionQty}")
    print(f"Foundry: {production_record.Foundry}")
    
    # Try to get the product master data
    try:
        product_data = MasterDataProductModel.objects.get(Product=production_record.Product)
        print(f"Product found in master data")
        print(f"DressMass: {product_data.DressMass}")
        
        if product_data.DressMass:
            tonnage = production_record.ProductionQty * product_data.DressMass
            print(f"Calculated tonnage: {tonnage}")
            
            # Check if we need to convert from kg to tonnes
            final_tonnage = tonnage / 1000 if tonnage > 10000 else tonnage
            print(f"Final tonnage (after kg->tonnes conversion if needed): {final_tonnage}")
        else:
            print("DressMass is None or 0")
    
    except MasterDataProductModel.DoesNotExist:
        print("Product not found in master data")
    
    # Test COI2 April 2025 data specifically
    print("\n--- Testing COI2 April 2025 ---")
    production_records = MasterDataHistoryOfProductionModel.objects.filter(
        Foundry='COI2',
        ProductionMonth__year=2025,
        ProductionMonth__month=4
    )
    
    print(f"Found {production_records.count()} records for COI2 April 2025")
    
    total_tonnage = 0
    for record in production_records[:5]:  # Test first 5 records
        try:
            product_data = MasterDataProductModel.objects.get(Product=record.Product)
            if product_data.DressMass:
                tonnage = record.ProductionQty * product_data.DressMass
                total_tonnage += tonnage
                print(f"  {record.Product}: {record.ProductionQty} * {product_data.DressMass} = {tonnage}")
            else:
                print(f"  {record.Product}: No DressMass data")
        except MasterDataProductModel.DoesNotExist:
            print(f"  {record.Product}: Not found in master data")
    
    final_total = total_tonnage / 1000 if total_tonnage > 10000 else total_tonnage
    print(f"Total tonnage for first 5 records: {final_total}")

if __name__ == '__main__':
    test_tonnage_conversion()
