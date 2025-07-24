#!/usr/bin/env python
import os
import sys
import django

# Setup Django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MasterDataOrderBook, MasterDataHistoryOfProductionModel
from django.db.models import Count

def check_order_book_duplicates():
    print("=== ORDER BOOK DUPLICATES ACROSS VERSIONS ===")
    
    # Find products that appear in multiple versions
    duplicates = MasterDataOrderBook.objects.values('productkey').annotate(
        version_count=Count('version', distinct=True)
    ).filter(version_count__gt=1).order_by('-version_count')[:10]

    print(f"Found {len(duplicates)} products in order book across multiple versions:\n")
    
    for item in duplicates:
        print(f"Product: {item['productkey']}, Versions: {item['version_count']}")
        
        # Show the actual versions and sites for this product
        records = MasterDataOrderBook.objects.filter(productkey=item['productkey']).values('version__version', 'site')
        for record in records:
            print(f"  Version: {record['version__version']}, Site: {record['site']}")
        print()

def check_production_history_duplicates():
    print("\n=== PRODUCTION HISTORY DUPLICATES ACROSS VERSIONS ===")
    
    # Find products that appear in multiple versions
    duplicates = MasterDataHistoryOfProductionModel.objects.values('Product').annotate(
        version_count=Count('version', distinct=True)
    ).filter(version_count__gt=1).order_by('-version_count')[:10]

    print(f"Found {len(duplicates)} products in production history across multiple versions:\n")
    
    for item in duplicates:
        print(f"Product: {item['Product']}, Versions: {item['version_count']}")
        
        # Show the actual versions and foundries for this product
        records = MasterDataHistoryOfProductionModel.objects.filter(Product=item['Product']).values('version__version', 'Foundry')
        for record in records:
            print(f"  Version: {record['version__version']}, Foundry: {record['Foundry']}")
        print()

if __name__ == "__main__":
    check_order_book_duplicates()
    check_production_history_duplicates()
