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
from collections import defaultdict

def check_products_with_different_sites():
    print("=== PRODUCTS WITH DIFFERENT SITES ACROSS VERSIONS ===")
    
    # Find products that appear in multiple versions with different sites
    all_products = MasterDataOrderBook.objects.values('productkey', 'version__version', 'site')
    
    product_versions = defaultdict(set)
    for record in all_products:
        product_versions[record['productkey']].add((record['version__version'], record['site']))
    
    different_sites = []
    for product, version_sites in product_versions.items():
        if len(version_sites) > 1:
            sites = set([site for version, site in version_sites])
            if len(sites) > 1:  # Multiple different sites
                different_sites.append((product, version_sites))
    
    print(f"Found {len(different_sites)} products with different sites across versions:\n")
    
    for product, version_sites in different_sites[:10]:  # Show first 10
        print(f"Product: {product}")
        for version, site in sorted(version_sites):
            print(f"  Version: {version}, Site: {site}")
        print()

def check_production_history_with_different_foundries():
    print("\n=== PRODUCTION HISTORY WITH DIFFERENT FOUNDRIES ACROSS VERSIONS ===")
    
    # Find products that appear in multiple versions with different foundries
    all_products = MasterDataHistoryOfProductionModel.objects.values('Product', 'version__version', 'Foundry')
    
    product_versions = defaultdict(set)
    for record in all_products:
        product_versions[record['Product']].add((record['version__version'], record['Foundry']))
    
    different_foundries = []
    for product, version_foundries in product_versions.items():
        if len(version_foundries) > 1:
            foundries = set([foundry for version, foundry in version_foundries])
            if len(foundries) > 1:  # Multiple different foundries
                different_foundries.append((product, version_foundries))
    
    print(f"Found {len(different_foundries)} products with different foundries across versions:\n")
    
    for product, version_foundries in different_foundries[:10]:  # Show first 10
        print(f"Product: {product}")
        for version, foundry in sorted(version_foundries):
            print(f"  Version: {version}, Foundry: {foundry}")
        print()

if __name__ == "__main__":
    check_products_with_different_sites()
    check_production_history_with_different_foundries()
