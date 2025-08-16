#!/usr/bin/env python
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import CalculatedProductionModel, scenarios
from django.db.models import Q

# Get the scenario
scenario = scenarios.objects.get(version='Aug 25 SP')
print(f'Testing search in scenario: {scenario.version}')

# Test basic count
total_records = CalculatedProductionModel.objects.filter(version=scenario).count()
print(f'Total production records: {total_records}')

# Test the exact search that was failing
search_terms = ['t810ep', '810', 'EP', '690', '1690', '1690ep', 'dap']

for search_term in search_terms:
    print(f'\n--- Searching for: "{search_term}" ---')
    
    # Try the same query as the view - PRODUCT CODE ONLY
    matching_products = CalculatedProductionModel.objects.filter(
        version=scenario
    ).filter(
        Q(product__Product__icontains=search_term)
    ).select_related('product')

    count = matching_products.count()
    print(f'Found {count} matching records')
    
    if count > 0:
        # Show first 5 matches
        print('First 5 matches:')
        for i, p in enumerate(matching_products[:5]):
            print(f'  {i+1}. {p.product.Product} - {p.product.ProductDescription}')

# Show some sample product names to see what actually exists
print(f'\n--- Sample of products in database ---')
sample_products = CalculatedProductionModel.objects.filter(version=scenario).select_related('product')[:10]
for i, p in enumerate(sample_products):
    print(f'  {i+1}. {p.product.Product} - {p.product.ProductDescription[:50]}...')
