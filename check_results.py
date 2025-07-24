#!/usr/bin/env python
import os
import sys
import django

# Add the project directory to sys.path
sys.path.insert(0, 'c:/Users/aali/OneDrive - bradken.com/Data/Training/SPR/SPR')

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import CalcualtedReplenishmentModel, CalculatedProductionModel, scenarios

# Get the scenario
scenario = scenarios.objects.get(version='Jul 25 SPR Inv')

# Get replenishment records for the test product
replenishment = CalcualtedReplenishmentModel.objects.filter(
    version=scenario, 
    Product__Product='1980-106-01B'
).order_by('ShippingDate')

# Get production records for the test product
production = CalculatedProductionModel.objects.filter(
    version=scenario, 
    product__Product='1980-106-01B'
).order_by('pouring_date')

print('=== V2 COMMAND RESULTS FOR PRODUCT 1980-106-01B ===')
print()
print('=== REPLENISHMENT RECORDS ===')
total_replenishment = 0
for r in replenishment:
    print(f'Product: {r.Product.Product}')
    print(f'  Location: {r.Location}')
    print(f'  Production Site: {r.Site.SiteName}')
    print(f'  Shipping Date: {r.ShippingDate}')
    print(f'  Replenishment Qty: {r.ReplenishmentQty}')
    print()
    total_replenishment += r.ReplenishmentQty

print(f'Total Replenishment Quantity: {total_replenishment}')
print()

print('=== PRODUCTION RECORDS ===')
total_production = 0
for p in production:
    print(f'Product: {p.product.Product}')
    print(f'  Production Site: {p.site.SiteName}')
    print(f'  Pouring Date: {p.pouring_date}')
    print(f'  Production Qty: {p.production_quantity}')
    print()
    total_production += p.production_quantity

print(f'Total Production Quantity: {total_production}')
