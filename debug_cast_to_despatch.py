#!/usr/bin/env python
import os
import sys
import django
from pathlib import Path

# Add the SPR directory to Python path
spr_dir = Path(__file__).parent / "SPR"
sys.path.insert(0, str(spr_dir))

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import CalculatedProductionModel, MasterDataCastToDespatchModel, scenarios, CalcualtedReplenishmentModel

# Get scenario
scenario = scenarios.objects.get(version="Aug 25 SPR")

print("=== BK57592A Production Records ===")
production_records = CalculatedProductionModel.objects.filter(
    version=scenario, 
    product="BK57592A"
).order_by('pouring_date')[:5]

for prod in production_records:
    print(f"Site: {prod.site}, Pouring Date: {prod.pouring_date}, Qty: {prod.production_quantity}")
    print(f"  Product Group: {prod.product_group}")

print("\n=== Cast-to-Despatch Data ===")
cast_records = MasterDataCastToDespatchModel.objects.filter(version=scenario)
for cast in cast_records:
    print(f"Site: {cast.Foundry.SiteName}, Days: {cast.CastToDespatchDays}")

# Check if BK57592A was assigned to a site that has cast-to-despatch data
print("\n=== BK57592A Site Analysis ===")
if production_records:
    first_record = production_records[0]
    site = first_record.site
    print(f"BK57592A assigned to site: {site}")
    
    # Check if this site has cast-to-despatch data
    cast_for_site = MasterDataCastToDespatchModel.objects.filter(
        version=scenario,
        Foundry__SiteName=site
    ).first()
    
    if cast_for_site:
        print(f"Cast-to-despatch days for {site}: {cast_for_site.CastToDespatchDays}")
    else:
        print(f"No cast-to-despatch data found for site: {site}")

# Check replenishment data for this product
print("\n=== BK57592A Replenishment Data ===")
replenishment_records = CalcualtedReplenishmentModel.objects.filter(
    version=scenario,
    Product="BK57592A"
).order_by('ShippingDate')[:5]

for rep in replenishment_records:
    print(f"Site: {rep.Site}, Shipping Date: {rep.ShippingDate}, Qty: {rep.Qty}")
