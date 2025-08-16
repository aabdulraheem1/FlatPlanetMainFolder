import os
import sys
import django

# Add the project root to the path
project_root = r'C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR'
sys.path.append(project_root)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import *
import datetime

print("=== Debugging Cast-to-Despatch Calculation Bug ===")

# Check the cast_to_despatch data for our product
scenario = ScenarioVersionModel.objects.get(version='V21')
replen = CalcualtedReplenishmentModel.objects.filter(
    version=scenario,
    Product='2037-203-01B',
    ShippingDate='2026-10-01'
).first()

if replen:
    print(f'Replenishment record: {replen.Product}, {replen.Site}, {replen.ShippingDate}')
    
    # Check cast_to_despatch for this site
    cast_data = MasterDataCastToDespatchModel.objects.filter(
        version=scenario,
        Foundry__SiteName=replen.Site
    ).first()
    
    if cast_data:
        print(f'Cast to despatch days: {cast_data.CastToDespatchDays}')
        calculated_pouring = replen.ShippingDate - datetime.timedelta(days=cast_data.CastToDespatchDays)
        print(f'Calculated pouring date: {calculated_pouring}')
        print(f'Pouring month: {calculated_pouring.strftime("%Y-%m")}')
    else:
        print('No cast_to_despatch data found')
else:
    print('No replenishment record found')

print("\n=== Analyzing the Bug in populate_calculated_production.py ===")
print("The bug is on lines 226-230 in the cast_to_despatch calculation:")
print("replenishments.filter(pl.col('ShippingDate') == shipping_date)['Site'][0]")
print("This line tries to filter the ENTIRE DataFrame within a map function!")
print("This is inefficient and can cause data corruption/filtering issues.")
print("In bulk processing, this might fail or return incorrect site values.")
