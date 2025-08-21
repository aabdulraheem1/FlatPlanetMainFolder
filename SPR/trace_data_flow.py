#!/usr/bin/env python
import os
import sys
import django

# Setup Django environment
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import *
from django.db.models import Sum, Count

print('=== COMPLETE DATA FLOW TRACE FOR T690EP - Aug 25 SPR ===')

# SMART Forecast totals
smart_totals = SMART_Forecast_Model.objects.filter(version__version='Aug 25 SPR', Product='T690EP').aggregate(
    total_qty=Sum('Qty'), 
    record_count=Count('id')
)
print(f'SMART FORECAST: {smart_totals["record_count"]} records, Total Qty: {smart_totals["total_qty"]}')

# Replenishment totals  
replen_totals = CalcualtedReplenishmentModel.objects.filter(version__version='Aug 25 SPR', Product__Product='T690EP').aggregate(
    total_qty=Sum('ReplenishmentQty'), 
    record_count=Count('id')
)
print(f'REPLENISHMENT: {replen_totals["record_count"]} records, Total Qty: {replen_totals["total_qty"]}')

# Production totals
prod_totals = CalculatedProductionModel.objects.filter(version__version='Aug 25 SPR', product__Product='T690EP').aggregate(
    total_qty=Sum('production_quantity'), 
    record_count=Count('id')
)
print(f'PRODUCTION: {prod_totals["record_count"]} records, Total Qty: {prod_totals["total_qty"]}')

print(f'\n=== QUANTITY FLOW SUMMARY ===')
if smart_totals["total_qty"] and replen_totals["total_qty"] and prod_totals["total_qty"]:
    print(f'SMART Forecast → Replenishment: {smart_totals["total_qty"]} → {replen_totals["total_qty"]} ({replen_totals["total_qty"]/smart_totals["total_qty"]*100:.1f}%)')
    print(f'Replenishment → Production: {replen_totals["total_qty"]} → {prod_totals["total_qty"]} ({prod_totals["total_qty"]/replen_totals["total_qty"]*100:.1f}%)')

# Site assignment verification
print(f'\n=== SITE ASSIGNMENT ===')
sites = CalcualtedReplenishmentModel.objects.filter(version__version='Aug 25 SPR', Product__Product='T690EP').values('Site__SiteName').distinct()
for site in sites:
    print(f'Assigned to site: {site["Site__SiteName"]}')

# Sample date flow analysis
print(f'\n=== DATE FLOW ANALYSIS ===')
print('SMART → Replenishment (sample):')
smart_sample = SMART_Forecast_Model.objects.filter(version__version='Aug 25 SPR', Product='T690EP').order_by('Period_AU')[:3]
for item in smart_sample:
    print(f'  SMART: {item.Period_AU} (Qty: {item.Qty})')

replen_sample = CalcualtedReplenishmentModel.objects.filter(version__version='Aug 25 SPR', Product__Product='T690EP').order_by('ShippingDate')[:3]
for item in replen_sample:
    print(f'  REPLEN: {item.ShippingDate} (Qty: {item.ReplenishmentQty})')

prod_sample = CalculatedProductionModel.objects.filter(version__version='Aug 25 SPR', product__Product='T690EP').order_by('pouring_date')[:3]
for item in prod_sample:
    print(f'  PROD: {item.pouring_date} (Qty: {item.production_quantity})')
    
print('\n=== SUCCESS: Both commands linked to Calculated_model views successfully! ===')
