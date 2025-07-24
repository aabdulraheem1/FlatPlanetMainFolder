#!/usr/bin/env python
import os
import sys
import django

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import CalcualtedReplenishmentModel, scenarios

# Get the scenario
try:
    scenario = scenarios.objects.get(version='Jul 25 SPR Inv')
    
    # Check DEWB135-1 specifically
    dewb_records = CalcualtedReplenishmentModel.objects.filter(
        version=scenario,
        Product__Product='DEWB135-1'
    )
    
    total_dewb_qty = sum(record.ReplenishmentQty for record in dewb_records)
    print(f'DEWB135-1 Analysis:')
    print(f'  Total replenishment records: {dewb_records.count()}')
    print(f'  Total replenishment quantity: {total_dewb_qty:,.1f} units')
    print()
    
    # Show breakdown by location and site
    print('DEWB135-1 Breakdown by Location and Site:')
    for record in dewb_records:
        print(f'  {record.Location} | {record.Site.SiteName} | {record.ShippingDate.strftime("%Y-%m")} | {record.ReplenishmentQty:,.1f} units')
    
    print()
    
    # Overall replenishment summary
    all_records = CalcualtedReplenishmentModel.objects.filter(version=scenario)
    total_records = all_records.count()
    total_qty = sum(record.ReplenishmentQty for record in all_records)
    
    print(f'Overall Replenishment Summary:')
    print(f'  Total records in replenishment table: {total_records:,}')
    print(f'  Total replenishment quantity (all products): {total_qty:,.1f} units')
    
    # Top 10 products by replenishment quantity
    print()
    print('Top 10 Products by Replenishment Quantity:')
    from collections import defaultdict
    product_totals = defaultdict(float)
    
    for record in all_records:
        product_totals[record.Product.Product] += record.ReplenishmentQty
    
    # Sort by quantity descending and show top 10
    sorted_products = sorted(product_totals.items(), key=lambda x: x[1], reverse=True)
    for i, (product, qty) in enumerate(sorted_products[:10], 1):
        print(f'  {i:2d}. {product}: {qty:,.1f} units')

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
