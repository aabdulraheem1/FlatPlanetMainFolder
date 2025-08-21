#!/usr/bin/env python
"""
Production Requirement Analysis for Z14EP
Traces through: SMART_Forecast → Replenishment → Production
"""

import os
import sys
import django

# Add the SPR project to Python path
sys.path.append(r'C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR')

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import *
from datetime import datetime

def analyze_z14ep_production_requirement():
    """Analyze Z14EP production requirement through the complete flow"""
    
    scenario_version = 'Aug 25 SPR'
    product_code = 'Z14EP'
    
    print('=' * 80)
    print(f'PRODUCTION REQUIREMENT ANALYSIS FOR {product_code}')
    print(f'Scenario: {scenario_version}')
    print('=' * 80)
    
    try:
        scenario = scenarios.objects.get(version=scenario_version)
        print(f'✅ Scenario found: {scenario.version}\n')
        
        # STEP 1: SMART_Forecast Analysis
        print('📊 STEP 1: SMART_Forecast Analysis')
        print('-' * 50)
        
        smart_forecasts = SMART_Forecast_Model.objects.filter(
            version=scenario,
            Product__icontains=product_code
        ).values('Product', 'Period_AU', 'Qty', 'Customer_code', 'Forecast_Region', 'Location')
        
        print(f'Found {len(smart_forecasts)} SMART_Forecast records for {product_code}:')
        
        total_forecast_qty = 0
        forecast_periods = set()
        
        for forecast in smart_forecasts:
            product = forecast['Product']
            period = forecast['Period_AU']
            qty = float(forecast['Qty'] or 0)
            customer = forecast['Customer_code']
            region = forecast['Forecast_Region']
            location = forecast['Location']
            
            total_forecast_qty += qty
            forecast_periods.add(period)
            
            print(f'  📅 {period}: {qty:,.0f} units')
            print(f'     Product: {product}')
            print(f'     Customer: {customer}, Region: {region}, Location: {location}')
            print()
        
        print(f'📈 Total Forecast Demand: {total_forecast_qty:,.0f} units')
        print(f'📅 Forecast Periods: {len(forecast_periods)} periods\n')
        
        if len(smart_forecasts) == 0:
            print(f'⚠️  No forecast data found for {product_code} in {scenario_version}')
            return
        
        # STEP 2: Replenishment Analysis
        print('🔄 STEP 2: Replenishment Analysis')
        print('-' * 50)
        
        replenishment_records = CalcualtedReplenishmentModel.objects.filter(
            version=scenario,
            Product__Product__icontains=product_code
        ).values('Product__Product', 'ShippingDate', 'ReplenishmentQty', 'Site__SiteName', 'latest_customer_invoice')
        
        print(f'Found {len(replenishment_records)} Replenishment records for {product_code}:')
        
        total_replenishment_qty = 0
        replenishment_by_site = {}
        
        for rep in replenishment_records:
            product = rep['Product__Product']
            period = rep['ShippingDate']
            qty = float(rep['ReplenishmentQty'] or 0)
            site = rep['Site__SiteName']
            customer = rep['latest_customer_invoice']
            
            total_replenishment_qty += qty
            
            if site not in replenishment_by_site:
                replenishment_by_site[site] = 0
            replenishment_by_site[site] += qty
            
            print(f'  📅 {period}: {qty:,.0f} units → Site: {site}')
            print(f'     Product: {product}, Customer: {customer}')
            print()
        
        print(f'🔄 Total Replenishment: {total_replenishment_qty:,.0f} units')
        print('🏭 Replenishment by Site:')
        for site, qty in replenishment_by_site.items():
            print(f'   {site}: {qty:,.0f} units')
        print()
        
        # STEP 3: Production Analysis
        print('🏭 STEP 3: Production Analysis')
        print('-' * 50)
        
        production_records = CalculatedProductionModel.objects.filter(
            version=scenario,
            product__Product__icontains=product_code
        ).values('product__Product', 'pouring_date', 'production_quantity', 'site__SiteName')
        
        print(f'Found {len(production_records)} Production records for {product_code}:')
        
        total_production_qty = 0
        production_by_site = {}
        
        for prod in production_records:
            product = prod['product__Product']
            period = prod['pouring_date']
            qty = float(prod['production_quantity'] or 0)
            site = prod['site__SiteName']
            
            total_production_qty += qty
            
            if site not in production_by_site:
                production_by_site[site] = 0
            production_by_site[site] += qty
            
            print(f'  📅 {period}: {qty:,.0f} units @ Site: {site}')
            print(f'     Product: {product}')
            print()
        
        print(f'🏭 Total Production: {total_production_qty:,.0f} units')
        print('🏭 Production by Site:')
        for site, qty in production_by_site.items():
            print(f'   {site}: {qty:,.0f} units')
        print()
        
        # STEP 4: Summary Analysis
        print('📋 STEP 4: Flow Summary')
        print('-' * 50)
        
        print(f'📊 Forecast → Replenishment → Production Flow:')
        print(f'   SMART_Forecast:  {total_forecast_qty:,.0f} units')
        print(f'   Replenishment:   {total_replenishment_qty:,.0f} units')
        print(f'   Production:      {total_production_qty:,.0f} units')
        print()
        
        # Check for discrepancies
        if total_forecast_qty != total_replenishment_qty:
            diff = total_replenishment_qty - total_forecast_qty
            print(f'⚠️  Forecast vs Replenishment Difference: {diff:,.0f} units')
        
        if total_replenishment_qty != total_production_qty:
            diff = total_production_qty - total_replenishment_qty
            print(f'⚠️  Replenishment vs Production Difference: {diff:,.0f} units')
        
        if total_forecast_qty == total_replenishment_qty == total_production_qty:
            print('✅ Perfect flow alignment - all quantities match!')
        
        print('\n🎯 PRODUCTION REQUIREMENT CONCLUSION:')
        print(f'   Product {product_code} requires {total_production_qty:,.0f} units of production')
        print(f'   Distributed across {len(production_by_site)} production sites')
        for site, qty in production_by_site.items():
            percentage = (qty / total_production_qty * 100) if total_production_qty > 0 else 0
            print(f'   - {site}: {qty:,.0f} units ({percentage:.1f}%)')
        
    except Exception as e:
        print(f'❌ Error in analysis: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    analyze_z14ep_production_requirement()
