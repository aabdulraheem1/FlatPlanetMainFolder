#!/usr/bin/env python
"""
Analysis: Why Z14EP replenishment is 8.2x higher than forecast
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
from datetime import datetime, date

def analyze_replenishment_multiplication():
    """Analyze why replenishment is 8.2x higher than forecast for Z14EP"""
    
    scenario_version = 'Aug 25 SPR'
    product_code = 'Z14EP'
    
    print('=' * 80)
    print('ANALYZING REPLENISHMENT MULTIPLICATION')
    print(f'Z14EP: 2,921 forecast → 24,075 replenishment (8.2x increase)')
    print('=' * 80)
    
    try:
        scenario = scenarios.objects.get(version=scenario_version)
        
        print('\n🔍 REASON 1: Safety Stock Impact')
        print('-' * 50)
        
        # Check safety stock for Z14EP
        safety_stocks = MasterDataSafetyStockModel.objects.filter(
            version=scenario,
            Product__Product__icontains=product_code
        ).values('Product__Product', 'Safety_Stock_Level', 'Site__SiteName')
        
        total_safety_stock = 0
        print('Safety Stock Requirements:')
        for ss in safety_stocks:
            product = ss['Product__Product']
            safety_level = float(ss['Safety_Stock_Level'] or 0)
            site = ss['Site__SiteName']
            total_safety_stock += safety_level
            print(f'  {product} @ {site}: {safety_level:,.0f} units')
        
        print(f'\nTotal Safety Stock: {total_safety_stock:,.0f} units')
        
        if total_safety_stock > 0:
            safety_multiplier = total_safety_stock / 2921
            print(f'Safety Stock Multiplier: {safety_multiplier:.1f}x of forecast')
        
        print('\n🔍 REASON 2: Lead Time Coverage')
        print('-' * 50)
        
        # Check lead times
        lead_times = MasterDataLeadTimeModel.objects.filter(
            version=scenario,
            Product__Product__icontains=product_code
        ).values('Product__Product', 'LeadTime_Days', 'Site__SiteName')
        
        print('Lead Time Requirements:')
        for lt in lead_times:
            product = lt['Product__Product']
            lead_days = float(lt['LeadTime_Days'] or 0)
            site = lt['Site__SiteName']
            print(f'  {product} @ {site}: {lead_days:,.0f} days')
        
        print('\n🔍 REASON 3: Forecast Pattern Analysis')
        print('-' * 50)
        
        # Analyze forecast distribution over time
        forecasts = SMART_Forecast_Model.objects.filter(
            version=scenario,
            Product__icontains=product_code
        ).values('Product', 'Period_AU', 'Qty', 'Customer_code')
        
        monthly_demand = {}
        total_forecast = 0
        
        for f in forecasts:
            period = f['Period_AU']
            qty = float(f['Qty'] or 0)
            total_forecast += qty
            
            if period not in monthly_demand:
                monthly_demand[period] = 0
            monthly_demand[period] += qty
        
        print(f'Total Forecast Periods: {len(monthly_demand)}')
        print(f'Average Monthly Demand: {total_forecast / len(monthly_demand):,.0f} units')
        
        # Show peak demand months
        sorted_months = sorted(monthly_demand.items(), key=lambda x: x[1], reverse=True)
        print('\nTop 5 Demand Months:')
        for period, qty in sorted_months[:5]:
            print(f'  {period}: {qty:,.0f} units')
        
        print('\n🔍 REASON 4: Replenishment Logic Analysis')
        print('-' * 50)
        
        # Check the actual replenishment records to understand the multiplication
        replenishments = CalcualtedReplenishmentModel.objects.filter(
            version=scenario,
            Product__Product__icontains=product_code
        ).values('Product__Product', 'ShippingDate', 'ReplenishmentQty', 'Site__SiteName')
        
        monthly_replenishment = {}
        total_replenishment = 0
        
        for rep in replenishments:
            date_obj = rep['ShippingDate']
            month_key = f"{date_obj.year}-{date_obj.month:02d}"
            qty = float(rep['ReplenishmentQty'] or 0)
            total_replenishment += qty
            
            if month_key not in monthly_replenishment:
                monthly_replenishment[month_key] = 0
            monthly_replenishment[month_key] += qty
        
        print(f'Total Replenishment Periods: {len(monthly_replenishment)}')
        print(f'Average Monthly Replenishment: {total_replenishment / len(monthly_replenishment):,.0f} units')
        
        # Show top replenishment months
        sorted_rep_months = sorted(monthly_replenishment.items(), key=lambda x: x[1], reverse=True)
        print('\nTop 5 Replenishment Months:')
        for month, qty in sorted_rep_months[:5]:
            print(f'  {month}: {qty:,.0f} units')
        
        print('\n📊 MULTIPLICATION ANALYSIS')
        print('-' * 50)
        
        print(f'Base Forecast: {total_forecast:,.0f} units')
        print(f'Final Replenishment: {total_replenishment:,.0f} units')
        print(f'Multiplication Factor: {total_replenishment / total_forecast:.1f}x')
        
        # Calculate potential contributing factors
        print('\nPotential Contributing Factors:')
        
        # Safety stock contribution
        if total_safety_stock > 0:
            safety_contribution = (total_safety_stock / total_forecast) * 100
            print(f'  Safety Stock: {safety_contribution:.1f}% of forecast')
        
        # Time horizon expansion (24 months vs forecast period)
        time_expansion = len(monthly_replenishment) / len(monthly_demand)
        print(f'  Time Expansion: {time_expansion:.1f}x (replenishment covers more periods)')
        
        # Check if there are minimum order quantities or batching
        unique_rep_qtys = set(rep['ReplenishmentQty'] for rep in replenishments)
        print(f'  Unique Replenishment Quantities: {len(unique_rep_qtys)} different batch sizes')
        
        print('\n🎯 CONCLUSION')
        print('-' * 50)
        print('The 8.2x multiplication is likely due to:')
        print('1. Safety stock requirements for buffer inventory')
        print('2. Lead time coverage ensuring continuous supply')
        print('3. Extended planning horizon (24 months)')
        print('4. Minimum order quantities and batching logic')
        print('5. Multiple customer segments and locations')
        
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    analyze_replenishment_multiplication()
