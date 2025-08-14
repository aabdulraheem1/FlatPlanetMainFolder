#!/usr/bin/env python
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import scenarios, CalculatedProductionModel, AggregatedForecast

def verify_cost_calculation():
    # Get scenario
    scenario = scenarios.objects.get(version='Aug 25 SP')
    print('=== VERIFICATION: Cost Calculation for PPS9BHPA-3 ===')
    
    # Check created production records
    records = CalculatedProductionModel.objects.filter(
        version=scenario, 
        product_id='PPS9BHPA-3'
    )
    
    print(f'✅ Total records created: {records.count()}')
    
    # Check sample with production quantity > 0
    sample = records.filter(production_quantity__gt=0).first()
    if sample:
        cost_per_unit = sample.production_aud / sample.production_quantity
        print(f'📊 Sample record with production:')
        print(f'   Date: {sample.pouring_date}')
        print(f'   Production Qty: {sample.production_quantity}')
        print(f'   Production AUD: {sample.production_aud:.2f}')
        print(f'   Cost per unit: {cost_per_unit:.4f}')
        print(f'   Tonnes: {sample.tonnes:.3f}')
    else:
        print('ℹ️  No records with production_quantity > 0 found')
    
    # Verify against AggregatedForecast source data
    agg_records = AggregatedForecast.objects.filter(
        version=scenario,
        product_id='PPS9BHPA-3'
    )
    
    if agg_records.exists():
        total_cogs = sum(record.cogs_aud or 0 for record in agg_records)
        total_qty = sum(record.qty or 0 for record in agg_records)
        
        if total_qty > 0:
            calculated_cost_per_unit = total_cogs / total_qty
            print(f'\n📈 AggregatedForecast verification:')
            print(f'   Total COGS: {total_cogs:.2f}')
            print(f'   Total Qty: {total_qty}')
            print(f'   Calculated cost per unit: {calculated_cost_per_unit:.4f}')
        else:
            print('\n⚠️  AggregatedForecast has no valid quantity data')
    else:
        print('\n❌ No AggregatedForecast data found for PPS9BHPA-3')
    
    print('\n✅ Cost calculation verification complete!')

if __name__ == "__main__":
    verify_cost_calculation()
