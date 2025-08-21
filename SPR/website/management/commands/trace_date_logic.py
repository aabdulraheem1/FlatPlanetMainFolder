from django.core.management.base import BaseCommand
from website.models import *
from django.db.models import Count
from collections import defaultdict

class Command(BaseCommand):
    help = 'Trace date calculations for T690EP to verify freight and cast-to-dispatch logic'

    def handle(self, *args, **options):
        print('=== DATE CALCULATION TRACE FOR T690EP - Aug 25 SPR ===')
        
        # 1. SMART Forecast dates
        print('\n🔍 STEP 1: SMART FORECAST DATES')
        smart_dates = SMART_Forecast_Model.objects.filter(
            version__version='Aug 25 SPR', 
            Product='T690EP'
        ).values('Period_AU').annotate(
            record_count=Count('id')
        ).order_by('Period_AU')
        
        print('SMART Forecast periods:')
        for item in smart_dates[:10]:  # First 10 periods
            print(f'  Period_AU: {item["Period_AU"]} ({item["record_count"]} records)')
        
        # 2. Replenishment shipping dates  
        print('\n🔍 STEP 2: REPLENISHMENT SHIPPING DATES')
        replen_dates = CalcualtedReplenishmentModel.objects.filter(
            version__version='Aug 25 SPR',
            Product__Product='T690EP'
        ).values('ShippingDate').annotate(
            record_count=Count('id')
        ).order_by('ShippingDate')
        
        print('Replenishment shipping dates:')
        for item in replen_dates[:10]:
            print(f'  ShippingDate: {item["ShippingDate"]} ({item["record_count"]} records)')
            
        # 3. Production pouring dates
        print('\n🔍 STEP 3: PRODUCTION POURING DATES') 
        prod_dates = CalculatedProductionModel.objects.filter(
            version__version='Aug 25 SPR',
            product__Product='T690EP'
        ).values('pouring_date').annotate(
            record_count=Count('id')
        ).order_by('pouring_date')
        
        print('Production pouring dates:')
        for item in prod_dates[:10]:
            print(f'  PouringDate: {item["pouring_date"]} ({item["record_count"]} records)')
        
        # 4. Check freight logic implementation
        print('\n🔍 STEP 4: FREIGHT LOGIC VERIFICATION')
        
        # Sample a few SMART records to check freight calculation
        smart_sample = SMART_Forecast_Model.objects.filter(
            version__version='Aug 25 SPR', 
            Product='T690EP'
        ).order_by('Period_AU')[:3]
        
        for smart in smart_sample:
            print(f'\n📋 Sample: {smart.Customer_code} → {smart.Location}')
            print(f'   SMART Period_AU: {smart.Period_AU}')
            
            # Find corresponding replenishment
            replen = CalcualtedReplenishmentModel.objects.filter(
                version__version='Aug 25 SPR',
                Product__Product='T690EP'
            ).first()  # Just get first one for demo
            
            if replen:
                print(f'   Replenishment ShippingDate: {replen.ShippingDate}')
                print(f'   Site assigned: {replen.Site.SiteName}')
                
                # Check if freight days were calculated
                if smart.Period_AU == replen.ShippingDate:
                    print('   ⚠️  ShippingDate = Period_AU (freight_days = 0)')
                else:
                    days_diff = (smart.Period_AU - replen.ShippingDate).days
                    print(f'   📅 Freight days applied: {days_diff} days')
                
        # 5. Check cast-to-dispatch logic
        print('\n🔍 STEP 5: CAST-TO-DISPATCH LOGIC VERIFICATION')
        
        # Check cast-to-dispatch data for WOD1
        cast_dispatch = MasterDataCastToDespatchModel.objects.filter(
            version__version='Aug 25 SPR',
            Foundry__SiteName='WOD1'
        ).first()
        
        if cast_dispatch:
            print(f'Cast-to-dispatch for WOD1: {cast_dispatch.CastToDespatchDays} days')
            
            # Sample replenishment vs production dates
            replen_sample = CalcualtedReplenishmentModel.objects.filter(
                version__version='Aug 25 SPR',
                Product__Product='T690EP'
            ).first()
            
            prod_sample = CalculatedProductionModel.objects.filter(
                version__version='Aug 25 SPR',
                product__Product='T690EP'
            ).first()
            
            if replen_sample and prod_sample:
                print(f'   Replenishment ship: {replen_sample.ShippingDate}')
                print(f'   Production pour: {prod_sample.pouring_date}')
                days_diff = (replen_sample.ShippingDate - prod_sample.pouring_date).days
                print(f'   Calculated cast-to-dispatch: {days_diff} days')
                
                if days_diff == cast_dispatch.CastToDespatchDays:
                    print('   ✅ Cast-to-dispatch logic CORRECT')
                else:
                    print(f'   ❌ Cast-to-dispatch logic WRONG (expected {cast_dispatch.CastToDespatchDays})')
        else:
            print('   ❌ No cast-to-dispatch data found for WOD1')
            
        # 6. Check minimum date validation
        print('\n🔍 STEP 6: MINIMUM DATE VALIDATION')
        
        # Get inventory snapshot date
        inventory_snapshot = MasterDataInventory.objects.filter(
            version__version='Aug 25 SPR'
        ).first()
        
        if inventory_snapshot:
            snapshot_date = inventory_snapshot.date_of_snapshot
            print(f'Inventory snapshot date: {snapshot_date}')
            
            # Calculate expected minimum date (beginning of month after snapshot)
            from datetime import datetime, timedelta
            min_date = datetime(snapshot_date.year, snapshot_date.month, 1) + timedelta(days=32)
            min_date = min_date.replace(day=1).date()
            print(f'Expected minimum date: {min_date}')
            
            # Check if any dates are before minimum
            early_replen = CalcualtedReplenishmentModel.objects.filter(
                version__version='Aug 25 SPR',
                Product__Product='T690EP',
                ShippingDate__lt=min_date
            ).count()
            
            early_prod = CalculatedProductionModel.objects.filter(
                version__version='Aug 25 SPR', 
                product__Product='T690EP',
                pouring_date__lt=min_date
            ).count()
            
            print(f'Replenishment records before min date: {early_replen}')
            print(f'Production records before min date: {early_prod}')
            
            if early_replen == 0 and early_prod == 0:
                print('   ✅ Minimum date validation CORRECT')
            else:
                print('   ❌ Minimum date validation FAILED')
                
        print('\n' + '='*80)
