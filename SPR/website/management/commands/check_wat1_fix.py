from django.core.management.base import BaseCommand
from website.models import *
from django.db.models import Sum, Count

class Command(BaseCommand):
    help = 'Check corrected WAT1 replenishment calculation'

    def handle(self, *args, **options):
        print('=== CORRECTED WAT1 REPLENISHMENT ANALYSIS ===')

        # Check replenishment for WAT1 after fix
        replen_wat1 = CalcualtedReplenishmentModel.objects.filter(
            version__version='Aug 25 SPR',
            Product__Product='T690EP', 
            Location='WAT1'
        ).order_by('ShippingDate')

        print(f'WAT1 Replenishment records: {replen_wat1.count()}')
        total_replen = sum(r.ReplenishmentQty for r in replen_wat1)
        print(f'Total WAT1 replenishment: {total_replen} units')

        if replen_wat1.exists():
            print('\nFirst few WAT1 replenishment records:')
            for item in replen_wat1[:5]:
                print(f'  Ship Date: {item.ShippingDate}, Qty: {item.ReplenishmentQty}, Site: {item.Site.SiteName}')
        else:
            print('✅ No replenishment records for WAT1 (inventory fully covers demand!)')

        # Check what happened to total quantities
        totals = CalcualtedReplenishmentModel.objects.filter(
            version__version='Aug 25 SPR',
            Product__Product='T690EP'
        ).aggregate(total_qty=Sum('ReplenishmentQty'))

        print(f'\nTotal T690EP replenishment (all locations): {totals["total_qty"]} units')
        print('Previous result: 177,408 units')
        if totals["total_qty"]:
            improvement = 177408 - float(totals['total_qty'])
            print(f'Improvement: {improvement:,.0f} units reduction ({improvement/177408*100:.1f}% less)')
            
        # Show inventory consumption for WAT1
        print(f'\n=== INVENTORY ANALYSIS FOR WAT1 ===')
        inventory_wat1 = MasterDataInventory.objects.filter(
            version__version='Aug 25 SPR',
            product='T690EP',
            site__SiteName='WAT1'
        ).first()

        if inventory_wat1:
            total_stock = (inventory_wat1.onhandstock_qty or 0) + (inventory_wat1.intransitstock_qty or 0) + (inventory_wat1.wip_stock_qty or 0)
            print(f'Original WAT1 inventory: {total_stock} units')
            
            # SMART demand for WAT1 
            smart_wat1_total = SMART_Forecast_Model.objects.filter(
                version__version='Aug 25 SPR', 
                Product='T690EP',
                Location__icontains='WAT1'
            ).aggregate(total_demand=Sum('Qty'))['total_demand'] or 0
            
            print(f'Total SMART demand for WAT1: {smart_wat1_total} units')
            print(f'Expected remaining inventory: {total_stock - smart_wat1_total} units')
            
            # Safety stock
            safety_wat1 = MasterDataSafetyStocks.objects.filter(
                version__version='Aug 25 SPR',
                Plant='WAT1',
                PartNum='T690EP'
            ).first()
            
            if safety_wat1:
                total_safety = (safety_wat1.MinimumQty or 0) + (safety_wat1.SafetyQty or 0)
                print(f'Safety stock requirement: {total_safety} units')
                remaining = total_stock - smart_wat1_total
                if remaining >= total_safety:
                    print(f'✅ Remaining inventory ({remaining}) >= Safety stock ({total_safety}) - No replenishment needed!')
                else:
                    print(f'⚠️  Remaining inventory ({remaining}) < Safety stock ({total_safety}) - Top-up needed!')
