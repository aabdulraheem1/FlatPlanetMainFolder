from django.core.management.base import BaseCommand
from website.models import (
    SMART_Forecast_Model, 
    CalcualtedReplenishmentModel, 
    CalculatedProductionModel
)


class Command(BaseCommand):
    help = 'Trace T690EP through the complete flow: SMART forecast → replenishment → production'

    def handle(self, *args, **options):
        scenario_name = "Aug 25 SPR"
        product = "T690EP"
        
        self.stdout.write(f"\n🔍 TRACING PRODUCT: {product}")
        self.stdout.write(f"📂 Scenario: {scenario_name}")
        self.stdout.write("="*80)

        # Step 1: SMART Forecast Data
        self.stdout.write(f"\n1️⃣ SMART FORECAST DATA:")
        forecast_records = SMART_Forecast_Model.objects.filter(
            version__version=scenario_name,
            Product=product
        ).order_by('Period_AU', 'Location')
        
        total_forecast_qty = 0
        period_totals = {}
        
        for forecast in forecast_records:
            period = forecast.Period_AU
            qty = float(forecast.Qty)
            total_forecast_qty += qty
            
            if period not in period_totals:
                period_totals[period] = 0
            period_totals[period] += qty
            
            self.stdout.write(f"   📅 {period} | {forecast.Location} | Qty: {qty}")
        
        self.stdout.write(f"\n   📊 SMART FORECAST SUMMARY:")
        for period, qty in sorted(period_totals.items()):
            self.stdout.write(f"   📅 {period}: Total {qty}")
        self.stdout.write(f"   🔢 TOTAL SMART FORECAST QTY: {total_forecast_qty}")

        # Step 2: Replenishment Data
        self.stdout.write(f"\n2️⃣ REPLENISHMENT DATA (After freight calculation):")
        replenishment_records = CalcualtedReplenishmentModel.objects.filter(
            version__version=scenario_name,
            Product__Product=product
        ).order_by('ShippingDate', 'Site__SiteName')
        
        total_replenishment_qty = 0
        shipping_totals = {}
        
        for rep in replenishment_records:
            shipping_date = rep.ShippingDate
            site = rep.Site.SiteName
            qty = float(rep.ReplenishmentQty)
            total_replenishment_qty += qty
            
            if shipping_date not in shipping_totals:
                shipping_totals[shipping_date] = 0
            shipping_totals[shipping_date] += qty
            
            self.stdout.write(f"   🚢 {shipping_date} | {site} | Qty: {qty}")
        
        self.stdout.write(f"\n   📊 REPLENISHMENT SUMMARY:")
        for shipping_date, qty in sorted(shipping_totals.items()):
            self.stdout.write(f"   🚢 {shipping_date}: Total {qty}")
        self.stdout.write(f"   🔢 TOTAL REPLENISHMENT QTY: {total_replenishment_qty}")

        # Step 3: Production Data
        self.stdout.write(f"\n3️⃣ PRODUCTION DATA (After cast-to-dispatch calculation):")
        production_records = CalculatedProductionModel.objects.filter(
            version__version=scenario_name,
            product__Product=product
        ).order_by('pouring_date', 'site__SiteName')
        
        total_production_qty = 0
        pouring_totals = {}
        
        for prod in production_records:
            pouring_date = prod.pouring_date
            site = prod.site.SiteName
            qty = float(prod.production_quantity)
            total_production_qty += qty
            
            if pouring_date not in pouring_totals:
                pouring_totals[pouring_date] = 0
            pouring_totals[pouring_date] += qty
            
            self.stdout.write(f"   🏭 {pouring_date} | {site} | Qty: {qty}")
        
        self.stdout.write(f"\n   📊 PRODUCTION SUMMARY:")
        for pouring_date, qty in sorted(pouring_totals.items()):
            self.stdout.write(f"   🏭 {pouring_date}: Total {qty}")
        self.stdout.write(f"   🔢 TOTAL PRODUCTION QTY: {total_production_qty}")

        # Step 4: Summary Comparison
        self.stdout.write(f"\n📊 COMPLETE FLOW SUMMARY FOR {product}:")
        self.stdout.write("="*60)
        self.stdout.write(f"   📅 SMART Forecast Total:    {total_forecast_qty:,.1f}")
        self.stdout.write(f"   🚢 Replenishment Total:     {total_replenishment_qty:,.1f}")
        self.stdout.write(f"   🏭 Production Total:        {total_production_qty:,.1f}")
        self.stdout.write("="*60)
        
        # Calculate differences
        forecast_to_rep_diff = total_replenishment_qty - total_forecast_qty
        rep_to_prod_diff = total_production_qty - total_replenishment_qty
        forecast_to_prod_diff = total_production_qty - total_forecast_qty
        
        self.stdout.write(f"\n📈 QUANTITY CHANGES:")
        self.stdout.write(f"   📅→🚢 Forecast to Replenishment: {forecast_to_rep_diff:+.1f}")
        self.stdout.write(f"   🚢→🏭 Replenishment to Production: {rep_to_prod_diff:+.1f}")
        self.stdout.write(f"   📅→🏭 Forecast to Production: {forecast_to_prod_diff:+.1f}")
        
        # Explain differences
        self.stdout.write(f"\n💡 EXPLANATION OF DIFFERENCES:")
        if forecast_to_rep_diff != 0:
            self.stdout.write(f"   • Forecast→Replenishment difference: Site assignment filtering or inventory consumption")
        if rep_to_prod_diff > 0:
            self.stdout.write(f"   • Replenishment→Production increase: Safety stock top-ups")
        elif rep_to_prod_diff < 0:
            self.stdout.write(f"   • Replenishment→Production decrease: Inventory consumption at production sites")
            
        self.stdout.write(f"\n✅ Complete T690EP trace finished!")
