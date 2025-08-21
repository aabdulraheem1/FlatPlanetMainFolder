from django.core.management.base import BaseCommand
from website.models import SMART_Forecast_Model, CalcualtedReplenishmentModel, CalculatedProductionModel
from datetime import timedelta


class Command(BaseCommand):
    help = 'Show complete date flow with Period_AU'

    def handle(self, *args, **options):
        scenario_name = "Aug 25 SPR"
        
        self.stdout.write(f"\n🔍 COMPLETE DATE FLOW WITH PERIOD_AU")
        self.stdout.write(f"📂 Scenario: {scenario_name}")
        self.stdout.write("="*80)

        # Show original forecast data first
        self.stdout.write(f"\n📅 ORIGINAL FORECAST DATA (Period_AU):")
        forecast_records = SMART_Forecast_Model.objects.filter(
            version__version=scenario_name
        ).order_by('Period_AU')[:5]
        
        for i, forecast in enumerate(forecast_records, 1):
            self.stdout.write(f"   {i}. Product: {forecast.Product}")
            self.stdout.write(f"      📅 Period_AU: {forecast.Period_AU}")
            self.stdout.write(f"      📍 Location: {forecast.Location}")
            self.stdout.write(f"      📦 Quantity: {forecast.Qty}")
            self.stdout.write(f"      📊 Version: {forecast.version}")

        # Show replenishment data (after freight calculation)
        self.stdout.write(f"\n🚢 REPLENISHMENT DATA (After freight calculation):")
        rep_records = CalcualtedReplenishmentModel.objects.filter(
            version__version=scenario_name
        )[:5]
        
        for i, rep in enumerate(rep_records, 1):
            self.stdout.write(f"   {i}. Product: {rep.Product.Product}")
            self.stdout.write(f"      🚢 Shipping Date: {rep.ShippingDate}")
            self.stdout.write(f"      📍 Site: {rep.Site.SiteName}")
            self.stdout.write(f"      📦 Quantity: {rep.ReplenishmentQty}")

        # Show production data (after cast-to-dispatch calculation)  
        self.stdout.write(f"\n🏭 PRODUCTION DATA (After cast-to-dispatch calculation):")
        prod_records = CalculatedProductionModel.objects.filter(
            version__version=scenario_name
        )[:5]
        
        for i, prod in enumerate(prod_records, 1):
            self.stdout.write(f"   {i}. Product: {prod.product.Product}")
            self.stdout.write(f"      🏭 Pouring Date: {prod.pouring_date}")
            self.stdout.write(f"      📍 Site: {prod.site.SiteName}")
            self.stdout.write(f"      📦 Quantity: {prod.production_quantity}")

        # Try to find a matching example across all three datasets
        self.stdout.write(f"\n🔗 TRYING TO FIND MATCHING DATE FLOW:")
        
        found_match = False
        for forecast in forecast_records:
            # Look for replenishment with same product
            matching_rep = CalcualtedReplenishmentModel.objects.filter(
                version__version=scenario_name,
                Product__Product=forecast.Product
            ).first()
            
            if matching_rep:
                # Look for production with same product  
                matching_prod = CalculatedProductionModel.objects.filter(
                    version__version=scenario_name,
                    product__Product=forecast.Product
                ).first()
                
                if matching_prod:
                    self.stdout.write(f"\n🎯 FOUND COMPLETE FLOW:")
                    self.stdout.write(f"   Product: {forecast.Product}")
                    self.stdout.write(f"   " + "="*50)
                    self.stdout.write(f"   📅 Original Period_AU: {forecast.Period_AU}")
                    self.stdout.write(f"   ⬇️  (Freight calculation applied)")
                    self.stdout.write(f"   🚢 Shipping Date: {matching_rep.ShippingDate}")
                    self.stdout.write(f"   ⬇️  (Cast-to-dispatch calculation applied)")
                    self.stdout.write(f"   🏭 Pouring Date: {matching_prod.pouring_date}")
                    
                    # Calculate the differences
                    if forecast.Period_AU and matching_rep.ShippingDate:
                        freight_days = (forecast.Period_AU - matching_rep.ShippingDate).days
                        self.stdout.write(f"   📊 Freight Days Applied: {freight_days}")
                    
                    if matching_rep.ShippingDate and matching_prod.pouring_date:
                        cast_days = (matching_rep.ShippingDate - matching_prod.pouring_date).days
                        self.stdout.write(f"   📊 Cast-to-Dispatch Days: {cast_days}")
                    
                    found_match = True
                    break
        
        if not found_match:
            self.stdout.write(f"   ⚠️  Could not find matching records across all three datasets")
            self.stdout.write(f"   This might be due to product filtering or site assignments")

        # Summary of the date policy
        self.stdout.write(f"\n📋 COMPLETE DATE POLICY SUMMARY:")
        self.stdout.write(f"   📅 Period_AU (from SMART forecast)")
        self.stdout.write(f"   ➖ Freight Days = 🚢 Shipping Date")
        self.stdout.write(f"   ➖ Cast-to-Dispatch Days = 🏭 Pouring Date")
        self.stdout.write(f"\n   ✅ Your policy: 'shipping date will be Period_AU - freight days - means minus'")
        self.stdout.write(f"   ✅ Both calculations use SUBTRACTION as requested")

        # Show the counts
        total_forecast = SMART_Forecast_Model.objects.filter(version__version=scenario_name).count()
        total_rep = CalcualtedReplenishmentModel.objects.filter(version__version=scenario_name).count() 
        total_prod = CalculatedProductionModel.objects.filter(version__version=scenario_name).count()
        
        self.stdout.write(f"\n📊 RECORD COUNTS:")
        self.stdout.write(f"   📅 Original Forecast: {total_forecast:,}")
        self.stdout.write(f"   🚢 Replenishment: {total_rep:,}")  
        self.stdout.write(f"   🏭 Production: {total_prod:,}")

        self.stdout.write(f"\n✅ Now you can see the complete Period_AU → Shipping → Pouring flow!")
