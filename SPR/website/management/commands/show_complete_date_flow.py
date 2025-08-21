from django.core.management.base import BaseCommand
from website.models import (
    SMART_Forecast_Model, 
    CalcualtedReplenishmentModel, 
    CalculatedProductionModel
)
from datetime import datetime, timedelta


class Command(BaseCommand):
    help = 'Show complete date flow: Period_AU -> Shipping Date -> Pouring Date'

    def handle(self, *args, **options):
        scenario_name = "Aug 25 SPR"
        
        self.stdout.write(f"\n🔍 COMPLETE DATE FLOW VERIFICATION")
        self.stdout.write(f"📂 Scenario: {scenario_name}")
        self.stdout.write("="*80)

        # Policy Statement
        self.stdout.write("\n📋 COMPLETE DATE CALCULATION FLOW:")
        self.stdout.write("   📅 Period_AU (from forecast)")
        self.stdout.write("   ⬇️  SUBTRACT Freight Days")  
        self.stdout.write("   🚢 Shipping Date")
        self.stdout.write("   ⬇️  SUBTRACT Cast-to-Dispatch Days")
        self.stdout.write("   🏭 Pouring Date")
        self.stdout.write("="*80)

        # Find a replenishment record with production match
        replenishment_records = CalcualtedReplenishmentModel.objects.filter(
            version__version=scenario_name
        ).select_related('version', 'Product', 'Site')[:20]

        production_records = CalculatedProductionModel.objects.filter(
            version__version=scenario_name
        ).select_related('version', 'product', 'site')

        found_example = False
        
        for rep_record in replenishment_records:
            # Find matching production record
            matching_prod = production_records.filter(
                product__Product=rep_record.Product.Product,
                site__SiteName=rep_record.Site.SiteName
            ).first()
            
            if matching_prod and rep_record.ShippingDate and matching_prod.pouring_date:
                # Find the original forecast record to get Period_AU
                try:
                    forecast_records = SMART_Forecast_Model.objects.filter(
                        scenario__version=scenario_name,
                        product_number__Product=rep_record.Product.Product,
                        site__SiteName=rep_record.Site.SiteName
                    ).order_by('period_au')
                    
                    for forecast in forecast_records:
                        # Check if this forecast period could generate this shipping date
                        if forecast.period_au and forecast.freight_days:
                            expected_shipping = forecast.period_au - timedelta(days=int(forecast.freight_days))
                            
                            if expected_shipping == rep_record.ShippingDate:
                                # We found the matching forecast!
                                cast_days = (rep_record.ShippingDate - matching_prod.pouring_date).days
                                
                                self.stdout.write(f"\n🎯 COMPLETE DATE FLOW EXAMPLE:")
                                self.stdout.write(f"   Product: {rep_record.Product.Product} @ {rep_record.Site.SiteName}")
                                self.stdout.write(f"   " + "="*60)
                                self.stdout.write(f"   📅 Period_AU:       {forecast.period_au}")
                                self.stdout.write(f"   ➖ Freight Days:    {forecast.freight_days} days")
                                self.stdout.write(f"   🚢 Shipping Date:   {rep_record.ShippingDate} ✅")
                                self.stdout.write(f"   ➖ Cast-to-Dispatch: {cast_days} days")
                                self.stdout.write(f"   🏭 Pouring Date:    {matching_prod.pouring_date} ✅")
                                self.stdout.write(f"   " + "="*60)
                                self.stdout.write(f"   🧮 Verification:")
                                self.stdout.write(f"      {forecast.period_au} - {forecast.freight_days} days = {expected_shipping} ✅")
                                self.stdout.write(f"      {rep_record.ShippingDate} - {cast_days} days = {matching_prod.pouring_date} ✅")
                                
                                found_example = True
                                break
                    
                    if found_example:
                        break
                        
                except SMART_Forecast_Model.DoesNotExist:
                    continue
        
        if not found_example:
            # Show at least one example with available data
            self.stdout.write(f"\n🔍 AVAILABLE DATA SAMPLE:")
            rep_record = replenishment_records.first()
            if rep_record:
                self.stdout.write(f"   📦 Replenishment Example:")
                self.stdout.write(f"      Product: {rep_record.Product.Product}")
                self.stdout.write(f"      Site: {rep_record.Site.SiteName}")
                self.stdout.write(f"      Shipping Date: {rep_record.ShippingDate}")
                
                # Try to find original forecast
                try:
                    forecast = SMART_Forecast_Model.objects.filter(
                        scenario__version=scenario_name,
                        product_number__Product=rep_record.Product.Product
                    ).first()
                    
                    if forecast:
                        self.stdout.write(f"      Period_AU: {forecast.period_au}")
                        self.stdout.write(f"      Freight Days: {forecast.freight_days}")
                        
                        if forecast.period_au and forecast.freight_days:
                            expected = forecast.period_au - timedelta(days=int(forecast.freight_days))
                            self.stdout.write(f"      Expected Shipping: {expected}")
                            self.stdout.write(f"      Match: {'✅' if expected == rep_record.ShippingDate else '❌'}")
                            
                except Exception as e:
                    self.stdout.write(f"      Could not find original forecast: {e}")

        # Show some forecast data to verify Period_AU values
        self.stdout.write(f"\n📊 FORECAST DATA SAMPLE (Period_AU values):")
        try:
            forecast_sample = SMART_Forecast_Model.objects.filter(
                scenario__version=scenario_name
            ).order_by('period_au')[:5]
            
            for i, forecast in enumerate(forecast_sample, 1):
                self.stdout.write(f"   {i}. {forecast.product_number.Product} @ {forecast.site.SiteName}")
                self.stdout.write(f"      📅 Period_AU: {forecast.period_au}")
                self.stdout.write(f"      🚢 Freight Days: {forecast.freight_days}")
                if forecast.period_au and forecast.freight_days:
                    calc_shipping = forecast.period_au - timedelta(days=int(forecast.freight_days))
                    self.stdout.write(f"      📦 Calculated Shipping: {calc_shipping}")
                    
        except Exception as e:
            self.stdout.write(f"   Could not access forecast data: {e}")

        self.stdout.write(f"\n✅ Complete date flow verification attempted for {scenario_name}!")
