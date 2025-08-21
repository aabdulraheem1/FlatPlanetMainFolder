from django.core.management.base import BaseCommand
from website.models import *
from datetime import timedelta
import pandas as pd

class Command(BaseCommand):
    help = 'Trace date calculations for PPS9BHPA-3 product'

    def handle(self, *args, **options):
        print("=" * 80)
        print("🔍 DATE CALCULATION VERIFICATION FOR PPS9BHPA-3 - Aug 25 SPR")
        print("=" * 80)
        
        # Step 1: Get SMART forecast dates for PPS9BHPA-3
        print("\n🔍 STEP 1: SMART FORECAST DATES FOR PPS9BHPA-3")
        smart_records = SMART_Forecast_Model.objects.filter(
            version__scenario="Aug 25 SPR",
            Product="PPS9BHPA-3"
        ).order_by('Period_AU')
        
        if not smart_records.exists():
            print("❌ No PPS9BHPA-3 records found in SMART forecast")
            return
            
        print(f"Found {smart_records.count()} SMART forecast records")
        for record in smart_records[:5]:  # Show first 5
            print(f"  📅 Period_AU: {record.Period_AU}, Qty: {record.Qty}, Customer: {record.Customer_code}, Location: {record.Location}")
            
        # Step 2: Get Replenishment shipping dates for PPS9BHPA-3
        print("\n🔍 STEP 2: REPLENISHMENT SHIPPING DATES FOR PPS9BHPA-3")
        rep_records = CalcualtedReplenishmentModel.objects.filter(
            version__scenario="Aug 25 SPR",
            Product="PPS9BHPA-3"
        ).order_by('ShippingDate')
        
        if not rep_records.exists():
            print("❌ No PPS9BHPA-3 records found in replenishment")
            return
            
        print(f"Found {rep_records.count()} replenishment records")
        for record in rep_records[:5]:  # Show first 5
            print(f"  📅 ShippingDate: {record.ShippingDate}, Qty: {record.ReplenishmentQty}, Site: {record.Site}, Location: {record.Location}")
            
        # Step 3: Get Production pouring dates for PPS9BHPA-3
        print("\n🔍 STEP 3: PRODUCTION POURING DATES FOR PPS9BHPA-3")
        prod_records = CalculatedProductionModel.objects.filter(
            version__scenario="Aug 25 SPR",
            Product="PPS9BHPA-3"
        ).order_by('PouringDate')
        
        if prod_records.exists():
            print(f"Found {prod_records.count()} production records")
            for record in prod_records[:5]:  # Show first 5
                print(f"  📅 PouringDate: {record.PouringDate}, Qty: {record.ProductionQty}, Site: {record.Site}")
        else:
            print("❌ No PPS9BHPA-3 records found in production")
            
        # Step 4: Policy Verification
        print("\n🔍 STEP 4: DATE POLICY VERIFICATION")
        print("\n📋 YOUR POLICIES:")
        print("  1. Shipping Date = Period_AU - Freight Days")  
        print("  2. Pouring Date = Shipping Date - Cast-to-Dispatch Days")
        
        # Get one sample for detailed analysis
        sample_smart = smart_records.first()
        sample_rep = rep_records.first()
        
        if sample_smart and sample_rep:
            print(f"\n📋 SAMPLE CALCULATION VERIFICATION:")
            print(f"  📅 SMART Period_AU: {sample_smart.Period_AU}")
            print(f"  📅 Replenishment ShippingDate: {sample_rep.ShippingDate}")
            
            # Calculate the difference
            if sample_smart.Period_AU and sample_rep.ShippingDate:
                if isinstance(sample_smart.Period_AU, pd.Timestamp):
                    period_date = sample_smart.Period_AU.date()
                else:
                    period_date = sample_smart.Period_AU
                    
                if isinstance(sample_rep.ShippingDate, pd.Timestamp):
                    ship_date = sample_rep.ShippingDate.date()
                else:
                    ship_date = sample_rep.ShippingDate
                    
                date_diff = (period_date - ship_date).days
                
                print(f"  🧮 Calculation: {period_date} vs {ship_date}")
                print(f"  📊 Date difference: {date_diff} days")
                
                if date_diff > 0:
                    print(f"  ✅ CORRECT: Shipping date is {date_diff} days BEFORE forecast period (freight days applied)")
                elif date_diff < 0:
                    print(f"  ❌ ERROR: Shipping date is {abs(date_diff)} days AFTER forecast period (wrong direction)")
                else:
                    print(f"  ⚠️  WARNING: Shipping date equals forecast period (freight days = 0)")
                    
        # Step 5: Cast-to-Dispatch Verification
        if sample_rep and prod_records.exists():
            sample_prod = prod_records.first()
            print(f"\n📋 CAST-TO-DISPATCH VERIFICATION:")
            print(f"  📅 Replenishment ShippingDate: {sample_rep.ShippingDate}")
            print(f"  📅 Production PouringDate: {sample_prod.PouringDate}")
            
            if sample_rep.ShippingDate and sample_prod.PouringDate:
                if isinstance(sample_rep.ShippingDate, pd.Timestamp):
                    ship_date = sample_rep.ShippingDate.date()
                else:
                    ship_date = sample_rep.ShippingDate
                    
                if isinstance(sample_prod.PouringDate, pd.Timestamp):
                    pour_date = sample_prod.PouringDate.date()
                else:
                    pour_date = sample_prod.PouringDate
                    
                cast_diff = (ship_date - pour_date).days
                
                print(f"  🧮 Calculation: {ship_date} vs {pour_date}")
                print(f"  📊 Cast-to-dispatch: {cast_diff} days")
                
                # Get expected cast-to-dispatch for the site
                site = sample_prod.Site
                print(f"  🏭 Production Site: {site}")
                
                if cast_diff > 0:
                    print(f"  ✅ CORRECT: Pouring date is {cast_diff} days BEFORE shipping date")
                else:
                    print(f"  ❌ ERROR: Pouring date is not before shipping date")
        
        print("\n" + "=" * 80)
        print("🎯 POLICY SUMMARY:")
        print("   Shipping Date = Period_AU - Freight Days (SUBTRACT freight)")  
        print("   Pouring Date = Shipping Date - Cast-to-Dispatch Days (SUBTRACT lead time)")
        print("=" * 80)
