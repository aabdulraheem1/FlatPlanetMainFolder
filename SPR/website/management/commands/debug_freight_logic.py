from django.core.management.base import BaseCommand
from website.models import *
from datetime import timedelta
import pandas as pd

class Command(BaseCommand):
    help = 'Debug freight logic calculation'

    def handle(self, *args, **options):
        print("=== FREIGHT LOGIC DEBUG ===")
        
        # Get one T690EP sample
        sample = SMART_Forecast_Model.objects.filter(
            version__scenario="Aug 25 SPR", 
            Product="T690EP"
        ).first()
        
        if not sample:
            print("No T690EP sample found")
            return
            
        print(f"Sample SMART record:")
        print(f"  Product: {sample.Product}")
        print(f"  Customer: {sample.Customer_code}")
        print(f"  Location: {sample.Location}")
        print(f"  Period_AU: {sample.Period_AU}")
        print(f"  Forecast Region: {sample.Forecast_Region}")
        
        # Get the freight calculation
        def calculate_freight_days(customer_code, forecast_region, site, incoterm_data, freight_data):
            # Get incoterm category for customer
            incoterm_category = incoterm_data.get(customer_code, 'EXW')  # Default EXW
            
            # Get freight components
            freight_key = (forecast_region, site)
            freight_info = freight_data.get(freight_key, {})
            
            plant_to_port = freight_info.get('plant_to_port', 0)
            ocean_freight = freight_info.get('ocean_freight', 0) 
            port_to_customer = freight_info.get('port_to_customer', 0)
            
            # Calculate total based on incoterm category
            if incoterm_category == 'EXW':
                return plant_to_port + ocean_freight + port_to_customer
            elif incoterm_category == 'FOB':
                return ocean_freight + port_to_customer
            elif incoterm_category in ['CIF', 'DDP']:
                return port_to_customer
            else:
                return plant_to_port + ocean_freight + port_to_customer  # Default to EXW
        
        # Load master data
        from website.models import ScenarioVersionModel, IncoTermModel, MasterDataFreightModel
        version = ScenarioVersionModel.objects.get(scenario="Aug 25 SPR")
        
        # Incoterm data
        incoterm_data = dict(IncoTermModel.objects.filter(version=version)
                           .values_list('Customer_Code', 'IncoTermCategory'))
        
        # Freight data
        freight_data = list(MasterDataFreightModel.objects.filter(version=version)
                           .select_related('ForecastRegion', 'ManufacturingSite')
                           .values('ForecastRegion__Forecast_region', 'ManufacturingSite__SiteName',
                                  'PlantToDomesticPortDays', 'OceanFreightDays', 'PortToCustomerDays'))
        freight_map = {
            (item['ForecastRegion__Forecast_region'], item['ManufacturingSite__SiteName']): {
                'plant_to_port': item['PlantToDomesticPortDays'] or 0,
                'ocean_freight': item['OceanFreightDays'] or 0,
                'port_to_customer': item['PortToCustomerDays'] or 0
            }
            for item in freight_data
        }
        
        # Site assignment (assume WOD1)
        site = "WOD1"
        
        # Calculate freight days
        freight_days = calculate_freight_days(
            sample.Customer_code, 
            sample.Forecast_Region, 
            site,
            incoterm_data, 
            freight_map
        )
        
        print(f"\nFreight Calculation:")
        print(f"  Customer: {sample.Customer_code}")
        print(f"  Incoterm Category: {incoterm_data.get(sample.Customer_code, 'EXW')}")
        print(f"  Forecast Region: {sample.Forecast_Region}")
        print(f"  Site: {site}")
        
        freight_key = (sample.Forecast_Region, site)
        freight_info = freight_map.get(freight_key, {})
        print(f"  Plant to Port: {freight_info.get('plant_to_port', 0)} days")
        print(f"  Ocean Freight: {freight_info.get('ocean_freight', 0)} days") 
        print(f"  Port to Customer: {freight_info.get('port_to_customer', 0)} days")
        print(f"  Total Freight Days: {freight_days} days")
        
        # Calculate shipping date
        period_au = sample.Period_AU
        shipping_date = period_au - timedelta(days=freight_days)
        
        print(f"\nDate Calculation:")
        print(f"  SMART Period_AU: {period_au}")
        print(f"  Freight Days: {freight_days}")
        print(f"  Calculation: {period_au} - {freight_days} days = {shipping_date}")
        print(f"  Expected: Period_AU - freight_days")
        
        print("\n" + "="*50)
