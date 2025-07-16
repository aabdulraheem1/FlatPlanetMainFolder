from django.core.management.base import BaseCommand
from django.db.models import Sum
from website.models import (
    scenarios,
    SMART_Forecast_Model,
    MasterDataInventory,
    MasterDataProductModel,
    MasterDataOrderBook,
    MasterDataHistoryOfProductionModel,
    MasterDataPlantModel,
    CalcualtedReplenishmentModel,
    MasterDataEpicorSupplierMasterDataModel,
    MasterDataFreightModel,
    MasterdataIncoTermsModel,
    MasterDataManuallyAssignProductionRequirement,
)
import pandas as pd
from datetime import timedelta

class Command(BaseCommand):
    help = "Populate data in CalcualtedReplenishmentModel from SMART_Forecast_Model (fast pandas version)"

    def add_arguments(self, parser):
        parser.add_argument(
            'version',
            type=str,
            help="The version of the scenario to populate data for.",
        )

    def handle(self, *args, **kwargs):
        version = kwargs['version']

        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            return

        CalcualtedReplenishmentModel.objects.filter(version=scenario.version).delete()

        # Load all relevant data into pandas DataFrames
        forecast_df = pd.DataFrame(list(
            SMART_Forecast_Model.objects.filter(version=scenario.version)
            .values('Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code', 'Qty')
        ))
        print("T690EP in forecast_df:", 'T690EP' in forecast_df['Product'].values)
        print("forecast_df rows for T690EP:")
        print(forecast_df[forecast_df['Product'] == 'T690EP'])
        if forecast_df.empty:
            self.stdout.write(self.style.WARNING("No forecast data found."))
            return

        forecast_df['Location'] = forecast_df['Location'].apply(self._transform_location)

        # Aggregate total_qty per group (sum Qty)
        forecast_df = forecast_df.groupby(['Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code'], as_index=False)['Qty'].sum()
        forecast_df.rename(columns={'Qty': 'total_qty'}, inplace=True)

        # Inventory lookup
        inventory_df = pd.DataFrame(list(
            MasterDataInventory.objects.filter(version=scenario.version)
            .values('product', 'site_id', 'onhandstock_qty', 'intransitstock_qty', 'date_of_snapshot')
        ))
        # Aggregate inventory
        inventory_agg = inventory_df.groupby(['product', 'site_id'], as_index=False).agg({
            'onhandstock_qty': 'sum',
            'intransitstock_qty': 'sum',
            'date_of_snapshot': 'min'
        })

        # Prepare product and plant maps
        product_map = {p.Product: p for p in MasterDataProductModel.objects.all()}
        print("T690EP in product_map:", 'T690EP' in product_map)
        plant_map = {p.SiteName: p for p in MasterDataPlantModel.objects.all()}

        # Prepare order book, production, supplier maps
        order_book_map = {
            (ob.version.version, ob.productkey): ob.site
            for ob in MasterDataOrderBook.objects.filter(version=scenario).exclude(site__isnull=True).exclude(site__exact='')
        }
        production_map = {
            (prod.version.version, prod.Product): prod.Foundry
            for prod in MasterDataHistoryOfProductionModel.objects.filter(version=scenario).exclude(Foundry__isnull=True).exclude(Foundry__exact='')
        }
        supplier_map = {
            (sup.version.version, sup.PartNum): sup.VendorID
            for sup in MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario).exclude(VendorID__isnull=True).exclude(VendorID__exact='')
        }

        # Prepare freight and incoterm lookups
        freight_map = {
            (f.version.version, f.ForecastRegion): f for f in MasterDataFreightModel.objects.filter(version=scenario)
        }
        incoterm_map = {
            (i.version.version, i.CustomerCode): getattr(i.Incoterm, 'IncoTerm', None)
            for i in MasterdataIncoTermsModel.objects.filter(version=scenario)
        }

        # Prepare manual assignment lookup
        manual_assign_map = {
            (m.version.version, m.Product.Product, m.ShippingDate): m.Site
            for m in MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario)
        }

        replenishment_records = []

        # Define sites where onhand stock should not be deducted in replenishment
        # (because it will be deducted later in production)
        excluded_sites = {'MTJ1', 'COI2', 'XUZ1', 'MER1', 'WUN1', 'WOD1', 'CHI1'}

        for idx, row in forecast_df.iterrows():
            product = row['Product']
            location = row['Location']
            period = row['Period_AU']
            forecast_region = row['Forecast_Region']
            customer_code = row['Customer_code']
            total_qty = row['total_qty']

            # --- Shipping Date Calculation Logic ---
            freight = freight_map.get((scenario.version, forecast_region))
            incoterm = incoterm_map.get((scenario.version, customer_code))
            shipping_date = period
            if incoterm and freight:
                if incoterm in ['CPT', 'CIF']:
                    shipping_date = period - timedelta(days=(freight.PlantToDomesticPortDays or 0) + (freight.OceanFreightDays or 0))
                elif incoterm in ['DDP', 'DAP', 'CIP']:
                    shipping_date = period - timedelta(days=(freight.PlantToDomesticPortDays or 0) + (freight.OceanFreightDays or 0) + (freight.PortToCustomerDays or 0))
                elif incoterm in ['CFR', 'FSA', 'FOB', 'FCA']:
                    shipping_date = period - timedelta(days=(freight.PlantToDomesticPortDays or 0))
                elif incoterm == 'EXW':
                    shipping_date = period

            # --- Site Assignment Logic ---
            site = order_book_map.get((scenario.version, product))
            if not site:
                foundry = production_map.get((scenario.version, product))
                site = foundry
            if not site:
                vendor_id = supplier_map.get((scenario.version, product))
                site = vendor_id
            # Overwrite with manual assignment if available
            manual_site = manual_assign_map.get((scenario.version, product, shipping_date))
            if manual_site:
                site = manual_site.SiteName if hasattr(manual_site, 'SiteName') else manual_site

            site_obj = plant_map.get(site)
            
            if not site_obj:
                continue

            # Inventory snapshot logic
            inv_row = inventory_agg[(inventory_agg['product'] == product) & (inventory_agg['site_id'] == site_obj.SiteName)]
            if not inv_row.empty:
                date_of_snapshot = inv_row.iloc[0]['date_of_snapshot']
                if shipping_date < date_of_snapshot:
                    shipping_date = date_of_snapshot
                onhand_stock = inv_row.iloc[0]['onhandstock_qty'] or 0
                intransit_stock = inv_row.iloc[0]['intransitstock_qty'] or 0
            else:
                onhand_stock = 0
                intransit_stock = 0

            remaining_qty = total_qty
            
            while remaining_qty > 0:
                # Only deduct onhand stock if the site is not in excluded sites
                # (to avoid double deduction with production)
                if onhand_stock > 0 and site_obj.SiteName not in excluded_sites:
                    if remaining_qty <= onhand_stock:
                        onhand_stock -= remaining_qty
                        remaining_qty = 0
                    else:
                        remaining_qty -= onhand_stock
                        onhand_stock = 0
                
                if remaining_qty > 0 and intransit_stock > 0:
                    if remaining_qty <= intransit_stock:
                        intransit_stock -= remaining_qty
                        remaining_qty = 0
                    else:
                        remaining_qty -= intransit_stock
                        intransit_stock = 0
                
                if remaining_qty > 0:
                    product_instance = product_map.get(product)
                    if not product_instance:
                        break
                    
                    replenishment_records.append(CalcualtedReplenishmentModel(
                        version=scenario,
                        Product=product_instance,
                        Location=location,
                        Site=site_obj,
                        ShippingDate=shipping_date,
                        ReplenishmentQty=remaining_qty
                    ))
                    remaining_qty = 0

        if replenishment_records:
            CalcualtedReplenishmentModel.objects.bulk_create(replenishment_records, batch_size=1000)

    def _transform_location(self, location):
        if location:
            if "_" in location:
                return location.split("_", 1)[1][:4]
            elif "-" in location:
                return location.split("-", 1)[1][:4]
        return location