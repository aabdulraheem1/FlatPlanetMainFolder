from django.core.management.base import BaseCommand
from django.db.models import Sum
from datetime import date, timedelta
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
)

class Command(BaseCommand):
    help = "Populate data in CalcualtedReplenishmentModel from SMART_Forecast_Model"

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

        # Prefetch and cache all related data
        product_data_map = {p.Product: p for p in MasterDataProductModel.objects.all()}
        plant_data_map = {p.SiteName: p for p in MasterDataPlantModel.objects.all()}

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

        # Inventory lookup as before
        inventory_data = MasterDataInventory.objects.filter(version=scenario.version).values(
            'product', 'site_id'
        ).annotate(
            total_onhand=Sum('onhandstock_qty'),
            total_intransit=Sum('intransitstock_qty')
        )
        inventory_lookup = {
            (item['product'], self._transform_location(item['site_id'])): {
                'onhand': item['total_onhand'],
                'intransit': item['total_intransit']
            }
            for item in inventory_data
        }

        forecast_data = SMART_Forecast_Model.objects.filter(version=scenario.version).values(
            'Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code'
        ).annotate(
            total_qty=Sum('Qty')
        ).order_by('Period_AU')

        replenishment_records = []

        for forecast in forecast_data:
            product = forecast['Product']
            location = self._transform_location(forecast['Location'])
            period = forecast['Period_AU']
            total_qty = forecast['total_qty'] or 0

            # --- Shipping Date Calculation Logic ---
            forecast_region = forecast.get('Forecast_Region')
            customer_code = forecast.get('Customer_code')

            freight = None
            if forecast_region:
                # site is not available yet, so skip site filter here
                freight = MasterDataFreightModel.objects.filter(
                    version=scenario,
                    ForecastRegion=forecast_region
                ).first()

            incoterm = None
            if customer_code:
                incoterm_obj = MasterdataIncoTermsModel.objects.filter(
                    version=scenario,
                    CustomerCode=customer_code
                ).first()
                if incoterm_obj and incoterm_obj.Incoterm:
                    incoterm = getattr(incoterm_obj.Incoterm, 'IncoTerm', None)

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
                else:
                    shipping_date = period
            # --- End Shipping Date Calculation Logic ---
           

            site = self._get_site_cached(
                scenario.version, product, plant_data_map,
                order_book_map, production_map, supplier_map,
                shipping_date=shipping_date
            )
            if not site:
                print(f"SKIP: No site for product={product}, version={scenario.version}")
                continue

              # Ensure shipping_date is not before inventory snapshot date
            inventory_snapshot = MasterDataInventory.objects.filter(
                version=scenario.version,
                product=product,
                site=site
            ).order_by('date_of_snapshot').first()
            
            if inventory_snapshot and shipping_date < inventory_snapshot.date_of_snapshot:
                shipping_date = inventory_snapshot.date_of_snapshot

            inventory = inventory_lookup.get((product, location), {})
            onhand_stock = inventory.get('onhand', 0)
            intransit_stock = inventory.get('intransit', 0)

            remaining_qty = total_qty
            while remaining_qty > 0:
                if onhand_stock > 0:
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
                    product_instance = product_data_map.get(product)
                    if not product_instance:
                        break
                    replenishment_records.append(CalcualtedReplenishmentModel(
                        version=scenario,
                        Product=product_instance,
                        Location=location,
                        Site=site,
                        ShippingDate=shipping_date,
                        ReplenishmentQty=remaining_qty
                    ))
                    remaining_qty = 0

            inventory_lookup[(product, location)] = {
                'onhand': onhand_stock,
                'intransit': intransit_stock
            }

        if replenishment_records:
            CalcualtedReplenishmentModel.objects.bulk_create(replenishment_records, batch_size=1000)

    def _transform_location(self, location):
        """
        Extract the 4 characters after "_" or "-" in the location if they exist.
        """
        if location:
            if "_" in location:
                return location.split("_", 1)[1][:4]
            elif "-" in location:
                return location.split("-", 1)[1][:4]
        return location

    def _get_site_cached(
        self, version, product, plant_data_map, order_book_map, production_map, supplier_map, shipping_date=None
    ):
        # OrderBook
        site_name = order_book_map.get((version, product))
        site = plant_data_map.get(site_name) if site_name else None

        # Production
        if not site:
            foundry = production_map.get((version, product))
            site = plant_data_map.get(foundry) if foundry else None

        # Supplier
        if not site:
            vendor_id = supplier_map.get((version, product))
            if vendor_id:
                vendor_to_site_mapping = {
                    "MTJ1", "COI2", "XUZ1", "MER1", "WOD1", "CHI1"
                }
                excluded_vendor_ids = {
                    "000000", "BKAU03", "BKCA01", "BKCL01", "BKCN01", "BKCN02",
                    "BKID02", "BKIN01", "BKMY01", "BKPE01", "BKUS01", "BKZA01"
                }
                if vendor_id in vendor_to_site_mapping:
                    site = plant_data_map.get(vendor_id)
                elif vendor_id not in excluded_vendor_ids:
                    site = plant_data_map.get(vendor_id)

        # Overwrite with manually assigned site if available
        if shipping_date:
            from website.models import MasterDataManuallyAssignProductionRequirement, MasterDataProductModel, MasterDataPlantModel, scenarios
            try:
                manual = MasterDataManuallyAssignProductionRequirement.objects.filter(
                    version__version=version,
                    Product__Product=product,
                    ShippingDate=shipping_date
                ).select_related('Site').first()
                if manual and manual.Site:
                    site = manual.Site
            except Exception:
                pass

        return site