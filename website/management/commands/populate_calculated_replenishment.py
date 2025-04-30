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

        # Fetch the scenario instance
        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario with version '{version}' does not exist."))
            return

        # Delete existing records for the same version
        CalcualtedReplenishmentModel.objects.filter(version=scenario).delete()

        # Fetch SMART_Forecast_Model data grouped by Product, Location, and Period_AU
        forecast_data = SMART_Forecast_Model.objects.filter(version=scenario).values(
            'Product', 'Location', 'Period_AU'
        ).annotate(
            total_qty=Sum('Qty')  # Consolidate total quantity for each group
        ).order_by('Period_AU')

        # Fetch on-hand and in-transit stock data grouped by Product and Location
        inventory_data = MasterDataInventory.objects.filter(version=scenario).values(
            'product', 'site_id'
        ).annotate(
            total_onhand=Sum('onhandstock_qty'),
            total_intransit=Sum('intransitstock_qty')
        )

        # Create a dictionary for quick lookup of inventory data
        inventory_lookup = {
            (item['product'], self._transform_location(item['site_id'])): {
                'onhand': item['total_onhand'],
                'intransit': item['total_intransit']
            }
            for item in inventory_data
        }

        # Iterate through forecast data and populate CalcualtedReplenishmentModel
        for forecast in forecast_data:
            product = forecast['Product']
            location = self._transform_location(forecast['Location'])
            period = forecast['Period_AU']
            total_qty = forecast['total_qty']  # Consolidated total quantity

            # Determine the site
            site = self._get_site(scenario, product)

            # Log the forecast details
            self.stdout.write(self.style.SUCCESS(
                f"Processing forecast for Product '{product}', Location '{location}', Period '{period}': "
                f"Total Qty: {total_qty}, Site: {site}"
            ))

            # Initialize on-hand and in-transit stock
            inventory = inventory_lookup.get((product, location), {})
            onhand_stock = inventory.get('onhand', 0)
            intransit_stock = inventory.get('intransit', 0)

            # Deduct stock for the current period
            remaining_qty = total_qty if total_qty is not None else 0  # Ensure remaining_qty is initialized as an integer
            while remaining_qty > 0:
                # Deduct on-hand stock first
                if onhand_stock > 0:
                    if remaining_qty <= onhand_stock:
                        onhand_stock -= remaining_qty
                        remaining_qty = 0
                    else:
                        remaining_qty -= onhand_stock
                        onhand_stock = 0

                # Deduct in-transit stock next
                if remaining_qty > 0 and intransit_stock > 0:
                    if remaining_qty <= intransit_stock:
                        intransit_stock -= remaining_qty
                        remaining_qty = 0
                    else:
                        remaining_qty -= intransit_stock
                        intransit_stock = 0

                # If there's still remaining quantity, create a replenishment record
                if remaining_qty > 0:
                    try:
                        product_instance = MasterDataProductModel.objects.get(Product=product)
                    except MasterDataProductModel.DoesNotExist:
                        self.stdout.write(self.style.ERROR(f"Product '{product}' does not exist in MasterDataProductModel. Skipping."))
                        break  # Skip this product and move to the next forecast

                    CalcualtedReplenishmentModel.objects.create(
                        version=scenario,
                        Product=product_instance,
                        Location=location,
                        Site=site,
                        ShippingDate=period,
                        ReplenishmentQty=remaining_qty
                    )
                    remaining_qty = 0  # Reset remaining_qty after creating the record

            # Update the inventory lookup for the next period
            inventory_lookup[(product, location)] = {
                'onhand': onhand_stock,
                'intransit': intransit_stock
            }

        self.stdout.write(self.style.SUCCESS(f"Finished processing for version '{version}'."))

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

    def _get_site(self, scenario, product):
        """
        Determine the site for the given scenario and product.
        """
        # Check MasterDataOrderBook for the site
        order_book_entry = MasterDataOrderBook.objects.filter(version=scenario, productkey=product).first()
        if order_book_entry:
            # Retrieve the MasterDataPlantModel instance for the site
            site = MasterDataPlantModel.objects.filter(SiteName=order_book_entry.site).first()
            if site:
                return site

        # Fallback to MasterDataHistoryOfProductionModel for the foundry
        production_entry = MasterDataHistoryOfProductionModel.objects.filter(version=scenario, Product=product).first()
        if production_entry:
            # Retrieve the MasterDataPlantModel instance for the foundry
            foundry = MasterDataPlantModel.objects.filter(SiteName=production_entry.Foundry).first()
            if foundry:
                return foundry

        # If no site is found, return None
        return None