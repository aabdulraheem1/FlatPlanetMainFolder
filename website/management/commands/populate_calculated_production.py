from datetime import timedelta
from django.core.management.base import BaseCommand
from django.db.models import Sum
from website.models import (
    scenarios,
    CalcualtedReplenishmentModel,
    MasterDataCastToDespatchModel,
    MasterDataProductModel,
    MasterDataInventory,
    CalculatedProductionModel,
    MasterDataPlantModel
)


class Command(BaseCommand):
    help = "Populate data in CalculatedProductionModel from CalcualtedReplenishmentModel"

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
        CalculatedProductionModel.objects.filter(version=scenario).delete()

        # Fetch replenishment data grouped by product, site, and shipping date
        replenishment_data = CalcualtedReplenishmentModel.objects.filter(version=scenario).values(
            'Product', 'Site', 'ShippingDate'
        ).annotate(
            total_qty=Sum('ReplenishmentQty')
        ).order_by('ShippingDate')

        # Fetch WIP data grouped by product and site
        wip_data = MasterDataInventory.objects.filter(version=scenario).values(
            'product', 'site_id'
        ).annotate(
            total_wip=Sum('wip_stock_qty')
        )

        # Create a dictionary for quick lookup of WIP data
        wip_lookup = {
            (item['product'], item['site_id']): item['total_wip']
            for item in wip_data
        }

        # Iterate through replenishment data and calculate production
        for replenishment in replenishment_data:
            product = replenishment['Product']
            site = replenishment['Site']
            shipping_date = replenishment['ShippingDate']
            total_qty = replenishment['total_qty']

            # Fetch the cast to despatch days for the site
            try:
                cast_to_despatch = MasterDataCastToDespatchModel.objects.get(version=scenario, Foundry=site)
                cast_to_despatch_days = cast_to_despatch.CastToDespatchDays
            except MasterDataCastToDespatchModel.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Cast to Despatch Days not found for site '{site}'. Skipping."))
                continue

            # Calculate the pouring date
            pouring_date = shipping_date - timedelta(days=cast_to_despatch_days)

            # Fetch the dress mass for the product
            try:
                product_instance = MasterDataProductModel.objects.get(Product=product)
                dress_mass = product_instance.DressMass or 0
            except MasterDataProductModel.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Product '{product}' not found in MasterDataProductModel. Skipping."))
                continue

            # Initialize WIP for the product and site
            wip_stock = wip_lookup.get((product, site), 0)

            # Deduct WIP stock and calculate production quantity
            production_quantity = 0
            if wip_stock > 0:
                if total_qty <= wip_stock:
                    production_quantity = 0
                    wip_stock -= total_qty
                else:
                    production_quantity = total_qty - wip_stock
                    wip_stock = 0
            else:
                production_quantity = total_qty

            # Calculate tonnes
            tonnes = (production_quantity * dress_mass) / 1000

            # Fetch the MasterDataPlantModel instance for the site
            try:
                site_instance = MasterDataPlantModel.objects.get(SiteName=site)
            except MasterDataPlantModel.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Site '{site}' not found in MasterDataPlantModel. Skipping."))
                continue

            # Save the calculated production data
            CalculatedProductionModel.objects.create(
                version=scenario,
                product=product_instance,
                site=site_instance,  # Use the MasterDataPlantModel instance
                pouring_date=pouring_date,
                production_quantity=production_quantity,
                tonnes=tonnes
            )

            # Update the WIP lookup
            wip_lookup[(product, site)] = wip_stock

        self.stdout.write(self.style.SUCCESS(f"Finished processing for version '{version}'."))