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

        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            return

        CalculatedProductionModel.objects.filter(version=scenario).delete()

        # Prefetch product and site data as dicts for fast lookup
        product_data_map = {
            product.Product: product
            for product in MasterDataProductModel.objects.all()
        }
        site_data_map = {
            site.SiteName: site
            for site in MasterDataPlantModel.objects.all()
        }

        # Prefetch cast-to-despatch days as a dict
        # For cast_to_despatch_map
        cast_to_despatch_map = {
            (entry.Foundry.SiteName, entry.version.version): entry.CastToDespatchDays
            for entry in MasterDataCastToDespatchModel.objects.filter(version=scenario)
}

        # For wip_lookup
        wip_lookup = {
            (item['product'], item['site_id']): item['total_wip']
            for item in MasterDataInventory.objects.filter(version=scenario).values('product', 'site_id').annotate(total_wip=Sum('wip_stock_qty'))
        }
        # For onhand_lookup
        onhand_lookup = {
            (item['product'], item['site_id']): item['total_onhand']
            for item in MasterDataInventory.objects.filter(version=scenario).values('product', 'site_id').annotate(total_onhand=Sum('onhandstock_qty'))
        }

        # Use iterator for memory efficiency
        batch_size = 1000
        calculated_productions = []

        for replenishment in CalcualtedReplenishmentModel.objects.filter(version=scenario).values(
            'Product', 'Site', 'ShippingDate'
        ).annotate(
            total_qty=Sum('ReplenishmentQty')
        ).order_by('ShippingDate').iterator(chunk_size=batch_size):

            product = replenishment['Product']
            site = replenishment['Site']
            shipping_date = replenishment['ShippingDate']
            total_qty = replenishment['total_qty']

            cast_to_despatch_days = cast_to_despatch_map.get((site, scenario.version))
            if cast_to_despatch_days is None:
                cast_to_despatch_days = 0

            pouring_date = shipping_date - timedelta(days=cast_to_despatch_days)
            product_instance = product_data_map.get(product)
            site_instance = site_data_map.get(site)
            if not product_instance or not site_instance:
                continue

            # Remove from onhand stock first
            onhand_stock = onhand_lookup.get((product, site), 0)
            if onhand_stock > 0:
                if total_qty <= onhand_stock:
                    production_quantity = 0
                    onhand_stock -= total_qty
                    total_qty = 0
                else:
                    total_qty -= onhand_stock
                    onhand_stock = 0
            else:
                # No onhand stock, proceed to WIP
                pass

            # Remove from WIP stock next
            wip_stock = wip_lookup.get((product, site), 0)
            if total_qty > 0 and wip_stock > 0:
                if total_qty <= wip_stock:
                    production_quantity = 0
                    wip_stock -= total_qty
                    total_qty = 0
                else:
                    total_qty -= wip_stock
                    wip_stock = 0

            # If still remaining, that's the production quantity
            if total_qty > 0:
                production_quantity = total_qty
            else:
                production_quantity = 0

            dress_mass = product_instance.DressMass or 0
            tonnes = (production_quantity * dress_mass) / 1000

            calculated_productions.append(CalculatedProductionModel(
                version=scenario,
                product=product_instance,
                site=site_instance,
                pouring_date=pouring_date,
                production_quantity=production_quantity,
                tonnes=tonnes
            ))

            # Update the lookups
            wip_lookup[(product, site)] = wip_stock
            onhand_lookup[(product, site)] = onhand_stock

            if len(calculated_productions) >= batch_size:
                CalculatedProductionModel.objects.bulk_create(calculated_productions, batch_size=batch_size)
                calculated_productions = []

        if calculated_productions:
            CalculatedProductionModel.objects.bulk_create(calculated_productions, batch_size=batch_size)