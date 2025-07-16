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
    MasterDataPlantModel, ProductSiteCostModel
)
import pandas as pd

class Command(BaseCommand):
    help = "Populate data in CalculatedProductionModel from CalcualtedReplenishmentModel (fast pandas version)"

    def add_arguments(self, parser):
        parser.add_argument(
            'scenario_version',
            type=str,
            help="The version of the scenario to populate data for.",
        )

    def handle(self, *args, **kwargs):
        version = kwargs['scenario_version']

        if not version:
            self.stdout.write(self.style.ERROR("No version argument provided."))
            return

        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            return

        CalculatedProductionModel.objects.filter(version=scenario).delete()

        # Get the first inventory snapshot date and calculate the threshold date
        first_inventory = MasterDataInventory.objects.filter(version=scenario).order_by('date_of_snapshot').first()
        if first_inventory:
            # Add one day to the first snapshot date
            inventory_threshold_date = first_inventory.date_of_snapshot + timedelta(days=1)
            # Set to first day of that month
            inventory_start_date = inventory_threshold_date.replace(day=1)
        else:
            inventory_threshold_date = None
            inventory_start_date = None

        # Load replenishments in bulk
        replenishments = pd.DataFrame(list(
            CalcualtedReplenishmentModel.objects.filter(version=scenario)
            .values('Product', 'Site', 'ShippingDate', 'ReplenishmentQty')
        ))
        if replenishments.empty:
            self.stdout.write(self.style.WARNING("No replenishment records found for this version."))
            return
        
        inventory_df = pd.DataFrame(list(
            MasterDataInventory.objects.filter(version=scenario)
            .values('version_id', 'product', 'site_id', 'cost_aud')
        ))

        # Aggregate replenishments
        replenishments = replenishments.groupby(['Product', 'Site', 'ShippingDate'], as_index=False)['ReplenishmentQty'].sum()
        replenishments.rename(columns={'ReplenishmentQty': 'total_qty'}, inplace=True)

        # Load product and site data
        product_df = pd.DataFrame(list(MasterDataProductModel.objects.all().values('Product', 'DressMass', 'ProductGroup', 'ParentProductGroupDescription')))
        site_df = pd.DataFrame(list(MasterDataPlantModel.objects.all().values('SiteName')))

        # Load cast to despatch days
        cast_to_despatch = {
            (entry.Foundry.SiteName, entry.version.version): entry.CastToDespatchDays
            for entry in MasterDataCastToDespatchModel.objects.filter(version=scenario)
        }

        # Load inventory snapshots
        wip_df = pd.DataFrame(list(
            MasterDataInventory.objects.filter(version=scenario)
            .values('product', 'site_id')
            .annotate(total_wip=Sum('wip_stock_qty'))
        ))
        onhand_df = pd.DataFrame(list(
            MasterDataInventory.objects.filter(version=scenario)
            .values('product', 'site_id')
            .annotate(total_onhand=Sum('onhandstock_qty'))
        ))

        # Load ProductSiteCostModel for cost lookups
        cost_df = pd.DataFrame(list(
            ProductSiteCostModel.objects.filter(version=scenario)
            .values('version_id', 'product_id', 'site_id', 'cost_aud', 'revenue_cost_aud')
        ))

        # Build all three lookup dicts
        cost_lookup = {
            str(row['product_id']): row['cost_aud']
            for _, row in cost_df.iterrows()
            if pd.notnull(row['cost_aud'])
        }
        inv_cost_lookup = {
            str(row['product']): row['cost_aud']
            for _, row in inventory_df.iterrows()
            if pd.notnull(row['cost_aud'])
        }
        revenue_cost_lookup = {
            str(row['product_id']): row['revenue_cost_aud']
            for _, row in cost_df.iterrows()
            if pd.notnull(row['revenue_cost_aud'])
        }
        # Build lookup dicts for fast access
        product_map = {row['Product']: row for _, row in product_df.iterrows()}
        site_map = {row['SiteName']: row for _, row in site_df.iterrows()}
        wip_lookup = {(row['product'], row['site_id']): row['total_wip'] for _, row in wip_df.iterrows()}
        onhand_lookup = {(row['product'], row['site_id']): row['total_onhand'] for _, row in onhand_df.iterrows()}



        calculated_productions = []

        for _, replenishment in replenishments.iterrows():
            product = replenishment['Product']
            site = replenishment['Site']
            shipping_date = replenishment['ShippingDate']
            total_qty = replenishment['total_qty']

            cast_to_despatch_days = cast_to_despatch.get((site, scenario.version), 0)
            pouring_date = shipping_date - timedelta(days=cast_to_despatch_days)

            # Apply inventory date logic
            if inventory_threshold_date and pouring_date < inventory_threshold_date:
                pouring_date = inventory_start_date

            product_row = product_map.get(product)
            site_row = site_map.get(site)

            if product_row is None or site_row is None:
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

            if total_qty > 0:
                production_quantity = total_qty
            else:
                production_quantity = 0

            dress_mass = product_row['DressMass'] or 0
            tonnes = (production_quantity * dress_mass) / 1000

            product_key = str(product_row['Product'])

            costs = [
                cost_lookup.get(product_key, 0),
                inv_cost_lookup.get(product_key, 0),
                revenue_cost_lookup.get(product_key, 0)
            ]
            cost = max(costs) if any(costs) else 0

            cogs_aud = cost * production_quantity

            calculated_productions.append(CalculatedProductionModel(
                version=scenario,
                product_id=product_row['Product'],
                site_id=site_row['SiteName'],
                pouring_date=pouring_date,
                production_quantity=production_quantity,
                tonnes=tonnes,
                product_group=product_row['ProductGroup'],
                parent_product_group=product_row.get('ParentProductGroupDescription', ''),
                cogs_aud=cogs_aud,
            ))

            wip_lookup[(product, site)] = wip_stock
            onhand_lookup[(product, site)] = onhand_stock

        if calculated_productions:
            CalculatedProductionModel.objects.bulk_create(calculated_productions, batch_size=1000)

        self.stdout.write(self.style.SUCCESS(f"CalculatedProductionModel populated for version {version} with inventory date logic applied."))