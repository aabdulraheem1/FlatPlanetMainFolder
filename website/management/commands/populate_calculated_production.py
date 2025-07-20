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
    MasterDataPlantModel, 
    ProductSiteCostModel,
    SMART_Forecast_Model,
    FixedPlantConversionModifiersModel,
    RevenueToCogsConversionModel,
    SiteAllocationModel,
    AggregatedForecast
)
import pandas as pd

class Command(BaseCommand):
    help = "Populate data in CalculatedProductionModel from multiple sources (replenishment, fixed plant, revenue forecast)"

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
            self.stdout.write(self.style.ERROR(f"Scenario version '{version}' not found."))
            return

        # Clear existing calculated production data
        CalculatedProductionModel.objects.filter(version=scenario).delete()
        self.stdout.write("Deleted existing calculated production data")

        # Get the first inventory snapshot date and calculate the threshold date
        first_inventory = MasterDataInventory.objects.filter(version=scenario).order_by('date_of_snapshot').first()
        if first_inventory:
            # Add one day to the first snapshot date
            inventory_threshold_date = first_inventory.date_of_snapshot + timedelta(days=1)
            # Set to first day of that month
            inventory_start_date = inventory_threshold_date.replace(day=1)
            self.stdout.write(f"Inventory threshold date: {inventory_threshold_date}")
            self.stdout.write(f"Minimum pouring date for Fixed Plant/Revenue: {inventory_start_date}")
        else:
            inventory_threshold_date = None
            inventory_start_date = None
            self.stdout.write("No inventory data found - will process all dates")

        calculated_productions = []

        # 1. Process Regular Replenishment Data (SMART forecast excluding Fixed Plant and Revenue Forecast)
        self.stdout.write("\n=== Processing Regular Replenishment Data ===")
        self.process_replenishment_data(scenario, inventory_threshold_date, inventory_start_date, calculated_productions)

        # 2. Process Fixed Plant Data
        self.stdout.write("\n=== Processing Fixed Plant Data ===")
        self.process_fixed_plant_data(scenario, inventory_start_date, calculated_productions)

        # 3. Process Revenue Forecast Data
        self.stdout.write("\n=== Processing Revenue Forecast Data ===")
        self.process_revenue_forecast_data(scenario, inventory_start_date, calculated_productions)

        # Bulk create all records
        if calculated_productions:
            CalculatedProductionModel.objects.bulk_create(calculated_productions, batch_size=1000)
            self.stdout.write(self.style.SUCCESS(f"Created {len(calculated_productions)} CalculatedProductionModel records"))
        else:
            self.stdout.write(self.style.WARNING("No records created"))

        self.stdout.write(self.style.SUCCESS(f"CalculatedProductionModel populated for version {version}"))

    def process_replenishment_data(self, scenario, inventory_threshold_date, inventory_start_date, calculated_productions):
        """Process regular replenishment data - CORRECTED production calculation"""
        
        # Load replenishments in bulk
        replenishments = pd.DataFrame(list(
            CalcualtedReplenishmentModel.objects.filter(version=scenario)
            .values('Product', 'Site', 'ShippingDate', 'ReplenishmentQty')
        ))
        
        if replenishments.empty:
            self.stdout.write("No replenishment records found")
            return

        # Load product and site data
        product_df = pd.DataFrame(list(MasterDataProductModel.objects.all().values('Product', 'DressMass', 'ProductGroup', 'ParentProductGroupDescription')))
        site_df = pd.DataFrame(list(MasterDataPlantModel.objects.all().values('SiteName')))

        # Load cast to despatch days
        cast_to_despatch = {
            (entry.Foundry.SiteName, entry.version.version): entry.CastToDespatchDays
            for entry in MasterDataCastToDespatchModel.objects.filter(version=scenario)
        }

        # Load cost data (keeping your existing cost logic)
        cost_df = pd.DataFrame(list(
            ProductSiteCostModel.objects.filter(version=scenario)
            .values('version_id', 'product_id', 'site_id', 'cost_aud', 'revenue_cost_aud')
        ))
        
        inventory_df = pd.DataFrame(list(
            MasterDataInventory.objects.filter(version=scenario)
            .values('version_id', 'product', 'site_id', 'cost_aud')
        ))

        # Build cost lookup dicts
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

        product_map = {row['Product']: row for _, row in product_df.iterrows()}
        site_map = {row['SiteName']: row for _, row in site_df.iterrows()}

        # Load production site inventory (opening balances)
        production_site_inventory = {}
        for _, row in pd.DataFrame(list(
            MasterDataInventory.objects.filter(version=scenario)
            .values('product', 'site_id', 'onhandstock_qty', 'intransitstock_qty', 'wip_stock_qty')
        )).iterrows():
            key = (str(row['product']), str(row['site_id']))
            production_site_inventory[key] = {
                'onhand': row['onhandstock_qty'] or 0,
                'intransit': row['intransitstock_qty'] or 0,
                'wip': row['wip_stock_qty'] or 0
            }

        print(f"DEBUG: Loaded production site inventory for {len(production_site_inventory)} product-site combinations")

        # Process each replenishment record individually, but aggregate by date
        replenishments['pouring_date'] = replenishments.apply(lambda row: 
            row['ShippingDate'] - timedelta(days=cast_to_despatch.get((row['Site'], scenario.version), 0)), axis=1)
        
        # Apply inventory date logic
        if inventory_threshold_date and inventory_start_date:
            replenishments['pouring_date'] = replenishments['pouring_date'].apply(
                lambda x: inventory_start_date if x < inventory_threshold_date else x
            )

        # Group by product, site, and pouring_date to handle multiple shipments on same day
        daily_replenishments = replenishments.groupby(['Product', 'Site', 'pouring_date'], as_index=False)['ReplenishmentQty'].sum()
        
        # Group by product-site and sort by pouring date
        replenishment_by_product_site = {}
        for _, row in daily_replenishments.iterrows():
            product = row['Product']
            site = row['Site']
            pouring_date = row['pouring_date']
            replenishment_qty = row['ReplenishmentQty']

            product_site_key = (product, site)
            if product_site_key not in replenishment_by_product_site:
                replenishment_by_product_site[product_site_key] = []
            
            replenishment_by_product_site[product_site_key].append({
                'pouring_date': pouring_date,
                'replenishment_qty': replenishment_qty
            })

        # Sort replenishments by pouring date for each product-site
        for product_site_key in replenishment_by_product_site:
            replenishment_by_product_site[product_site_key].sort(key=lambda x: x['pouring_date'])

        # Process each product-site combination
        for (product, site), replenishments_list in replenishment_by_product_site.items():
            product_row = product_map.get(product)
            site_row = site_map.get(site)

            if product_row is None or site_row is None:
                continue

            # Get opening inventory at production site
            production_site_key = (str(product), str(site))
            opening_inventory = production_site_inventory.get(production_site_key, {
                'onhand': 0, 'intransit': 0, 'wip': 0
            })
            
            # Start with opening balance - this will be consumed first
            remaining_stock = opening_inventory['onhand'] + opening_inventory['intransit'] + opening_inventory['wip']
            
            print(f"DEBUG: Processing {product} at {site} - Opening balance: {remaining_stock}")
            
            for replenishment in replenishments_list:
                pouring_date = replenishment['pouring_date']
                replenishment_qty = replenishment['replenishment_qty']
                
                # Start with the full replenishment quantity as needed production
                production_quantity = replenishment_qty
                
                # Use remaining stock to reduce production quantity
                if remaining_stock > 0:
                    if remaining_stock >= production_quantity:
                        # We have enough stock to cover entire production
                        stock_used = production_quantity
                        production_quantity = 0
                        remaining_stock -= stock_used
                        print(f"DEBUG: {product} at {site} on {pouring_date}:")
                        print(f"DEBUG: - Stock before: {remaining_stock + stock_used}")
                        print(f"DEBUG: - Replenishment: {replenishment_qty}")
                        print(f"DEBUG: - Stock used: {stock_used}")
                        print(f"DEBUG: - Production: {production_quantity}")
                        print(f"DEBUG: - Remaining stock: {remaining_stock}")
                    else:
                        # Use all remaining stock, still need some production
                        stock_used = remaining_stock
                        production_quantity -= remaining_stock
                        remaining_stock = 0
                        print(f"DEBUG: {product} at {site} on {pouring_date}:")
                        print(f"DEBUG: - Stock before: {stock_used}")
                        print(f"DEBUG: - Replenishment: {replenishment_qty}")
                        print(f"DEBUG: - Stock used: {stock_used}")
                        print(f"DEBUG: - Production: {production_quantity}")
                        print(f"DEBUG: - Remaining stock: {remaining_stock}")
                else:
                    # No stock left, need full production
                    print(f"DEBUG: {product} at {site} on {pouring_date}:")
                    print(f"DEBUG: - Stock before: 0")
                    print(f"DEBUG: - Replenishment: {replenishment_qty}")
                    print(f"DEBUG: - Stock used: 0")
                    print(f"DEBUG: - Production: {production_quantity}")
                    print(f"DEBUG: - Remaining stock: 0")

                # Calculate derived values
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

                # Create production record (even if production_quantity is 0)
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
                print(f"DEBUG: - Created production record: {production_quantity} units")

        self.stdout.write(f"Processed {len(daily_replenishments)} daily replenishment records")

    def process_fixed_plant_data(self, scenario, inventory_start_date, calculated_productions):
        """Process Fixed Plant data (similar to populate_aggregated_forecast logic)"""
        
        # Get Fixed Plant forecasts
        fixed_plant_forecasts = SMART_Forecast_Model.objects.filter(
            version=scenario,
            Data_Source='Fixed Plant'
        ).select_related()

        if not fixed_plant_forecasts.exists():
            self.stdout.write("No Fixed Plant forecast data found")
            return

        for forecast in fixed_plant_forecasts:
            try:
                if not forecast.Product or not forecast.Qty:
                    continue

                # Get product object
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=forecast.Product)
                except MasterDataProductModel.DoesNotExist:
                    self.stdout.write(f"Warning: Product {forecast.Product} not found in master data")
                    continue

                qty = forecast.Qty or 0.0
                pouring_date = forecast.Period_AU

                # Check if pouring date is before inventory start date - skip if so
                if inventory_start_date and pouring_date < inventory_start_date:
                    self.stdout.write(f"Skipping Fixed Plant record for {forecast.Product} - pouring date {pouring_date} is before inventory start date {inventory_start_date}")
                    continue

                try:
                    # Get Fixed Plant conversion modifier (NO SITE FILTERING - same as aggregate forecast)
                    modifier = FixedPlantConversionModifiersModel.objects.get(
                        version=scenario,
                        Product=product_obj
                    )

                    # Revenue AUD = Qty
                    revenue_aud = qty

                    # Convert percentages to decimals
                    freight_percentage = (modifier.FreightPercentage or 0.0) / 100.0
                    gross_margin = (modifier.GrossMargin or 0.0) / 100.0
                    external_material_percentage = (modifier.ExternalMaterialComponents or 0.0) / 100.0
                    material_cost_percentage = (modifier.MaterialCostPercentage or 0.0) / 100.0
                    cost_per_sqm_or_kg = modifier.CostPerSQMorKgAUD or 0.0

                    # COGS = (1 - (freight_percentage + gross_margin)) * Qty
                    cogs_aud = (1 - (freight_percentage + gross_margin)) * qty

                    # Tonnes calculation - divide by 1000 to convert KG to tonnes
                    if cost_per_sqm_or_kg > 0:
                        tonnes = (
                            (1 - (freight_percentage + gross_margin)) * 
                            qty * 
                            (1 - external_material_percentage) * 
                            material_cost_percentage
                        ) / cost_per_sqm_or_kg / 1000
                    else:
                        tonnes = 0.0

                    # Determine site based on product type (for display purposes only)
                    if forecast.Product in ['FP-Duablock', 'FP-Cast Liners']:
                        site_name = 'WUN1'
                    else:
                        site_name = 'BAS1'

                    self.stdout.write(f"Fixed Plant - Product: {forecast.Product}, Site: {site_name}, Revenue: {revenue_aud}, COGS: {cogs_aud}, Tonnes: {tonnes}")

                    # Create record for the determined site with production_quantity = 0
                    calculated_productions.append(CalculatedProductionModel(
                        version=scenario,
                        product_id=product_obj.Product,
                        site_id=site_name,
                        pouring_date=pouring_date,
                        production_quantity=0,  # Zero for Fixed Plant
                        tonnes=tonnes,
                        product_group=product_obj.ProductGroup,
                        parent_product_group=product_obj.ParentProductGroupDescription,
                        cogs_aud=cogs_aud,
                        revenue_aud=revenue_aud,
                    ))

                except FixedPlantConversionModifiersModel.DoesNotExist:
                    self.stdout.write(f"Warning: No Fixed Plant conversion modifier found for product {forecast.Product} - using DressMass fallback")
                    
                    # Fallback to DressMass calculation
                    try:
                        dress_mass = product_obj.DressMass or 0
                        tonnes = (qty * dress_mass) / 1000 if dress_mass > 0 else 0
                        
                        # Default site assignment
                        if forecast.Product in ['FP-Duablock', 'FP-Cast Liners']:
                            site_name = 'WUN1'
                        else:
                            site_name = 'BAS1'
                        
                        calculated_productions.append(CalculatedProductionModel(
                            version=scenario,
                            product_id=product_obj.Product,
                            site_id=site_name,
                            pouring_date=pouring_date,
                            production_quantity=0,
                            tonnes=tonnes,
                            product_group=product_obj.ProductGroup,
                            parent_product_group=product_obj.ParentProductGroupDescription,
                            cogs_aud=0.0,
                            revenue_aud=qty,
                        ))
                    except Exception as fallback_error:
                        self.stdout.write(f"Error in DressMass fallback for {forecast.Product}: {fallback_error}")
                        continue

            except Exception as e:
                self.stdout.write(f"Error processing Fixed Plant record: {e}")
                continue

        self.stdout.write(f"Processed Fixed Plant data")

    def process_revenue_forecast_data(self, scenario, inventory_start_date, calculated_productions):
        """Process Revenue Forecast data (similar to populate_aggregated_forecast logic)"""
        
        # Get Revenue Forecast data
        revenue_forecasts = SMART_Forecast_Model.objects.filter(
            version=scenario,
            Data_Source='Revenue Forecast'
        ).select_related()

        if not revenue_forecasts.exists():
            self.stdout.write("No Revenue Forecast data found")
            return

        for forecast in revenue_forecasts:
            try:
                if not forecast.Product or not forecast.Qty:
                    continue

                # Get product object
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=forecast.Product)
                except MasterDataProductModel.DoesNotExist:
                    self.stdout.write(f"Warning: Product {forecast.Product} not found in master data")
                    continue

                qty = forecast.Qty or 0.0
                pouring_date = forecast.Period_AU

                # Check if pouring date is before inventory start date - skip if so
                if inventory_start_date and pouring_date < inventory_start_date:
                    self.stdout.write(f"Skipping Revenue Forecast record for {forecast.Product} - pouring date {pouring_date} is before inventory start date {inventory_start_date}")
                    continue

                try:
                    # Step 1: Convert Revenue to COGS and Tonnes
                    conversion_modifier = RevenueToCogsConversionModel.objects.get(
                        version=scenario,
                        Product=product_obj
                    )

                    # Revenue = Qty
                    revenue_aud = qty

                    # Convert percentages to decimals
                    gross_margin = (conversion_modifier.GrossMargin or 0.0) / 100.0
                    inhouse_production = (conversion_modifier.InHouseProduction or 0.0) / 100.0
                    cost_aud_per_kg = conversion_modifier.CostAUDPerKG or 0.0

                    # COGS = qty * (1 - GrossMargin)
                    total_cogs_aud = qty * (1 - gross_margin)

                    # Tonnes = (COGS * inhouse_production) / costAUDPerKG / 1000 (convert KG to tonnes)
                    if cost_aud_per_kg > 0:
                        total_tonnes = (total_cogs_aud * inhouse_production) / cost_aud_per_kg / 1000
                    else:
                        total_tonnes = 0.0

                    # Step 2: Allocate to sites based on allocation percentages
                    site_allocations = SiteAllocationModel.objects.filter(
                        version=scenario,
                        Product=product_obj
                    )

                    if site_allocations.exists():
                        for allocation in site_allocations:
                            allocation_percentage = (allocation.AllocationPercentage or 0.0) / 100.0

                            # Allocate based on percentage
                            allocated_revenue = revenue_aud * allocation_percentage
                            allocated_cogs = total_cogs_aud * allocation_percentage
                            allocated_tonnes = total_tonnes * allocation_percentage

                            self.stdout.write(f"Revenue Allocation - Product: {forecast.Product}, Site: {allocation.Site.SiteName}, Revenue: {allocated_revenue}, COGS: {allocated_cogs}, Tonnes: {allocated_tonnes}")

                            # Create record for this site with production_quantity = 0
                            calculated_productions.append(CalculatedProductionModel(
                                version=scenario,
                                product_id=product_obj.Product,
                                site_id=allocation.Site.SiteName,
                                pouring_date=pouring_date,
                                production_quantity=0,  # Zero for Revenue Forecast
                                tonnes=allocated_tonnes,
                                product_group=product_obj.ProductGroup,
                                parent_product_group=product_obj.ParentProductGroupDescription,
                                cogs_aud=allocated_cogs,
                                revenue_aud=allocated_revenue,
                            ))
                    else:
                        self.stdout.write(f"Warning: No site allocation found for product {forecast.Product}")

                except RevenueToCogsConversionModel.DoesNotExist:
                    self.stdout.write(f"Warning: No revenue conversion modifier found for product {forecast.Product}")
                    continue

            except Exception as e:
                self.stdout.write(f"Error processing Revenue Forecast record: {e}")
                continue

        self.stdout.write(f"Processed Revenue Forecast data")