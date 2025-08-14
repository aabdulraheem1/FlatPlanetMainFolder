from datetime import timedelta
import time
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
import polars as pl
from website.powerbi_invoice_integration import get_customer_mapping_dict

class Command(BaseCommand):
    help = "Populate data in CalculatedProductionModel from multiple sources (replenishment, fixed plant, revenue forecast)"

    def add_arguments(self, parser):
        parser.add_argument(
            'scenario_version',
            type=str,
            help="The version of the scenario to populate data for.",
        )
        parser.add_argument(
            '--product',
            type=str,
            help="Optional: Filter calculation to a specific product only (much faster for testing)",
        )

    def handle(self, *args, **kwargs):
        version = kwargs['scenario_version']
        single_product = kwargs.get('product')

        if not version:
            self.stdout.write(self.style.ERROR("No version argument provided."))
            return

        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario version '{version}' not found."))
            return

        # Clear existing calculated production data (for single product if specified)
        if single_product:
            deleted_count = CalculatedProductionModel.objects.filter(
                version=scenario,
                product__Product=single_product
            ).delete()[0]
            self.stdout.write(f"Deleted existing calculated production data for product {single_product}: {deleted_count} records")
        else:
            CalculatedProductionModel.objects.filter(version=scenario).delete()
            self.stdout.write("Deleted existing calculated production data")

        # Fetch customer mapping for this scenario (NOW FROM LOCAL DATABASE - MUCH FASTER)
        self.stdout.write("🔗 Loading customer data from MasterDataProductModel (local, fast)...")
        customer_start = time.time()
        
        if single_product:
            customer_products = MasterDataProductModel.objects.filter(
                Product=single_product,
                latest_customer_name__isnull=False
            ).values('Product', 'latest_customer_name', 'latest_invoice_date')
        else:
            customer_products = MasterDataProductModel.objects.filter(
                latest_customer_name__isnull=False
            ).values('Product', 'latest_customer_name', 'latest_invoice_date')
        
        # Build customer mapping dictionary
        customer_mapping = {}
        for product_data in customer_products:
            customer_mapping[product_data['Product']] = {
                'customer_name': product_data['latest_customer_name'],
                'invoice_date': product_data['latest_invoice_date']
            }
        
        customer_load_time = time.time() - customer_start
        
        if single_product:
            self.stdout.write(f"✅ Loaded customer data for {len(customer_mapping)} product(s) in {customer_load_time:.3f}s (filtered for {single_product})")
        else:
            self.stdout.write(f"✅ Loaded customer data for {len(customer_mapping)} products in {customer_load_time:.3f}s")
        
        self.stdout.write(self.style.SUCCESS(f"🚀 PERFORMANCE IMPROVEMENT: Customer data loaded from local database instead of slow PowerBI queries"))

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
        self.process_replenishment_data(scenario, inventory_threshold_date, inventory_start_date, calculated_productions, customer_mapping, single_product)

        # 2. Process Fixed Plant Data
        self.stdout.write("\n=== Processing Fixed Plant Data ===")
        self.process_fixed_plant_data(scenario, inventory_start_date, calculated_productions, customer_mapping, single_product)

        # 3. Process Revenue Forecast Data
        self.stdout.write("\n=== Processing Revenue Forecast Data ===")
        self.process_revenue_forecast_data(scenario, inventory_start_date, calculated_productions, customer_mapping, single_product)

        # Bulk create all records
        if calculated_productions:
            CalculatedProductionModel.objects.bulk_create(calculated_productions, batch_size=1000)
            self.stdout.write(self.style.SUCCESS(f"Created {len(calculated_productions)} CalculatedProductionModel records"))
        else:
            self.stdout.write(self.style.WARNING("No records created"))

        self.stdout.write(self.style.SUCCESS(f"CalculatedProductionModel populated for version {version}"))

    def process_replenishment_data(self, scenario, inventory_threshold_date, inventory_start_date, calculated_productions, customer_mapping, single_product=None):
        """Process regular replenishment data - CORRECTED production calculation"""
        
        # Load replenishments in bulk using pandas first for better schema handling
        if single_product:
            replenishments_data = list(
                CalcualtedReplenishmentModel.objects.filter(
                    version=scenario,
                    Product__Product=single_product
                ).values('Product', 'Site', 'ShippingDate', 'ReplenishmentQty')
            )
            self.stdout.write(f"Loading replenishment data for single product: {single_product}")
        else:
            replenishments_data = list(
                CalcualtedReplenishmentModel.objects.filter(version=scenario)
                .values('Product', 'Site', 'ShippingDate', 'ReplenishmentQty')
            )
            self.stdout.write("Loading replenishment data for all products")
        
        if not replenishments_data:
            self.stdout.write("No replenishment records found")
            return
            
        replenishments = pl.from_pandas(pd.DataFrame(replenishments_data))

        # Load product and site data
        product_df = pl.from_pandas(pd.DataFrame(list(MasterDataProductModel.objects.all().values('Product', 'DressMass', 'ProductGroup', 'ParentProductGroupDescription'))))
        site_df = pl.from_pandas(pd.DataFrame(list(MasterDataPlantModel.objects.all().values('SiteName'))))

        # Load cast to despatch days
        cast_to_despatch = {
            (entry.Foundry.SiteName, entry.version.version): entry.CastToDespatchDays
            for entry in MasterDataCastToDespatchModel.objects.filter(version=scenario)
        }

        # Load cost data from AggregatedForecast (PERFORMANCE ENHANCEMENT: Use aggregated COGS/Qty ratio)
        self.stdout.write("📊 Loading cost data from AggregatedForecast (fast Polars method)...")
        cost_start_time = time.time()
        
        aggregated_forecast_data = list(
            AggregatedForecast.objects.filter(version=scenario)
            .values('product_id', 'cogs_aud', 'qty')
        )
        
        if aggregated_forecast_data:
            # Use Polars for fast aggregation
            cost_df = pl.from_pandas(pd.DataFrame(aggregated_forecast_data))
            
            # Filter out records with zero or null qty to avoid division by zero
            cost_df = cost_df.filter(
                (pl.col('qty').is_not_null()) & 
                (pl.col('qty') > 0) & 
                (pl.col('cogs_aud').is_not_null())
            )
            
            # Group by product and calculate weighted average cost per unit
            cost_summary = cost_df.group_by('product_id').agg([
                pl.col('cogs_aud').sum().alias('total_cogs'),
                pl.col('qty').sum().alias('total_qty')
            ]).with_columns([
                (pl.col('total_cogs') / pl.col('total_qty')).alias('cost_per_unit')
            ]).filter(pl.col('total_qty') > 0)  # Additional safety check
            
            # Build cost lookup dict from aggregated forecast data
            cost_lookup = {}
            for row in cost_summary.iter_rows(named=True):
                cost_lookup[str(row['product_id'])] = row['cost_per_unit']
                
            cost_load_time = time.time() - cost_start_time
            self.stdout.write(f"✅ Loaded cost data for {len(cost_lookup)} products from AggregatedForecast in {cost_load_time:.3f}s")
            self.stdout.write(f"🚀 PERFORMANCE: Using AggregatedForecast COGS/Qty ratio instead of multiple cost tables")
        else:
            cost_lookup = {}
            self.stdout.write("⚠️  No AggregatedForecast data found - cost lookup will be empty")

        product_map = {row['Product']: row for row in product_df.iter_rows(named=True)}
        site_map = {row['SiteName']: row for row in site_df.iter_rows(named=True)}

        # Load production site inventory (opening balances)
        production_site_inventory = {}
        inventory_data = pl.from_pandas(pd.DataFrame(list(
            MasterDataInventory.objects.filter(version=scenario)
            .values('product', 'site_id', 'onhandstock_qty', 'intransitstock_qty', 'wip_stock_qty')
        )))
        
        for row in inventory_data.iter_rows(named=True):
            key = (str(row['product']), str(row['site_id']))
            production_site_inventory[key] = {
                'onhand': row['onhandstock_qty'] or 0,
                'intransit': row['intransitstock_qty'] or 0,
                'wip': row['wip_stock_qty'] or 0
            }

        replenishments = replenishments.with_columns([
            pl.col('ShippingDate').map_elements(
                lambda shipping_date: shipping_date - timedelta(
                    days=cast_to_despatch.get((replenishments.filter(pl.col('ShippingDate') == shipping_date)['Site'][0], scenario.version), 0)
                ), return_dtype=pl.Date
            ).alias('pouring_date')
        ])
        
        # Apply inventory date logic
        if inventory_threshold_date and inventory_start_date:
            replenishments = replenishments.with_columns([
                pl.col('pouring_date').map_elements(
                    lambda x: inventory_start_date if x < inventory_threshold_date else x,
                    return_dtype=pl.Date
                )
            ])

        # Group by product, site, and pouring_date to handle multiple shipments on same day
        daily_replenishments = replenishments.group_by(['Product', 'Site', 'pouring_date']).agg([
            pl.col('ReplenishmentQty').sum()
        ])
        
        # Group by product-site and sort by pouring date
        replenishment_by_product_site = {}
        for row in daily_replenishments.iter_rows(named=True):
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
                    else:
                        # Use all remaining stock, still need some production
                        stock_used = remaining_stock
                        production_quantity -= remaining_stock
                        remaining_stock = 0
                else:
                    # No stock left, need full production
                    pass

                # Calculate derived values
                dress_mass = product_row['DressMass'] or 0
                tonnes = (production_quantity * dress_mass) / 1000

                product_key = str(product_row['Product'])
                # Use simplified cost calculation from AggregatedForecast COGS/Qty ratio
                cost = cost_lookup.get(product_key, 0)
                cogs_aud = cost * production_quantity

                # Get customer invoice data
                customer_data = customer_mapping.get(product_key, {})
                latest_customer_invoice = customer_data.get('customer_name')
                latest_customer_invoice_date = customer_data.get('invoice_date')

                # Check if site is outsourced
                try:
                    site_obj = MasterDataPlantModel.objects.get(SiteName=site_row['SiteName'])
                    is_outsourced = site_obj.mark_as_outsource_supplier
                except MasterDataPlantModel.DoesNotExist:
                    is_outsourced = False

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
                    production_aud=cogs_aud,  # Use production_aud field name
                    latest_customer_invoice=latest_customer_invoice,
                    latest_customer_invoice_date=latest_customer_invoice_date,
                    is_outsourced=is_outsourced,
                ))

        self.stdout.write(f"Processed {len(daily_replenishments)} daily replenishment records")

    def process_fixed_plant_data(self, scenario, inventory_start_date, calculated_productions, customer_mapping, single_product=None):
        """Process Fixed Plant data (similar to populate_aggregated_forecast logic)"""
        
        # Get Fixed Plant forecasts
        if single_product:
            fixed_plant_forecasts = SMART_Forecast_Model.objects.filter(
                version=scenario,
                Data_Source='Fixed Plant',
                Product=single_product
            ).select_related()
            self.stdout.write(f"Loading Fixed Plant data for single product: {single_product}")
        else:
            fixed_plant_forecasts = SMART_Forecast_Model.objects.filter(
                version=scenario,
                Data_Source='Fixed Plant'
            ).select_related()
            self.stdout.write("Loading Fixed Plant data for all products")

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

                    # Get customer invoice data
                    customer_data = customer_mapping.get(forecast.Product, {})
                    latest_customer_invoice = customer_data.get('customer_name')
                    latest_customer_invoice_date = customer_data.get('invoice_date')

                    # Check if site is outsourced
                    try:
                        site_obj = MasterDataPlantModel.objects.get(SiteName=site_name)
                        is_outsourced = site_obj.mark_as_outsource_supplier
                    except MasterDataPlantModel.DoesNotExist:
                        is_outsourced = False

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
                        production_aud=cogs_aud,  # Use production_aud field name
                        revenue_aud=revenue_aud,
                        latest_customer_invoice=latest_customer_invoice,
                        latest_customer_invoice_date=latest_customer_invoice_date,
                        is_outsourced=is_outsourced,
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
                        
                        # Get customer invoice data for fallback
                        customer_data = customer_mapping.get(forecast.Product, {})
                        latest_customer_invoice = customer_data.get('customer_name')
                        latest_customer_invoice_date = customer_data.get('invoice_date')
                        
                        # Check if site is outsourced
                        try:
                            site_obj = MasterDataPlantModel.objects.get(SiteName=site_name)
                            is_outsourced = site_obj.mark_as_outsource_supplier
                        except MasterDataPlantModel.DoesNotExist:
                            is_outsourced = False
                        
                        calculated_productions.append(CalculatedProductionModel(
                            version=scenario,
                            product_id=product_obj.Product,
                            site_id=site_name,
                            pouring_date=pouring_date,
                            production_quantity=0,
                            tonnes=tonnes,
                            product_group=product_obj.ProductGroup,
                            parent_product_group=product_obj.ParentProductGroupDescription,
                            production_aud=0.0,
                            revenue_aud=qty,
                            latest_customer_invoice=latest_customer_invoice,
                            latest_customer_invoice_date=latest_customer_invoice_date,
                            is_outsourced=is_outsourced,
                        ))
                    except Exception as fallback_error:
                        self.stdout.write(f"Error in DressMass fallback for {forecast.Product}: {fallback_error}")
                        continue

            except Exception as e:
                self.stdout.write(f"Error processing Fixed Plant record: {e}")
                continue

        self.stdout.write(f"Processed Fixed Plant data")

    def process_revenue_forecast_data(self, scenario, inventory_start_date, calculated_productions, customer_mapping, single_product=None):
        """Process Revenue Forecast data (similar to populate_aggregated_forecast logic)"""
        
        # Get Revenue Forecast data
        if single_product:
            revenue_forecasts = SMART_Forecast_Model.objects.filter(
                version=scenario,
                Data_Source='Revenue Forecast',
                Product=single_product
            ).select_related()
            self.stdout.write(f"Loading Revenue Forecast data for single product: {single_product}")
        else:
            revenue_forecasts = SMART_Forecast_Model.objects.filter(
                version=scenario,
                Data_Source='Revenue Forecast'
            ).select_related()
            self.stdout.write("Loading Revenue Forecast data for all products")

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

                            # Get customer invoice data
                            customer_data = customer_mapping.get(forecast.Product, {})
                            latest_customer_invoice = customer_data.get('customer_name')
                            latest_customer_invoice_date = customer_data.get('invoice_date')

                            # Check if site is outsourced
                            try:
                                site_obj = MasterDataPlantModel.objects.get(SiteName=allocation.Site.SiteName)
                                is_outsourced = site_obj.mark_as_outsource_supplier
                            except MasterDataPlantModel.DoesNotExist:
                                is_outsourced = False

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
                                production_aud=allocated_cogs,
                                revenue_aud=allocated_revenue,
                                latest_customer_invoice=latest_customer_invoice,
                                latest_customer_invoice_date=latest_customer_invoice_date,
                                is_outsourced=is_outsourced,
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