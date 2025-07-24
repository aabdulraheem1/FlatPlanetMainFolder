from django.core.management.base import BaseCommand
from django.db.models import Sum, Min, Max
from django.core.cache import cache
from django.db import transaction
from website.models import (
    scenarios,
    SMART_Forecast_Model,
    MasterDataInventory,
    MasterDataProductModel,
    MasterDataOrderBook,
    MasterDataHistoryOfProductionModel,
    MasterDataPlantModel,
    CalcualtedReplenishmentModel,
    CalculatedProductionModel,
    MasterDataFreightModel,
    MasterdataIncoTermsModel,
    MasterDataIncotTermTypesModel,
    MasterDataManuallyAssignProductionRequirement,
    MasterDataSafetyStocks,
    MasterDataCastToDespatchModel,
    MasterDataEpicorMethodOfManufacturingModel,
)
import pandas as pd
from datetime import timedelta, date
from collections import defaultdict
import calendar

class Command(BaseCommand):
    help = "V2: New comprehensive inventory and production planning with chronological month processing"

    def add_arguments(self, parser):
        parser.add_argument(
            'version',
            type=str,
            help="The version of the scenario to populate data for.",
        )
        parser.add_argument(
            '--product',
            type=str,
            help="Optional: Process only this specific product for testing",
            default=None
        )

    def extract_location_from_forecast(self, location):
        """Extract last 4 characters after - or _ delimiter"""
        if location:
            if "_" in location:
                parts = location.split("_")
                if len(parts) > 1:
                    return parts[-1][-4:]  # Last 4 characters of last part
            elif "-" in location:
                parts = location.split("-")
                if len(parts) > 1:
                    return parts[-1][-4:]  # Last 4 characters of last part
        return location[-4:] if location and len(location) >= 4 else location

    def calculate_shipping_date(self, period, customer_code, forecast_region, production_site, scenario, snapshot_date):
        """Calculate shipping date based on Incoterm and freight data"""
        shipping_date = period
        
        # Get Incoterm information
        incoterm_mapping = MasterdataIncoTermsModel.objects.filter(
            version=scenario,
            CustomerCode=customer_code
        ).first()
        
        if incoterm_mapping:
            try:
                incoterm_category = incoterm_mapping.Incoterm.IncoTermCaregory
            except AttributeError:
                incoterm_category = None
        else:
            incoterm_category = None

        # Calculate freight lead time if we have incoterm and production site
        if incoterm_category and production_site:
            freight_data = MasterDataFreightModel.objects.filter(
                version=scenario,
                ForecastRegion__Forecast_region=forecast_region,
                ManufacturingSite__SiteName=production_site
            ).first()
            
            if freight_data:
                lead_time_days = 0
                normalized_category = ' '.join(incoterm_category.split()) if incoterm_category else ''
                
                if normalized_category == "NO FREIGHT":
                    lead_time_days = 0
                elif normalized_category == "PLANT TO DOMESTIC PORT":
                    lead_time_days = freight_data.PlantToDomesticPortDays
                elif normalized_category == "PLANT TO DOMESTIC PORT + INT FREIGHT":
                    lead_time_days = freight_data.PlantToDomesticPortDays + freight_data.OceanFreightDays
                elif normalized_category == "PLANT TO DOMESTIC PORT + INT FREIGHT + DOM FREIGHT":
                    lead_time_days = (freight_data.PlantToDomesticPortDays + 
                                    freight_data.OceanFreightDays + 
                                    freight_data.PortToCustomerDays)
                
                if lead_time_days > 0:
                    shipping_date = period - timedelta(days=lead_time_days)

        # Adjust if shipping date is before snapshot date
        if shipping_date < snapshot_date:
            # Set to first day of month after snapshot
            next_month = snapshot_date.replace(day=1)
            if next_month.month == 12:
                next_month = next_month.replace(year=next_month.year + 1, month=1)
            else:
                next_month = next_month.replace(month=next_month.month + 1)
            shipping_date = next_month

        return shipping_date

    def can_assign_to_foundry(self, product, proposed_site):
        """
        Check if a product can be assigned to a foundry site based on manufacturing operations.
        
        Rules:
        1. If product has MOM record but NO Pour/Moulding/Casting operations → Cannot assign to foundries
        2. If product has NO MOM record → Can assign to foundries
        3. If product has MOM record WITH Pour/Moulding/Casting operations → Can assign to foundries
        """
        foundry_sites = ['XUZ1', 'MTJ1', 'COI2', 'MER1', 'WOD1', 'WUN1', 'CHI1']
        
        # If the proposed site is not a foundry, allow assignment
        if proposed_site not in foundry_sites:
            return True
        
        # Check if product has any manufacturing operations
        mom_records = MasterDataEpicorMethodOfManufacturingModel.objects.filter(ProductKey=product)
        
        if not mom_records.exists():
            # Rule 2: No MOM record → Can assign to foundries
            return True
        
        # Check if any operation contains foundry-related keywords
        foundry_keywords = ['pour', 'moulding', 'casting']
        for record in mom_records:
            operation_desc = (record.OperationDesc or '').lower()
            if any(keyword in operation_desc for keyword in foundry_keywords):
                # Rule 3: Has foundry operations → Can assign to foundries
                return True
        
        # Rule 1: Has MOM record but no foundry operations → Cannot assign to foundries
        return False

    def assign_production_site(self, product, shipping_date, scenario):
        """Assign production site based on priority logic with manufacturing capability validation"""
        
        # Track blocked assignments for summary (avoid print statements during processing)
        if not hasattr(self, 'blocked_assignments'):
            self.blocked_assignments = {'manual': 0, 'order_book': 0, 'production_history': 0}
        
        # Priority 1: Manual assignment (check same month)
        manual_assignments = MasterDataManuallyAssignProductionRequirement.objects.filter(
            version=scenario,
            Product__Product=product,
            ShippingDate__year=shipping_date.year,
            ShippingDate__month=shipping_date.month
        )
        
        if manual_assignments.exists():
            proposed_site = manual_assignments.first().Site.SiteName
            if self.can_assign_to_foundry(product, proposed_site):
                return proposed_site
            else:
                self.blocked_assignments['manual'] += 1

        # Priority 2: Order book
        order_book_records = MasterDataOrderBook.objects.filter(
            version=scenario,
            productkey=product
        )
        if order_book_records.exists():
            proposed_site = order_book_records.first().site
            if self.can_assign_to_foundry(product, proposed_site):
                return proposed_site
            else:
                self.blocked_assignments['order_book'] += 1

        # Priority 3: Production history
        production_history = MasterDataHistoryOfProductionModel.objects.filter(
            version=scenario,
            Product=product
        ).first()
        
        if production_history:
            proposed_site = production_history.Foundry
            if self.can_assign_to_foundry(product, proposed_site):
                return proposed_site
            else:
                self.blocked_assignments['production_history'] += 1

        return None

    def can_assign_to_foundry_optimized(self, product, proposed_site):
        """Optimized version using pre-loaded cache"""
        foundry_sites = {'XUZ1', 'MTJ1', 'COI2', 'MER1', 'WOD1', 'WUN1', 'CHI1'}
        
        # If the proposed site is not a foundry, allow assignment
        if proposed_site not in foundry_sites:
            return True
        
        # Check cache for manufacturing operations
        if product not in self.manufacturing_cache:
            # Rule 2: No MOM record → Can assign to foundries
            return True
        
        # Rule 3: Has foundry operations → Can assign to foundries
        # Rule 1: Has MOM record but no foundry operations → Cannot assign to foundries
        return self.manufacturing_cache[product]['has_foundry_ops']

    def assign_production_site_optimized(self, product, shipping_date, scenario):
        """Optimized version using pre-loaded cache data"""
        
        # Track blocked assignments for summary (avoid print statements during processing)
        if not hasattr(self, 'blocked_assignments'):
            self.blocked_assignments = {'manual': 0, 'order_book': 0, 'production_history': 0}
        
        # Priority 1: Manual assignment (check same month)
        manual_key = (product, shipping_date.year, shipping_date.month)
        if manual_key in self.manual_assignments_cache:
            proposed_site = self.manual_assignments_cache[manual_key]
            if self.can_assign_to_foundry_optimized(product, proposed_site):
                return proposed_site
            else:
                self.blocked_assignments['manual'] += 1

        # Priority 2: Order book
        if product in self.order_book_cache:
            proposed_site = self.order_book_cache[product]
            if self.can_assign_to_foundry_optimized(product, proposed_site):
                return proposed_site
            else:
                self.blocked_assignments['order_book'] += 1

        # Priority 3: Production history
        if product in self.production_history_cache:
            proposed_site = self.production_history_cache[product]
            if self.can_assign_to_foundry_optimized(product, proposed_site):
                return proposed_site
            else:
                self.blocked_assignments['production_history'] += 1

        return None

    def calculate_shipping_date_optimized(self, period, customer_code, forecast_region, production_site, snapshot_date):
        """Optimized version using pre-loaded cache data"""
        shipping_date = period
        
        # Get Incoterm information from cache
        incoterm_category = self.incoterms_cache.get(customer_code)

        # Calculate freight lead time if we have incoterm and production site
        if incoterm_category and production_site:
            freight_key = (forecast_region, production_site)
            freight_data = self.freight_cache.get(freight_key)
            
            if freight_data:
                lead_time_days = 0
                normalized_category = ' '.join(incoterm_category.split()) if incoterm_category else ''
                
                if normalized_category == "NO FREIGHT":
                    lead_time_days = 0
                elif normalized_category == "PLANT TO DOMESTIC PORT":
                    lead_time_days = freight_data['plant_to_port']
                elif normalized_category == "PLANT TO DOMESTIC PORT + INT FREIGHT":
                    lead_time_days = freight_data['plant_to_port'] + freight_data['ocean']
                elif normalized_category == "PLANT TO DOMESTIC PORT + INT FREIGHT + DOM FREIGHT":
                    lead_time_days = (freight_data['plant_to_port'] + 
                                    freight_data['ocean'] + 
                                    freight_data['port_to_customer'])
                
                if lead_time_days > 0:
                    shipping_date = period - timedelta(days=lead_time_days)

        # Adjust if shipping date is before snapshot date
        if shipping_date < snapshot_date:
            # Set to first day of month after snapshot
            next_month = snapshot_date.replace(day=1)
            if next_month.month == 12:
                next_month = next_month.replace(year=next_month.year + 1, month=1)
            else:
                next_month = next_month.replace(month=next_month.month + 1)
            shipping_date = next_month

        return shipping_date

    def calculate_pouring_date_optimized(self, shipping_date, production_site, snapshot_date):
        """Optimized version using pre-loaded cache data"""
        cast_days = self.cast_despatch_cache.get(production_site, 0)
        
        if cast_days:
            pouring_date = shipping_date - timedelta(days=cast_days)
        else:
            # Default to shipping date if no cast to despatch data
            pouring_date = shipping_date

        # Adjust if pouring date is before snapshot date
        if pouring_date < snapshot_date:
            # Set to first day of month after snapshot
            next_month = snapshot_date.replace(day=1)
            if next_month.month == 12:
                next_month = next_month.replace(year=next_month.year + 1, month=1)
            else:
                next_month = next_month.replace(month=next_month.month + 1)
            pouring_date = next_month

        return pouring_date

    def calculate_pouring_date(self, shipping_date, production_site, scenario, snapshot_date):
        """Calculate pouring date by subtracting cast to despatch days"""
        cast_to_despatch = MasterDataCastToDespatchModel.objects.filter(
            version=scenario,
            Foundry__SiteName=production_site
        ).first()
        
        if cast_to_despatch:
            pouring_date = shipping_date - timedelta(days=cast_to_despatch.CastToDespatchDays)
        else:
            # Default to shipping date if no cast to despatch data
            pouring_date = shipping_date

        # Adjust if pouring date is before snapshot date
        if pouring_date < snapshot_date:
            # Set to first day of month after snapshot
            next_month = snapshot_date.replace(day=1)
            if next_month.month == 12:
                next_month = next_month.replace(year=next_month.year + 1, month=1)
            else:
                next_month = next_month.replace(month=next_month.month + 1)
            pouring_date = next_month

        return pouring_date

    def handle(self, *args, **kwargs):
        version = kwargs['version']
        single_product = kwargs.get('product')
        
        if single_product:
            self.stdout.write(f"Processing only product: {single_product}")

        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario '{version}' does not exist"))
            return

        # Check for concurrent execution
        cache_key = f"replenishment_v2_running_{scenario.version.replace(' ', '_')}"
        if single_product:
            cache_key += f"_{single_product}"
        
        if cache.get(cache_key):
            self.stdout.write(self.style.ERROR(f"Replenishment V2 calculation already running for version {version}"))
            return

        # Set the execution lock
        cache.set(cache_key, True, timeout=3600)  # 1 hour timeout

        try:
            with transaction.atomic():
                # Delete existing records
                if single_product:
                    deleted_replenishment = CalcualtedReplenishmentModel.objects.filter(
                        version=scenario,
                        Product__Product=single_product
                    ).delete()[0]
                    deleted_production = CalculatedProductionModel.objects.filter(
                        version=scenario,
                        product__Product=single_product
                    ).delete()[0]
                    self.stdout.write(f"Deleted {deleted_replenishment} replenishment and {deleted_production} production records for product '{single_product}'")
                else:
                    deleted_replenishment = CalcualtedReplenishmentModel.objects.filter(version=scenario).delete()[0]
                    deleted_production = CalculatedProductionModel.objects.filter(version=scenario).delete()[0]
                    self.stdout.write(f"Deleted {deleted_replenishment} replenishment and {deleted_production} production records")

                # PERFORMANCE OPTIMIZATION: Pre-load all lookup data to avoid repeated DB queries
                self.stdout.write("Loading master data for performance optimization...")
                
                # Pre-load manufacturing operations for all products
                self.manufacturing_cache = {}
                foundry_sites = {'XUZ1', 'MTJ1', 'COI2', 'MER1', 'WOD1', 'WUN1', 'CHI1'}
                foundry_keywords = ['pour', 'moulding', 'casting']
                
                mom_queryset = MasterDataEpicorMethodOfManufacturingModel.objects.all()
                if single_product:
                    mom_queryset = mom_queryset.filter(ProductKey=single_product)
                
                for mom in mom_queryset:
                    product = mom.ProductKey
                    if product not in self.manufacturing_cache:
                        self.manufacturing_cache[product] = {'has_mom': True, 'has_foundry_ops': False}
                    
                    # Check if this operation is foundry-related
                    operation_desc = (mom.OperationDesc or '').lower()
                    if any(keyword in operation_desc for keyword in foundry_keywords):
                        self.manufacturing_cache[product]['has_foundry_ops'] = True

                # Pre-load assignment data
                self.manual_assignments_cache = {}
                for ma in MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario):
                    if ma.Product and ma.Site and ma.ShippingDate:
                        key = (ma.Product.Product, ma.ShippingDate.year, ma.ShippingDate.month)
                        self.manual_assignments_cache[key] = ma.Site.SiteName

                self.order_book_cache = {}
                for ob in MasterDataOrderBook.objects.filter(version=scenario):
                    self.order_book_cache[ob.productkey] = ob.site

                self.production_history_cache = {}
                for ph in MasterDataHistoryOfProductionModel.objects.filter(version=scenario):
                    self.production_history_cache[ph.Product] = ph.Foundry

                # Pre-load incoterms
                self.incoterms_cache = {}
                for it in MasterdataIncoTermsModel.objects.filter(version=scenario):
                    self.incoterms_cache[it.CustomerCode] = it.Incoterm.IncoTermCaregory

                # Pre-load freight data
                self.freight_cache = {}
                for f in MasterDataFreightModel.objects.filter(version=scenario):
                    key = (f.ForecastRegion.Forecast_region, f.ManufacturingSite.SiteName)
                    self.freight_cache[key] = {
                        'plant_to_port': f.PlantToDomesticPortDays,
                        'ocean': f.OceanFreightDays,
                        'port_to_customer': f.PortToCustomerDays
                    }

                # Pre-load cast to despatch
                self.cast_despatch_cache = {}
                for c in MasterDataCastToDespatchModel.objects.filter(version=scenario):
                    self.cast_despatch_cache[c.Foundry.SiteName] = c.CastToDespatchDays

                self.stdout.write("Master data loaded - performance optimizations active")

                # STEP 1: Load and process forecast data
                self.stdout.write("STEP 1: Loading forecast data...")
                
                forecast_filter = {'version': scenario}
                if single_product:
                    forecast_filter['Product'] = single_product

                forecast_queryset = SMART_Forecast_Model.objects.filter(**forecast_filter).values(
                    'Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code', 'Qty'
                )
                
                forecast_df = pd.DataFrame(list(forecast_queryset))
                
                if forecast_df.empty:
                    self.stdout.write("No forecast data found.")
                    return

                self.stdout.write(f"Loaded {len(forecast_df)} forecast records")

                # Extract warehouse location (last 4 characters after delimiter)
                forecast_df['warehouse_location'] = forecast_df['Location'].apply(self.extract_location_from_forecast)

                # Get inventory snapshot date (same across all products in scenario)
                snapshot_date = MasterDataInventory.objects.filter(
                    version=scenario
                ).aggregate(snapshot_date=Min('date_of_snapshot'))['snapshot_date']
                
                if not snapshot_date:
                    self.stdout.write(self.style.ERROR("No inventory snapshot date found"))
                    return

                self.stdout.write(f"Using inventory snapshot date: {snapshot_date}")

                # STEP 2: Calculate shipping dates and assign production sites
                self.stdout.write("STEP 2: Calculating shipping dates and assigning production sites...")
                
                processed_rows = []
                for idx, row in forecast_df.iterrows():
                    # First assign production site (needed for shipping date calculation)
                    production_site = self.assign_production_site_optimized(
                        row['Product'], 
                        row['Period_AU'], 
                        scenario
                    )
                    
                    if not production_site:
                        continue  # Skip if no production site found

                    # Calculate shipping date
                    shipping_date = self.calculate_shipping_date_optimized(
                        row['Period_AU'],
                        row['Customer_code'],
                        row['Forecast_Region'],
                        production_site,
                        snapshot_date
                    )

                    processed_rows.append({
                        'Product': row['Product'],
                        'warehouse_location': row['warehouse_location'],
                        'Period_AU': row['Period_AU'],
                        'Forecast_Region': row['Forecast_Region'],
                        'Customer_code': row['Customer_code'],
                        'Qty': row['Qty'],
                        'production_site': production_site,
                        'shipping_date': shipping_date
                    })

                processed_df = pd.DataFrame(processed_rows)
                self.stdout.write(f"Processed {len(processed_df)} records with production sites assigned")

                # STEP 3: Aggregate by Product, Location, Qty, shipping date (monthly)
                self.stdout.write("STEP 3: Aggregating forecast data monthly...")
                
                # Add year-month for grouping
                processed_df['shipping_year_month'] = processed_df['shipping_date'].apply(
                    lambda x: x.replace(day=1)
                )

                # Aggregate
                aggregated_df = processed_df.groupby([
                    'Product', 'warehouse_location', 'shipping_year_month', 'production_site'
                ], as_index=False).agg({
                    'Qty': 'sum',
                    'shipping_date': 'first',  # Keep one shipping date per group
                    'Forecast_Region': 'first',
                    'Customer_code': 'first'
                })

                # Sort chronologically
                aggregated_df = aggregated_df.sort_values(['Product', 'warehouse_location', 'shipping_year_month'])
                self.stdout.write(f"Aggregated to {len(aggregated_df)} monthly forecast records")

                # STEP 4: Already done - production site assignment
                
                # STEP 5: Inventory deduction and replenishment calculation
                self.stdout.write("STEP 5: Processing inventory deduction and replenishment calculation...")
                
                # Load inventory data
                inventory_df = pd.DataFrame(list(
                    MasterDataInventory.objects.filter(version=scenario).values(
                        'product', 'site__SiteName', 'onhandstock_qty', 'intransitstock_qty', 'wip_stock_qty'
                    )
                ))

                # Load safety stock data
                safety_stock_map = {}
                for ss in MasterDataSafetyStocks.objects.filter(version=scenario):
                    key = (ss.Plant, ss.PartNum)
                    safety_stock_map[key] = {
                        'minimum_qty': float(ss.MinimumQty or 0),
                        'safety_qty': float(ss.SafetyQty or 0)
                    }

                self.stdout.write(f"Loaded safety stock data for {len(safety_stock_map)} combinations")

                # Initialize inventory tracking by location
                location_inventory = defaultdict(lambda: defaultdict(lambda: {'onhand': 0, 'intransit': 0}))
                
                if not inventory_df.empty:
                    for _, inv_row in inventory_df.iterrows():
                        location_key = inv_row['site__SiteName']
                        product_key = inv_row['product']
                        location_inventory[location_key][product_key]['onhand'] += inv_row['onhandstock_qty'] or 0
                        location_inventory[location_key][product_key]['intransit'] += inv_row['intransitstock_qty'] or 0

                self.stdout.write(f"Loaded inventory data for {len(location_inventory)} locations")

                # Process replenishment chronologically by month
                replenishment_records = []
                unique_months = sorted(aggregated_df['shipping_year_month'].unique())
                
                for month in unique_months:
                    self.stdout.write(f"Processing month: {month.strftime('%Y-%m')}")
                    month_data = aggregated_df[aggregated_df['shipping_year_month'] == month]
                    
                    for _, row in month_data.iterrows():
                        product = row['Product']
                        warehouse_location = row['warehouse_location']
                        production_site = row['production_site']
                        demand_qty = row['Qty']
                        shipping_date = row['shipping_date']
                        
                        # Get safety stock requirements for this location-product combination
                        safety_stock_key = (warehouse_location, product)
                        safety_stock_data = safety_stock_map.get(safety_stock_key, {'minimum_qty': 0, 'safety_qty': 0})
                        required_closing_stock = safety_stock_data['minimum_qty'] + safety_stock_data['safety_qty']

                        # Check if production site matches warehouse location (Step 6)
                        if production_site == warehouse_location:
                            # Don't deduct inventory if production site matches location
                            remaining_qty = demand_qty
                        else:
                            # Deduct available inventory
                            current_inventory = location_inventory[warehouse_location][product]
                            total_available = current_inventory['onhand'] + current_inventory['intransit']
                            
                            # Calculate usable stock (must leave required closing stock)
                            usable_stock = max(0, total_available - required_closing_stock)
                            stock_used = min(demand_qty, usable_stock)
                            remaining_qty = demand_qty - stock_used
                            
                            # Update inventory
                            if stock_used > 0:
                                intransit_used = min(stock_used, current_inventory['intransit'])
                                onhand_used = stock_used - intransit_used
                                
                                current_inventory['intransit'] -= intransit_used
                                current_inventory['onhand'] -= onhand_used

                        # Calculate replenishment needed
                        current_stock_after_demand = (
                            location_inventory[warehouse_location][product]['onhand'] + 
                            location_inventory[warehouse_location][product]['intransit']
                        )
                        
                        # Ensure we maintain minimum stock level
                        min_stock_needed = max(remaining_qty, required_closing_stock - current_stock_after_demand)
                        replenishment_needed = max(0, min_stock_needed)

                        if replenishment_needed > 0:
                            replenishment_records.append({
                                'Product': product,
                                'Location': warehouse_location,
                                'Site': production_site,
                                'ShippingDate': shipping_date,
                                'ReplenishmentQty': replenishment_needed
                            })

                self.stdout.write(f"Generated {len(replenishment_records)} replenishment records")

                # STEP 7: Calculate pouring dates for replenishment records
                self.stdout.write("STEP 7: Calculating pouring dates...")
                
                for record in replenishment_records:
                    record['PouringDate'] = self.calculate_pouring_date_optimized(
                        record['ShippingDate'],
                        record['Site'],
                        snapshot_date
                    )

                # Create replenishment model instances
                product_map = {p.Product: p for p in MasterDataProductModel.objects.all()}
                plant_map = {p.SiteName: p for p in MasterDataPlantModel.objects.all()}

                replenishment_instances = []
                for record in replenishment_records:
                    product_instance = product_map.get(record['Product'])
                    site_instance = plant_map.get(record['Site'])
                    
                    if product_instance and site_instance:
                        replenishment_instances.append(CalcualtedReplenishmentModel(
                            version=scenario,
                            Product=product_instance,
                            Location=record['Location'],
                            Site=site_instance,
                            ShippingDate=record['ShippingDate'],
                            ReplenishmentQty=record['ReplenishmentQty']
                        ))

                # Bulk create replenishment records
                if replenishment_instances:
                    CalcualtedReplenishmentModel.objects.bulk_create(replenishment_instances, batch_size=1000)
                    self.stdout.write(f"Created {len(replenishment_instances)} replenishment records")

                # STEP 8: Calculate production requirements
                self.stdout.write("STEP 8: Calculating production requirements...")
                
                # Aggregate replenishment by site and pouring date
                production_df = pd.DataFrame([{
                    'product': r['Product'],
                    'site': r['Site'],
                    'pouring_date': r['PouringDate'],
                    'replenishment_qty': r['ReplenishmentQty']
                } for r in replenishment_records])

                if not production_df.empty:
                    # Group by site, product, and pouring month
                    production_df['pouring_year_month'] = production_df['pouring_date'].apply(
                        lambda x: x.replace(day=1)
                    )

                    production_aggregated = production_df.groupby([
                        'site', 'product', 'pouring_year_month'
                    ], as_index=False).agg({
                        'replenishment_qty': 'sum',
                        'pouring_date': 'first'
                    })

                    # Load production site inventory
                    production_inventory = defaultdict(lambda: defaultdict(lambda: {'onhand': 0, 'intransit': 0, 'wip': 0}))
                    
                    for _, inv_row in inventory_df.iterrows():
                        site_key = inv_row['site__SiteName']
                        product_key = inv_row['product']
                        production_inventory[site_key][product_key]['onhand'] += inv_row['onhandstock_qty'] or 0
                        production_inventory[site_key][product_key]['intransit'] += inv_row['intransitstock_qty'] or 0
                        production_inventory[site_key][product_key]['wip'] += inv_row['wip_stock_qty'] or 0

                    # Calculate production requirements
                    production_instances = []
                    
                    for _, row in production_aggregated.iterrows():
                        site = row['site']
                        product = row['product']
                        replenishment_qty = row['replenishment_qty']
                        pouring_date = row['pouring_date']
                        
                        # Get current inventory at production site
                        current_inventory = production_inventory[site][product]
                        total_available = (current_inventory['onhand'] + 
                                         current_inventory['intransit'] + 
                                         current_inventory['wip'])
                        
                        # Get safety stock for production site
                        production_safety_key = (site, product)
                        production_safety_data = safety_stock_map.get(production_safety_key, {'minimum_qty': 0, 'safety_qty': 0})
                        required_safety_stock = production_safety_data['minimum_qty'] + production_safety_data['safety_qty']
                        
                        # Calculate net production requirement
                        net_requirement = replenishment_qty - total_available
                        production_needed = max(0, max(net_requirement, required_safety_stock))

                        if production_needed > 0:
                            product_instance = product_map.get(product)
                            site_instance = plant_map.get(site)
                            
                            if product_instance and site_instance:
                                # Get product details for additional fields
                                product_group = product_instance.ProductGroup or ''
                                parent_product_group = product_instance.ParentProductGroup or ''
                                
                                production_instances.append(CalculatedProductionModel(
                                    version=scenario,
                                    product=product_instance,
                                    site=site_instance,
                                    pouring_date=pouring_date,
                                    production_quantity=production_needed,
                                    tonnes=0,  # To be calculated later if needed
                                    product_group=product_group,
                                    parent_product_group=parent_product_group,
                                    price_aud=0,  # To be calculated later
                                    cogs_aud=0,  # To be calculated later
                                    revenue_aud=0  # To be calculated later
                                ))

                    # Bulk create production records
                    if production_instances:
                        CalculatedProductionModel.objects.bulk_create(production_instances, batch_size=1000)
                        self.stdout.write(f"Created {len(production_instances)} production records")
                    else:
                        self.stdout.write("No production records created")
                else:
                    self.stdout.write("No replenishment data available for production calculation")

                # Final summary
                final_replenishment_count = CalcualtedReplenishmentModel.objects.filter(version=scenario).count()
                final_production_count = CalculatedProductionModel.objects.filter(version=scenario).count()
                
                self.stdout.write(self.style.SUCCESS(f"Calculation completed successfully!"))
                self.stdout.write(self.style.SUCCESS(f"Total replenishment records: {final_replenishment_count}"))
                self.stdout.write(self.style.SUCCESS(f"Total production records: {final_production_count}"))
                
                # Manufacturing validation summary
                if hasattr(self, 'blocked_assignments'):
                    total_blocked = sum(self.blocked_assignments.values())
                    if total_blocked > 0:
                        self.stdout.write(f"\n--- Manufacturing Validation Summary ---")
                        self.stdout.write(f"Total assignments blocked: {total_blocked}")
                        self.stdout.write(f"  Manual assignments blocked: {self.blocked_assignments['manual']}")
                        self.stdout.write(f"  Order book assignments blocked: {self.blocked_assignments['order_book']}")
                        self.stdout.write(f"  Production history assignments blocked: {self.blocked_assignments['production_history']}")
                        self.stdout.write(f"Reason: Products with MOM records but no foundry operations cannot be assigned to foundries")
                    else:
                        self.stdout.write(f"\n--- Manufacturing Validation Summary ---")
                        self.stdout.write(f"No assignments were blocked by manufacturing validation")

        finally:
            # Always release the execution lock
            cache.delete(cache_key)
