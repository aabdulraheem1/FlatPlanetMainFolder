from django.core.management.base import BaseCommand
from django.core.cache import cache
from website.models import (
    scenarios, 
    SMART_Forecast_Model, 
    CalcualtedReplenishmentModel,
    MasterDataPlantModel, 
    MasterDataProductModel,
    MasterDataOrderBook,
    MasterDataHistoryOfProductionModel,
    MasterDataEpicorSupplierMasterDataModel,
    MasterDataManuallyAssignProductionRequirement,
    MasterDataInventory,
    MasterDataSafetyStocks,
    MasterDataFreightModel,
    MasterDataCastToDespatchModel,
    MasterdataIncoTermsModel,
    MasterDataIncotTermTypesModel,
    MasterDataCustomersModel,
    MasterDataEpicorMethodOfManufacturingModel
)
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
import traceback

class Command(BaseCommand):
    help = "Clean replenishment calculation following proper monthly inventory flow"

    def add_arguments(self, parser):
        parser.add_argument('version', type=str, help='The scenario version to process')
        parser.add_argument('--product', type=str, help='Optional: Process only this specific product')

    def extract_location_from_forecast(self, location_string):
        """Extract location code from forecast location string.
        
        Examples:
        - 'CA01-DTC1' -> 'DTC1'
        - 'AU03_POB1' -> 'POB1'
        - 'DTC1' -> 'DTC1'
        """
        if '-' in location_string:
            return location_string.split('-')[1]
        elif '_' in location_string:
            return location_string.split('_')[1]
        else:
            # If no separator, assume it's already the location code
            return location_string

    def handle(self, *args, **kwargs):
        version = kwargs['version']
        product_filter = kwargs.get('product')  # Optional product filter
        
        if product_filter:
            self.stdout.write(f"Processing only product: {product_filter}")
        
        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario '{version}' not found"))
            return

        # Prevent concurrent execution (include product filter in cache key if specified)
        cache_key = f"replenishment_calculation_{version}_{product_filter or 'all'}"
        if cache.get(cache_key):
            self.stdout.write(self.style.ERROR("Another replenishment calculation is already running"))
            return
        
        cache.set(cache_key, True, timeout=3600)

        try:
            self.stdout.write(f"Starting clean replenishment calculation for scenario: {version}")
            
            # Step 0: Clean up any existing replenishment records for this version
            if product_filter:
                # Only delete records for the specific product
                existing_count = CalcualtedReplenishmentModel.objects.filter(
                    version=scenario, 
                    Product__Product=product_filter
                ).count()
                if existing_count > 0:
                    CalcualtedReplenishmentModel.objects.filter(
                        version=scenario, 
                        Product__Product=product_filter
                    ).delete()
                    self.stdout.write(f"Deleted {existing_count:,} existing replenishment records for product '{product_filter}' in version '{version}'")
                else:
                    self.stdout.write(f"No existing replenishment records found for product '{product_filter}'")
            else:
                # Delete all records for the version
                existing_count = CalcualtedReplenishmentModel.objects.filter(version=scenario).count()
                if existing_count > 0:
                    CalcualtedReplenishmentModel.objects.filter(version=scenario).delete()
                    self.stdout.write(f"Deleted {existing_count:,} existing replenishment records for version '{version}'")
                else:
                    self.stdout.write("No existing replenishment records found to delete")
            
            # Step 1: Load all master data
            self.load_master_data(scenario)
            
            # Step 2: Process SMART forecast with site assignment and date adjustments
            self.process_smart_forecast(scenario, product_filter)
            
            # Step 3: Calculate monthly replenishment needs
            self.calculate_monthly_replenishment(scenario)
            
            self.stdout.write(self.style.SUCCESS("Replenishment calculation completed successfully"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error in replenishment calculation: {str(e)}"))
            self.stdout.write(self.style.ERROR(traceback.format_exc()))
        finally:
            cache.delete(cache_key)

    def load_master_data(self, scenario):
        """Load all required master data"""
        self.stdout.write("Loading master data...")
        
        # Load products with casting/foundry operations (expanded criteria)
        casting_keywords = ['cast', 'pour', 'mould', 'coulée', 'foundry', 'shakeout', 'core making', 'cool time', 'pouring']
        casting_products = set()
        
        for keyword in casting_keywords:
            products = MasterDataEpicorMethodOfManufacturingModel.objects.filter(
                OperationDesc__icontains=keyword
            ).values_list('ProductKey', flat=True)
            casting_products.update(products)
        
        # If no casting products found, include ALL products to avoid filtering everything out
        if not casting_products:
            self.stdout.write("No casting operations found, including all products")
            product_queryset = MasterDataProductModel.objects.all()
        else:
            product_queryset = MasterDataProductModel.objects.filter(Product__in=casting_products)
            
        self.product_map = {p.Product: p for p in product_queryset}
        self.stdout.write(f"Loaded {len(self.product_map)} products with casting operations")
        
        # Load site assignment priorities
        self.manual_assignments = {
            (ma.Product.Product, ma.ShippingDate): ma.Site
            for ma in MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario)
            if ma.Product and ma.Site and ma.ShippingDate
        }
        
        self.order_book = {
            (ob.productkey,): ob.site 
            for ob in MasterDataOrderBook.objects.filter(version=scenario)
        }
        
        self.production_history = {
            (ph.Product,): ph.Foundry
            for ph in MasterDataHistoryOfProductionModel.objects.filter(version=scenario)
        }
        
        self.supplier_map = {
            (sm.PartNum,): sm.Plant
            for sm in MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario)
            if sm.SourceType == 'Make'
        }
        
        # Load plants
        self.plants = {p.SiteName: p for p in MasterDataPlantModel.objects.all()}
        
        # Load freight and lead time data
        self.freight_map = {
            (f.ForecastRegion.Forecast_region, f.ManufacturingSite.SiteName): 
            (f.PlantToDomesticPortDays + f.OceanFreightDays + f.PortToCustomerDays)
            for f in MasterDataFreightModel.objects.filter(version=scenario)
        }
        
        self.cast_to_despatch = {
            c.Foundry.SiteName: c.CastToDespatchDays
            for c in MasterDataCastToDespatchModel.objects.filter(version=scenario)
        }
        
        # Load Incoterms
        self.incoterms = {}
        for it in MasterdataIncoTermsModel.objects.filter(version=scenario):
            self.incoterms[it.CustomerCode] = it.Incoterm.IncoTerm
        
        # Load safety stocks
        self.safety_stocks = {
            (ss.Plant, ss.PartNum): {
                'minimum_qty': float(ss.MinimumQty or 0),
                'safety_qty': float(ss.SafetyQty or 0)
            }
            for ss in MasterDataSafetyStocks.objects.filter(version=scenario)
        }
        
        # Load opening inventory
        self.opening_inventory = {}
        for inv in MasterDataInventory.objects.filter(version=scenario):
            # Use the site_id directly from inventory, not product code
            location = inv.site_id
            key = (inv.product, location)
            if key not in self.opening_inventory:
                self.opening_inventory[key] = {
                    'onhand': 0,
                    'intransit': 0,
                    'snapshot_date': inv.date_of_snapshot
                }
            self.opening_inventory[key]['onhand'] += inv.onhandstock_qty
            self.opening_inventory[key]['intransit'] += inv.intransitstock_qty
        
        self.stdout.write(f"Loaded master data:")
        self.stdout.write(f"  - Manual assignments: {len(self.manual_assignments)}")
        self.stdout.write(f"  - Order book: {len(self.order_book)}")
        self.stdout.write(f"  - Production history: {len(self.production_history)}")
        self.stdout.write(f"  - Supplier mappings: {len(self.supplier_map)}")
        self.stdout.write(f"  - Safety stocks: {len(self.safety_stocks)}")
        self.stdout.write(f"  - Opening inventory: {len(self.opening_inventory)}")

    def get_assigned_site(self, product, period_date):
        """Get assigned site using priority logic"""
        # Priority 1: Manual assignment
        manual_key = (product, period_date)
        if manual_key in self.manual_assignments:
            return self.manual_assignments[manual_key].SiteName
        
        # Priority 2: Order book
        order_key = (product,)
        if order_key in self.order_book:
            return self.order_book[order_key]
        
        # Priority 3: Production history
        history_key = (product,)
        if history_key in self.production_history:
            return self.production_history[history_key]
        
        # Priority 4: Supplier mapping
        supplier_key = (product,)
        if supplier_key in self.supplier_map:
            return self.supplier_map[supplier_key]
        
        return None

    def calculate_shipping_date(self, forecast_record, assigned_site):
        """Calculate shipping date based on Incoterms and freight"""
        base_date = forecast_record.Period_AU
        
        # Get customer incoterm
        incoterm = self.incoterms.get(forecast_record.Customer_code, 'EXW')  # Default EXW
        
        # Get freight days
        freight_key = (forecast_record.Forecast_Region, assigned_site)
        freight_days = self.freight_map.get(freight_key, 0)
        
        # Get cast-to-despatch days
        cast_days = self.cast_to_despatch.get(assigned_site, 0)
        
        # Calculate total lead time
        if incoterm in ['EXW', 'FCA', 'FOB']:
            total_days = cast_days
        else:  # CIF, DDP, etc.
            total_days = cast_days + freight_days
        
        # Calculate shipping date
        shipping_date = base_date - timedelta(days=total_days)
        
        # Ensure shipping date is not before snapshot date
        # Extract plant code from compound location using helper function
        location = self.extract_location_from_forecast(forecast_record.Location)
        inventory_key = (forecast_record.Product, location)
        if inventory_key in self.opening_inventory:
            snapshot_date = self.opening_inventory[inventory_key]['snapshot_date']
            if shipping_date < snapshot_date:
                shipping_date = snapshot_date
        
        return shipping_date

    def process_smart_forecast(self, scenario, product_filter=None):
        """Process SMART forecast with deduplication and site assignment"""
        self.stdout.write("Processing SMART forecast...")
        
        # Load and process forecast records with deduplication
        forecast_query = SMART_Forecast_Model.objects.filter(version=scenario)
        
        # Apply product filter if specified
        if product_filter:
            forecast_query = forecast_query.filter(Product=product_filter)
            self.stdout.write(f"Filtering forecast data for product: {product_filter}")
        
        forecast_records = forecast_query
        
        # First pass: Deduplicate forecast data by summing quantities for same product-location-period
        deduplicated_forecast = defaultdict(float)
        forecast_metadata = {}  # Store metadata for each unique combination
        
        duplicate_count = 0
        total_records = 0
        
        for record in forecast_records:
            if record.Product not in self.product_map:
                continue
                
            total_records += 1
            
            # Extract plant code from compound location using helper function
            location = self.extract_location_from_forecast(record.Location)
            
            # Create unique key for deduplication
            dedup_key = (record.Product, location, record.Period_AU)
            
            # Sum quantities for duplicates
            if dedup_key in deduplicated_forecast:
                duplicate_count += 1
            
            deduplicated_forecast[dedup_key] += record.Qty or 0
            
            # Store metadata (using the first occurrence)
            if dedup_key not in forecast_metadata:
                forecast_metadata[dedup_key] = {
                    'original_location': record.Location,
                    'customer_code': record.Customer_code,
                    'forecast_region': record.Forecast_Region,
                    'period_au': record.Period_AU
                }
        
        self.stdout.write(f"Deduplication: {total_records} records -> {len(deduplicated_forecast)} unique combinations")
        self.stdout.write(f"Removed {duplicate_count} duplicate records")
        
        # Second pass: Process deduplicated data
        self.monthly_demand = defaultdict(lambda: defaultdict(float))
        self.assigned_sites = {}
        
        processed = 0
        for (product, location, period_au), qty in deduplicated_forecast.items():
            # Get assigned site
            assigned_site = self.get_assigned_site(product, period_au)
            if not assigned_site:
                continue
            
            # Create a mock record object for calculate_shipping_date
            class MockRecord:
                def __init__(self, product, location, period_au, customer_code, forecast_region):
                    self.Product = product
                    self.Location = location
                    self.Period_AU = period_au
                    self.Customer_code = customer_code
                    self.Forecast_Region = forecast_region
            
            metadata = forecast_metadata[(product, location, period_au)]
            mock_record = MockRecord(
                product, 
                metadata['original_location'], 
                period_au, 
                metadata['customer_code'],
                metadata['forecast_region']
            )
            
            # Calculate shipping date
            shipping_date = self.calculate_shipping_date(mock_record, assigned_site)
            
            # Group by month
            month_key = shipping_date.replace(day=1)
            demand_key = (product, location)
            
            self.monthly_demand[month_key][demand_key] += qty
            
            # Store site assignment
            if demand_key not in self.assigned_sites:
                self.assigned_sites[demand_key] = assigned_site
            
            processed += 1
            
            if processed % 1000 == 0:
                self.stdout.write(f"Processed {processed} deduplicated records...")
        
        self.stdout.write(f"Processed {processed} deduplicated forecast records")
        self.stdout.write(f"Created {len(self.monthly_demand)} monthly periods")

    def calculate_monthly_replenishment(self, scenario):
        """Calculate replenishment using month-by-month inventory tracking"""
        self.stdout.write("Calculating monthly replenishment...")
        
        # Get all product-location combinations
        all_product_locations = set()
        for month_demands in self.monthly_demand.values():
            all_product_locations.update(month_demands.keys())
        
        # Initialize running inventory for each product-location
        running_inventory = {}
        for product, location in all_product_locations:
            inv_key = (product, location)
            if inv_key in self.opening_inventory:
                running_inventory[inv_key] = (
                    self.opening_inventory[inv_key]['onhand'] + 
                    self.opening_inventory[inv_key]['intransit']
                )
            else:
                running_inventory[inv_key] = 0
        
        # Process months in chronological order
        replenishment_records = []
        months = sorted(self.monthly_demand.keys())
        
        for month in months:
            month_demands = self.monthly_demand[month]
            self.stdout.write(f"Processing month: {month.strftime('%Y-%m')}")
            
            for (product, location), demand_qty in month_demands.items():
                # Get safety stock requirement
                assigned_site = self.assigned_sites.get((product, location))
                if not assigned_site:
                    continue
                
                # CRITICAL FIX: Safety stock is for the consumption location, not production site
                safety_key = (location, product)  # Use location instead of assigned_site
                safety_data = self.safety_stocks.get(safety_key, {'minimum_qty': 0, 'safety_qty': 0})
                required_closing_stock = safety_data['minimum_qty'] + safety_data['safety_qty']
                
                # Get current inventory
                inv_key = (product, location)
                current_inventory = running_inventory.get(inv_key, 0)
                
                # Calculate net requirement for this month
                # Net requirement = demand + safety stock - current inventory
                net_requirement = demand_qty + required_closing_stock - current_inventory
                
                if net_requirement > 0:
                    # Need replenishment to bring inventory to zero plus safety stock
                    replenishment_qty = net_requirement
                    
                    # Create replenishment record
                    product_instance = self.product_map[product]
                    site_instance = self.plants.get(assigned_site)
                    
                    if product_instance and site_instance:
                        replenishment_records.append(
                            CalcualtedReplenishmentModel(
                                version=scenario,
                                Product=product_instance,
                                Location=location,
                                Site=site_instance,
                                ShippingDate=month,
                                ReplenishmentQty=replenishment_qty
                            )
                        )
                    
                    # Update running inventory (add replenishment)
                    running_inventory[inv_key] = current_inventory + replenishment_qty
                
                # Deduct demand from running inventory for next month
                running_inventory[inv_key] = max(0, running_inventory[inv_key] - demand_qty)
        
        # Bulk create replenishment records
        if replenishment_records:
            CalcualtedReplenishmentModel.objects.bulk_create(replenishment_records, batch_size=1000)
            self.stdout.write(f"Created {len(replenishment_records)} replenishment records")
        else:
            self.stdout.write("No replenishment records created")
        
        # Final summary
        final_count = CalcualtedReplenishmentModel.objects.filter(version=scenario).count()
        self.stdout.write(f"Total replenishment records in database: {final_count}")
