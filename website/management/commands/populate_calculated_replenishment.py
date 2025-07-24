# OLD POPULATE_CALCULATED_REPLENISHMENT COMMAND - DISABLED
# This file has been replaced by populate_calculated_replenishment_clean.py
# The clean version provides better performance and fixes quantity inflation issues
# Keeping this file commented out for reference/rollback purposes only

# from django.core.management.base import BaseCommand
# from django.db.models import Sum
# from django.core.cache import cache
# from django.db import transaction
# from website.models import (
#     scenarios,
#     SMART_Forecast_Model,
#     MasterDataInventory,
#     MasterDataProductModel,
#     MasterDataOrderBook,
#     MasterDataHistoryOfProductionModel,
#     MasterDataPlantModel,
#     CalcualtedReplenishmentModel,
#     MasterDataEpicorSupplierMasterDataModel,
#     MasterDataFreightModel,
#     MasterdataIncoTermsModel,
#     MasterDataManuallyAssignProductionRequirement,
#     MasterDataEpicorMethodOfManufacturingModel,
#     MasterDataSafetyStocks,  # Add this import
# )
# import pandas as pd
# from datetime import timedelta
# from collections import Counter

# class Command(BaseCommand):
#     help = "Populate data in CalcualtedReplenishmentModel from SMART_Forecast_Model with inventory depletion tracking, Incoterm logic, and safety stock requirements"

#     def add_arguments(self, parser):
#         parser.add_argument(
#             'version',
#             type=str,
#             help="The version of the scenario to populate data for.",
#         )

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._casting_products_cache = None

#     def _initialize_casting_products_cache(self):
#         """Load all products that have casting operations for fast lookup."""
#         if self._casting_products_cache is not None:
#             return
        
#         casting_operations = ['pour', 'casting', 'coulée']
#         manufacturing_records = MasterDataEpicorMethodOfManufacturingModel.objects.values('ProductKey', 'OperationDesc')
        
#         casting_products = set()
#         for record in manufacturing_records:
#             if record['OperationDesc']:
#                 operation_desc_lower = record['OperationDesc'].lower()
#                 if any(casting_op in operation_desc_lower for casting_op in casting_operations):
#                     casting_products.add(record['ProductKey'])
        
#         self._casting_products_cache = casting_products

#     def _can_product_be_assigned_to_foundry_sites(self, product):
#         """Check if a product can be assigned to foundry sites.
        
#         Rules:
#         1. If product doesn't exist in manufacturing operations at all -> Allow foundry assignment
#         2. If product exists in manufacturing operations -> Only allow if it has casting operations
#         """
#         if self._casting_products_cache is None:
#             self._initialize_casting_products_cache()
        
#         # Check if product exists in manufacturing operations at all
#         manufacturing_exists = MasterDataEpicorMethodOfManufacturingModel.objects.filter(
#             ProductKey=product
#         ).exists()
        
#         if not manufacturing_exists:
#             # Product doesn't exist in manufacturing operations, allow foundry assignment
#             return True
        
#         # Product exists in manufacturing operations, check if it has casting operations
#         return product in self._casting_products_cache

#     def _transform_location(self, location):
#         """Transform location strings by extracting the site code."""
#         if location:
#             if "_" in location:
#                 return location.split("_", 1)[1][:4]
#             elif "-" in location:
#                 return location.split("-", 1)[1][:4]
#         return location

#     def handle(self, *args, **kwargs):
#         version = kwargs['version']
        
#         try:
#             scenario = scenarios.objects.get(version=version)
#         except scenarios.DoesNotExist:
#             self.stdout.write(self.style.ERROR(f"Scenario '{version}' does not exist"))
#             return

#         # Check for concurrent execution
#         cache_key = f"replenishment_running_{scenario.version.replace(' ', '_')}"
#         if cache.get(cache_key):
#             self.stdout.write(self.style.ERROR(f"Replenishment calculation already running for version {version}"))
#             return

#         # Set the execution lock
#         cache.set(cache_key, True, timeout=3600)  # 1 hour timeout

#         try:
#             with transaction.atomic():
#                 # Initialize casting products cache
#                 self._initialize_casting_products_cache()
#                 self.stdout.write(f"Loaded {len(self._casting_products_cache)} products with casting operations")

#                 # Delete existing records for this version
#                 deleted_count = CalcualtedReplenishmentModel.objects.filter(version=scenario.version).delete()[0]
#                 self.stdout.write(f"Deleted {deleted_count} existing records")

#                 # Load forecast data
#                 forecast_df = pd.DataFrame(list(
#                     SMART_Forecast_Model.objects.filter(version=scenario.version).values(
#                         'Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code', 'Qty'
#                     )
#                 ))
                
#                 if forecast_df.empty:
#                     self.stdout.write("No forecast data found.")
#                     return

#                 self.stdout.write(f"Loaded {len(forecast_df)} forecast records")

#                 # Transform locations
#                 forecast_df['Location'] = forecast_df['Location'].apply(self._transform_location)

#                 # Aggregate total_qty per group
#                 forecast_df = forecast_df.groupby(['Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code'], as_index=False)['Qty'].sum()
#                 forecast_df.rename(columns={'Qty': 'total_qty'}, inplace=True)

#                 # Sort by Period_AU to process chronologically
#                 forecast_df = forecast_df.sort_values(['Product', 'Location', 'Period_AU'])
#                 self.stdout.write(f"Aggregated to {len(forecast_df)} unique forecast combinations")

#                 # Load inventory data
#                 inventory_df = pd.DataFrame(list(
#                     MasterDataInventory.objects.filter(version=scenario.version).values(
#                         'product', 'site_id', 'onhandstock_qty', 'intransitstock_qty', 'date_of_snapshot'
#                     )
#                 ))
                
#                 if not inventory_df.empty:
#                     inventory_agg = inventory_df.groupby(['product', 'site_id'], as_index=False).agg({
#                         'onhandstock_qty': 'sum',
#                         'intransitstock_qty': 'sum',
#                         'date_of_snapshot': 'min'
#                     })
#                     self.stdout.write(f"Loaded inventory data for {len(inventory_agg)} product-location combinations")
#                 else:
#                     inventory_agg = pd.DataFrame(columns=['product', 'site_id', 'onhandstock_qty', 'intransitstock_qty', 'date_of_snapshot'])
#                     self.stdout.write("No inventory data found")

#                 # Load safety stock data
#                 safety_stock_map = {
#                     (ss.Plant, ss.PartNum): {
#                         'minimum_qty': float(ss.MinimumQty or 0),
#                         'safety_qty': float(ss.SafetyQty or 0)
#                     }
#                     for ss in MasterDataSafetyStocks.objects.filter(version=scenario)
#                 }
#                 self.stdout.write(f"Loaded safety stock data for {len(safety_stock_map)} plant-product combinations")

#                 # Prepare lookup dictionaries
#                 product_map = {p.Product: p for p in MasterDataProductModel.objects.all()}
#                 plant_map = {p.SiteName: p for p in MasterDataPlantModel.objects.all()}

#                 order_book_map = {
#                     (ob.version.version, ob.productkey): ob.site
#                     for ob in MasterDataOrderBook.objects.filter(version=scenario).exclude(site__isnull=True).exclude(site__exact='')
#                 }
#                 production_map = {
#                     (prod.version.version, prod.Product): prod.Foundry
#                     for prod in MasterDataHistoryOfProductionModel.objects.filter(version=scenario).exclude(Foundry__isnull=True).exclude(Foundry__exact='')
#                 }
#                 supplier_map = {
#                     (sup.version.version, sup.PartNum): sup.VendorID
#                     for sup in MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario).exclude(VendorID__isnull=True).exclude(VendorID__exact='')
#                 }

#                 # Incoterm and freight mapping
#                 incoterm_map = {
#                     (i.version.version, i.CustomerCode): i.Incoterm
#                     for i in MasterdataIncoTermsModel.objects.filter(version=scenario).exclude(Incoterm__isnull=True)
#                 }
                
#                 freight_map = {
#                     (f.version.version, f.ForecastRegion.Forecast_region, f.ManufacturingSite.SiteName): f 
#                     for f in MasterDataFreightModel.objects.filter(version=scenario)
#                 }

#                 manual_assign_map = {
#                     (m.version.version, m.Product.Product, m.ShippingDate): m.Site
#                     for m in MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario)
#                 }

#                 self.stdout.write(f"Loaded lookup data:")
#                 self.stdout.write(f"  - Order book mappings: {len(order_book_map)}")
#                 self.stdout.write(f"  - Production history mappings: {len(production_map)}")
#                 self.stdout.write(f"  - Supplier mappings: {len(supplier_map)}")
#                 self.stdout.write(f"  - Incoterm mappings: {len(incoterm_map)}")
#                 self.stdout.write(f"  - Freight mappings: {len(freight_map)}")
#                 self.stdout.write(f"  - Manual assignment mappings: {len(manual_assign_map)}")
#                 self.stdout.write(f"  - Safety stock mappings: {len(safety_stock_map)}")

#                 # Define excluded sites and foundry sites
#                 excluded_sites = {'MTJ1', 'COI2', 'XUZ1', 'MER1', 'WUN1', 'WOD1', 'CHI1'}
#                 foundry_sites = {'XUZ1', 'MTJ1', 'COI2', 'MER1', 'WUN1', 'WOD1', 'CHI1'}

#                 # Create inventory tracking dictionary
#                 remaining_inventory = {}
#                 for idx, inv_row in inventory_agg.iterrows():
#                     location_key = (inv_row['product'], inv_row['site_id'])
#                     remaining_inventory[location_key] = {
#                         'onhand': inv_row['onhandstock_qty'] or 0,
#                         'intransit': inv_row['intransitstock_qty'] or 0
#                     }

#                 replenishment_records = []
#                 processed_count = 0
#                 total_records = len(forecast_df)

#                 self.stdout.write(f"Processing {total_records} forecast records...")

#                 for idx, row in forecast_df.iterrows():
#                     product = row['Product']
#                     location = row['Location']
#                     period = row['Period_AU']
#                     forecast_region = row['Forecast_Region']
#                     customer_code = row['Customer_code']
#                     total_qty = row['total_qty']

#                     # Progress indicator
#                     processed_count += 1
#                     if processed_count % 5000 == 0:
#                         self.stdout.write(f"Processed {processed_count}/{total_records} records ({processed_count/total_records*100:.1f}%)")

#                     # Get incoterm information
#                     incoterm_lookup_key = (scenario.version, customer_code)
#                     incoterm_obj = incoterm_map.get(incoterm_lookup_key)
                    
#                     if incoterm_obj:
#                         incoterm_code = incoterm_obj.IncoTerm
#                         incoterm_category = incoterm_obj.IncoTermCaregory
#                     else:
#                         incoterm_code = None
#                         incoterm_category = None

#                     # Calculate adjusted shipping date based on Incoterm FIRST (needed for manual assignment lookup)
#                     adjusted_shipping_date = period
                    
#                     # Pre-calculate adjusted shipping date for ALL potential sites to check manual assignment
#                     potential_sites = []
                    
#                     # Get potential sites from order book, production history, and suppliers
#                     order_book_site = order_book_map.get((scenario.version, product))
#                     if order_book_site:
#                         potential_sites.append(order_book_site)
                    
#                     production_site = production_map.get((scenario.version, product))
#                     if production_site:
#                         potential_sites.append(production_site)
                    
#                     supplier_site = supplier_map.get((scenario.version, product))
#                     if supplier_site:
#                         potential_sites.append(supplier_site)
                    
#                     # Calculate adjusted shipping date using the first available site for freight calculation
#                     temp_site = potential_sites[0] if potential_sites else None
                    
#                     if temp_site and incoterm_category:
#                         freight_lookup_key = (scenario.version, forecast_region, temp_site)
#                         freight_data = freight_map.get(freight_lookup_key)
                        
#                         if freight_data:
#                             lead_time_days = 0
                            
#                             # Normalize incoterm category to handle whitespace variations
#                             normalized_category = ' '.join(incoterm_category.split()) if incoterm_category else ''
                            
#                             if normalized_category == "NO FREIGHT":
#                                 lead_time_days = 0
#                             elif normalized_category == "PLANT TO DOMESTIC PORT":
#                                 lead_time_days = freight_data.PlantToDomesticPortDays
#                             elif normalized_category == "PLANT TO DOMESTIC PORT + INT FREIGHT":
#                                 lead_time_days = freight_data.PlantToDomesticPortDays + freight_data.OceanFreightDays
#                             elif normalized_category == "PLANT TO DOMESTIC PORT + INT FREIGHT + DOM FREIGHT":
#                                 lead_time_days = (freight_data.PlantToDomesticPortDays + 
#                                                 freight_data.OceanFreightDays + 
#                                                 freight_data.PortToCustomerDays)
                            
#                             if lead_time_days > 0:
#                                 adjusted_shipping_date = period - timedelta(days=lead_time_days)

#                     # PRIORITY 1: Manual assignment check (HIGHEST PRIORITY)
#                     # Check if this product and period match any manual assignment
#                     manual_lookup_key = (scenario.version, product, period)  # Use original period, not adjusted
#                     manual_site = manual_assign_map.get(manual_lookup_key)
                    
#                     if manual_site:
#                         # Manual assignment found - this takes absolute priority
#                         manual_site_name = manual_site.SiteName if hasattr(manual_site, 'SiteName') else manual_site
#                         site = manual_site_name
#                         self.stdout.write(f"MANUAL ASSIGNMENT: Product {product} for period {period} assigned to {site}")
#                     else:
#                         # PRIORITY 2: Order Book (if no manual assignment)
#                         site = order_book_map.get((scenario.version, product))
                        
#                         if not site:
#                             # PRIORITY 3: Production History (if no order book assignment)
#                             foundry = production_map.get((scenario.version, product))
#                             site = foundry
                        
#                         if not site:
#                             # PRIORITY 4: Supplier (if no production history assignment)
#                             vendor_id = supplier_map.get((scenario.version, product))
#                             site = vendor_id

#                     # Check foundry assignment (only for non-manual assignments)
#                     if site and site in foundry_sites and not manual_site:
#                         can_assign = self._can_product_be_assigned_to_foundry_sites(product)
#                         if not can_assign:
#                             site = None

#                     site_obj = plant_map.get(site)

                   
                    
#                     if not site_obj:
#                         continue  # This is where records get skipped!
                    
                    

#                     # Get safety stock requirements for this product-location combination
#                     safety_stock_key = (location, product)
#                     safety_stock_data = safety_stock_map.get(safety_stock_key, {'minimum_qty': 0, 'safety_qty': 0})
#                     required_closing_stock = safety_stock_data['minimum_qty'] + safety_stock_data['safety_qty']

#                     # Inventory analysis with safety stock consideration
#                     location_key = (product, location)
#                     remaining_qty = total_qty
                    
#                     if location not in excluded_sites and location_key in remaining_inventory:
#                         location_onhand_stock = remaining_inventory[location_key]['onhand']
#                         location_intransit_stock = remaining_inventory[location_key]['intransit']
#                         total_available_stock = location_onhand_stock + location_intransit_stock
                        
#                         if total_available_stock > 0:
#                             # Calculate how much stock we can use (must leave required closing stock)
#                             usable_stock = max(0, total_available_stock - required_closing_stock)
#                             stock_used = min(total_qty, usable_stock)
#                             remaining_qty = max(0, remaining_qty - stock_used)
                            
#                             # Update remaining inventory
#                             if stock_used > 0:
#                                 intransit_used = min(stock_used, location_intransit_stock)
#                                 onhand_used = stock_used - intransit_used
                                
#                                 remaining_inventory[location_key]['intransit'] -= intransit_used
#                                 remaining_inventory[location_key]['onhand'] -= onhand_used

#                             # Debug output for safety stock logic (disabled)
#                             # if required_closing_stock > 0:
#                             #     final_stock = total_available_stock - stock_used
#                             #     print(f"DEBUG Safety Stock - Product: {product}, Location: {location}")
#                             #     print(f"  Total Available: {total_available_stock}")
#                             #     print(f"  Required Closing: {required_closing_stock} (Min: {safety_stock_data['minimum_qty']}, Safety: {safety_stock_data['safety_qty']})")
#                             #     print(f"  Stock Used: {stock_used}")
#                             #     print(f"  Final Stock: {final_stock}")
#                             #     print(f"  Replenishment Needed: {remaining_qty}")

#                     # Calculate replenishment needed to fulfill demand and maintain safety stock
#                     current_stock_after_demand = 0
#                     if location_key in remaining_inventory:
#                         current_stock_after_demand = (remaining_inventory[location_key]['onhand'] + 
#                                                     remaining_inventory[location_key]['intransit'])
                    
#                     # Calculate total replenishment needed: max of (demand, minimum stock level)
#                     # This ensures we either fulfill demand OR maintain safety stock, whichever is higher
#                     min_stock_needed = max(remaining_qty, required_closing_stock - current_stock_after_demand)
#                     total_replenishment_needed = max(0, min_stock_needed)
                    
#                     # Create replenishment record if needed
#                     if total_replenishment_needed > 0:
#                         product_instance = product_map.get(product)
#                         if product_instance:
#                             replenishment_records.append({
#                                 'version': scenario,
#                                 'Product': product_instance,
#                                 'Location': location,
#                                 'Site': site_obj,
#                                 'ShippingDate': adjusted_shipping_date,
#                                 'ReplenishmentQty': total_replenishment_needed
#                             })

#                 # CRITICAL FIX: Aggregate replenishment records by (Product, Site, ShippingDate) to prevent quantity duplication
#                 self.stdout.write(f"Pre-aggregation: {len(replenishment_records)} replenishment records")
                
#                 # Group and sum replenishment quantities for the same product, site, and shipping date
#                 aggregated_replenishments = {}
#                 for record in replenishment_records:
#                     key = (record['Product'].Product, record['Site'].SiteName, record['ShippingDate'])
#                     if key not in aggregated_replenishments:
#                         aggregated_replenishments[key] = {
#                             'version': record['version'],
#                             'Product': record['Product'],
#                             'Location': record['Location'],  # Use first location found for this combination
#                             'Site': record['Site'],
#                             'ShippingDate': record['ShippingDate'],
#                             'ReplenishmentQty': 0
#                         }
#                     aggregated_replenishments[key]['ReplenishmentQty'] += record['ReplenishmentQty']

#                 # Convert back to model instances
#                 final_replenishment_records = [
#                     CalcualtedReplenishmentModel(
#                         version=data['version'],
#                         Product=data['Product'],
#                         Location=data['Location'],
#                         Site=data['Site'],
#                         ShippingDate=data['ShippingDate'],
#                         ReplenishmentQty=data['ReplenishmentQty']
#                     )
#                     for data in aggregated_replenishments.values()
#                 ]

#                 self.stdout.write(f"Post-aggregation: {len(final_replenishment_records)} replenishment records")

#                 # Bulk create records
#                 if final_replenishment_records:
#                     CalcualtedReplenishmentModel.objects.bulk_create(final_replenishment_records, batch_size=1000)
#                     self.stdout.write(self.style.SUCCESS(f"Created {len(final_replenishment_records)} replenishment records"))
#                 else:
#                     self.stdout.write("No replenishment records created")

#                 # Final summary
#                 final_count = CalcualtedReplenishmentModel.objects.filter(version=scenario.version).count()
#                 self.stdout.write(self.style.SUCCESS(f"Total records in database: {final_count}"))

#         finally:
#             # Always release the execution lock
#             cache.delete(cache_key)