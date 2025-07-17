from django.core.management.base import BaseCommand
from django.conf import settings
from website.models import (
    AggregatedForecast, SMART_Forecast_Model, MasterDataProductModel,
    ProductSiteCostModel, MasterDataInventory, scenarios,
    FixedPlantConversionModifiersModel, RevenueToCogsConversionModel, SiteAllocationModel
)
import pandas as pd
from sqlalchemy import create_engine
from django.db.models import Sum

def extract_site_code(location):
    if not isinstance(location, str):
        return None
    # Split by '-' and get the part after the dash
    parts = location.split('-')
    if len(parts) > 1:
        candidate = parts[-1]
        # If there are underscores, get the last part after '_'
        candidate = candidate.split('_')[-1]
        if len(candidate) == 4 and candidate.isalnum():
            return candidate
    return None

class Command(BaseCommand):
    help = 'Populate the AggregatedForecast model with data from all sources (Regular SMART, Fixed Plant, Revenue Forecast)'
    
    

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
            self.stdout.write(self.style.ERROR(f"Scenario version '{version}' not found."))
            return

        # Delete old records for this version
        AggregatedForecast.objects.filter(version=scenario).delete()
        self.stdout.write("Deleted existing aggregated forecast data")

        # Build SQLAlchemy engine using Django settings
        db_settings = settings.DATABASES['default']
        if db_settings['ENGINE'].endswith('sqlite3'):
            engine = create_engine(f"sqlite:///{db_settings['NAME']}")
        else:
            user = db_settings['USER']
            password = db_settings['PASSWORD']
            host = db_settings['HOST']
            port = db_settings['PORT']
            name = db_settings['NAME']
            if port:
                engine = create_engine(f"mssql+pyodbc://{user}:{password}@{host}:{port}/{name}?driver=ODBC+Driver+17+for+SQL+Server")
            else:
                engine = create_engine(f"mssql+pyodbc://{user}:{password}@{host}/{name}?driver=ODBC+Driver+17+for+SQL+Server")

        # Load all SMART_Forecast_Model rows for this version
        forecast_df = pd.read_sql_query(
            "SELECT * FROM website_smart_forecast_model WHERE version_id = ?", engine, params=(version,)
        )
        
        if forecast_df.empty:
            self.stdout.write(self.style.WARNING(f"No SMART forecast data found for version {version}"))
            return

        # Load all MasterDataProductModel rows
        product_df = pd.read_sql_query(
            "SELECT Product, DressMass, ProductGroupDescription, ParentProductGroupDescription FROM website_masterdataproductmodel",
            engine
        )

        # Load ProductSiteCostModel
        product_site_cost_df = pd.read_sql_query(
            "SELECT version_id, product_id, site_id, cost_aud, revenue_cost_aud FROM website_productsitecostmodel WHERE version_id = ?",
            engine, params=(version,)
        )

        # Load MasterDataInventory
        inventory_df = pd.read_sql_query(
            "SELECT version_id, product, site_id, cost_aud FROM website_masterdatainventory WHERE version_id = ?",
            engine, params=(version,)
        )

        # Separate processing by data source
        all_agg_objs = []
        
        # Track data source metrics
        data_source_counts = forecast_df['Data_Source'].value_counts()
        self.stdout.write(f"Data source breakdown: {dict(data_source_counts)}")
        
        # 1. Process Regular SMART Forecast (excluding Fixed Plant and Revenue Forecast)
        regular_df = forecast_df[~forecast_df['Data_Source'].isin(['Fixed Plant', 'Revenue Forecast'])].copy()
        if not regular_df.empty:
            self.stdout.write(f"Processing {len(regular_df)} regular SMART forecast records...")
            regular_objs = self.process_regular_forecast(regular_df, product_df, product_site_cost_df, inventory_df, scenario)
            all_agg_objs.extend(regular_objs)
            regular_tonnes = sum(obj.tonnes for obj in regular_objs)
            self.stdout.write(f"Regular forecast: {len(regular_objs)} records, {regular_tonnes:.2f} tonnes")

        # 2. Process Fixed Plant
        fixed_plant_df = forecast_df[forecast_df['Data_Source'] == 'Fixed Plant'].copy()
        if not fixed_plant_df.empty:
            self.stdout.write(f"Processing {len(fixed_plant_df)} Fixed Plant forecast records...")
            fixed_plant_objs = self.process_fixed_plant_forecast(fixed_plant_df, product_df, scenario)
            all_agg_objs.extend(fixed_plant_objs)
            fixed_plant_tonnes = sum(obj.tonnes for obj in fixed_plant_objs)
            self.stdout.write(f"Fixed Plant: {len(fixed_plant_objs)} records, {fixed_plant_tonnes:.2f} tonnes")

        # 3. Process Revenue Forecast
        revenue_df = forecast_df[forecast_df['Data_Source'] == 'Revenue Forecast'].copy()
        if not revenue_df.empty:
            self.stdout.write(f"Processing {len(revenue_df)} Revenue Forecast records...")
            revenue_objs = self.process_revenue_forecast(revenue_df, product_df, scenario)
            all_agg_objs.extend(revenue_objs)
            revenue_tonnes = sum(obj.tonnes for obj in revenue_objs)
            self.stdout.write(f"Revenue Forecast: {len(revenue_objs)} records, {revenue_tonnes:.2f} tonnes")

        # Bulk create all records
        if all_agg_objs:
            AggregatedForecast.objects.bulk_create(all_agg_objs, batch_size=1000)
            total_tonnes = sum(obj.tonnes for obj in all_agg_objs)
            total_revenue = sum(obj.revenue_aud for obj in all_agg_objs)
            total_cogs = sum(obj.cogs_aud for obj in all_agg_objs)
            
            self.stdout.write(self.style.SUCCESS(f"Successfully created {len(all_agg_objs)} AggregatedForecast records for version {version}"))
            self.stdout.write(f"Summary: {total_tonnes:.2f} tonnes, ${total_revenue:,.2f} revenue, ${total_cogs:,.2f} COGS")
        else:
            self.stdout.write(self.style.WARNING("No records were created"))

        # Validate products and show any issues
        self._validate_products(scenario)

    def process_regular_forecast(self, forecast_df, product_df, product_site_cost_df, inventory_df, scenario):
        """Process regular SMART forecast data (ORIGINAL WORKING LOGIC)."""
        
        # Merge forecast and product data
        merged = pd.merge(
            forecast_df,
            product_df,
            left_on='Product',
            right_on='Product',
            how='left',
            suffixes=('', '_product')
        )

        # Calculate tonnes
        def calc_tonnes(row):
            qty = row['Qty']
            dress_mass = row['DressMass']
            price_aud = row.get('PriceAUD', None)
            if pd.notnull(qty) and pd.notnull(dress_mass) and dress_mass != 0:
                return qty * dress_mass / 1000
            elif pd.notnull(qty) and pd.notnull(price_aud):
                return (qty * price_aud * 0.65) / 5000
            else:
                return 0

        merged['tonnes'] = merged.apply(calc_tonnes, axis=1)
        merged['site_code'] = merged['Location'].apply(extract_site_code)
        merged['revenue_aud'] = merged['Qty'] * merged['PriceAUD']
        
        agg_df = pd.DataFrame({
            'version_id': merged['version_id'],
            'tonnes': merged['tonnes'],
            'forecast_region': merged['Forecast_Region'],
            'customer_code': merged['Customer_code'],
            'period': merged['Period_AU'],
            'product_id': merged['Product'],
            'product_group_description': merged['ProductGroupDescription'],
            'parent_product_group_description': merged['ParentProductGroupDescription'],
            'site_id': merged['site_code'],
            'Qty': merged['Qty'],
            'revenue_aud': merged['revenue_aud'],
        })

        # Clean product_id: convert to string, strip whitespace, filter out empty
        agg_df['product_id'] = agg_df['product_id'].astype(str).str.strip()
        agg_df = agg_df[agg_df['product_id'] != '']

        # Get all valid product codes from the DB
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        agg_df = agg_df[agg_df['product_id'].isin(valid_products)]

        # --- FAST LOOKUP DICTS ---
        # Build cost lookup dicts using site name
        cost_lookup = {
            (str(row['version_id']), str(row['product_id']), str(row['site_id'])): row['cost_aud']
            for _, row in product_site_cost_df.iterrows()
            if pd.notnull(row['cost_aud'])
        }
        inv_cost_lookup = {
            (str(row['version_id']), str(row['product']), str(row['site_id'])): row['cost_aud']
            for _, row in inventory_df.iterrows()
            if pd.notnull(row['cost_aud'])
        }
        revenue_cost_lookup = {
            (str(row['version_id']), str(row['product_id']), str(row['site_id'])): row['revenue_cost_aud']
            for _, row in product_site_cost_df.iterrows()
            if pd.notnull(row['revenue_cost_aud'])
        }

        def fast_cogs_aud(row):
            key = (str(row['version_id']), str(row['product_id']), str(row['site_id']))
            qty = row.get('Qty', 1)
            cost = cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            cost = inv_cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            cost = revenue_cost_lookup.get(key)
            if cost is not None:
                return cost * qty
            return 0

        agg_df['cogs_aud'] = agg_df.apply(fast_cogs_aud, axis=1)
        for col in ['cogs_aud', 'revenue_aud', 'Qty', 'tonnes']:
            agg_df[col] = pd.to_numeric(agg_df[col], errors='coerce').fillna(0)

        objs = [
            AggregatedForecast(
                version=scenario,  # Changed from version_id to version (scenario object)
                tonnes=float(row['tonnes']),
                forecast_region=row['forecast_region'],
                customer_code=row['customer_code'],
                period=row['period'],
                product_id=row['product_id'],
                product_group_description=row['product_group_description'],
                parent_product_group_description=row['parent_product_group_description'],
                cogs_aud=float(row['cogs_aud']),
                qty=float(row['Qty']),
                revenue_aud=float(row['revenue_aud']),
            )
            for _, row in agg_df.iterrows()
        ]
        
        self.stdout.write(f"Created {len(objs)} regular forecast objects")
        return objs

    def process_fixed_plant_forecast(self, forecast_df, product_df, scenario):
        """Process Fixed Plant forecast data using conversion modifiers."""
        
        # Get all valid product codes for validation
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        
        # Merge with product data using inner join to exclude invalid products
        merged = pd.merge(
            forecast_df,
            product_df,
            left_on='Product',
            right_on='Product',
            how='inner'  # Only keep records with valid products
        )

        if merged.empty:
            self.stdout.write("No valid products found in Fixed Plant forecast data")
            return []

        objs = []
        processed_products = set()
        
        for _, row in merged.iterrows():
            try:
                product_code = str(row['Product']).strip()
                
                # Double-check product validation
                if product_code not in valid_products:
                    continue
                
                if pd.isnull(row['Qty']) or row['Qty'] == 0:
                    continue

                qty = float(row['Qty'])
                
                # Only show debug for first occurrence of each product
                show_debug = product_code not in processed_products
                processed_products.add(product_code)

                # Get product object
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                except MasterDataProductModel.DoesNotExist:
                    if show_debug:
                        self.stdout.write(f"Warning: Product {product_code} not found in master data")
                    continue

                try:
                    # Get Fixed Plant conversion modifier (no site filtering)
                    modifier = FixedPlantConversionModifiersModel.objects.get(
                        version=scenario,
                        Product=product_obj
                    )

                    if show_debug:
                        self.stdout.write(f"Found Fixed Plant modifier for {product_code}")

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

                    if show_debug:
                        self.stdout.write(f"Fixed Plant {product_code}: {tonnes:.4f} tonnes per record (using modifier)")

                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=cogs_aud,
                        qty=qty,
                        revenue_aud=revenue_aud,
                    ))

                except FixedPlantConversionModifiersModel.DoesNotExist:
                    if show_debug:
                        self.stdout.write(f"Fixed Plant {product_code}: No modifier found - using DressMass fallback")
                    
                    # Create default record with original DressMass logic
                    dress_mass = row['DressMass']
                    if pd.notnull(dress_mass) and dress_mass != 0:
                        tonnes = qty * dress_mass / 1000
                    else:
                        tonnes = 0
                    
                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=0.0,
                        qty=qty,
                        revenue_aud=qty,
                    ))

            except Exception as e:
                self.stdout.write(f"Error processing Fixed Plant record for {product_code}: {e}")
                continue

        self.stdout.write(f"Created {len(objs)} Fixed Plant objects")
        
        # Summary of tonnes created
        total_tonnes = sum(obj.tonnes for obj in objs)
        self.stdout.write(f"Fixed Plant total tonnes: {total_tonnes:.2f}")
        
        return objs

    def process_revenue_forecast(self, forecast_df, product_df, scenario):

        """Process Revenue Forecast data using conversion modifiers and site allocation."""
        
        # Get all valid product codes for validation
        valid_products = set(MasterDataProductModel.objects.values_list('Product', flat=True))
        
        # Merge with product data using inner join to exclude invalid products
        merged = pd.merge(
            forecast_df,
            product_df,
            left_on='Product',
            right_on='Product',
            how='inner'  # Only keep records with valid products
        )

        if merged.empty:
            self.stdout.write("No valid products found in Revenue Forecast data")
            return []

        objs = []
        for _, row in merged.iterrows():
            try:
                product_code = str(row['Product']).strip()
                
                # Double-check product validation
                if product_code not in valid_products:
                    self.stdout.write(f"Skipping invalid Revenue Forecast product: {product_code}")
                    continue
                
                if pd.isnull(row['Qty']) or row['Qty'] == 0:
                    continue

                qty = float(row['Qty'])

                # Get product object
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                except MasterDataProductModel.DoesNotExist:
                    self.stdout.write(f"Warning: Product {product_code} not found in master data")
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
                            allocated_qty = qty * allocation_percentage

                            objs.append(AggregatedForecast(
                                version=scenario,
                                tonnes=allocated_tonnes,
                                forecast_region=row['Forecast_Region'],
                                customer_code=row['Customer_code'],
                                period=row['Period_AU'],
                                product_id=product_code,
                                product_group_description=row['ProductGroupDescription'],
                                parent_product_group_description=row['ParentProductGroupDescription'],
                                cogs_aud=allocated_cogs,
                                qty=allocated_qty,
                                revenue_aud=allocated_revenue,
                            ))
                    else:
                        self.stdout.write(f"Warning: No site allocation found for product {product_code}")
                        # Create single record with no site allocation
                        objs.append(AggregatedForecast(
                            version=scenario,
                            tonnes=total_tonnes,
                            forecast_region=row['Forecast_Region'],
                            customer_code=row['Customer_code'],
                            period=row['Period_AU'],
                            product_id=product_code,
                            product_group_description=row['ProductGroupDescription'],
                            parent_product_group_description=row['ParentProductGroupDescription'],
                            cogs_aud=total_cogs_aud,
                            qty=qty,
                            revenue_aud=revenue_aud,
                        ))

                except RevenueToCogsConversionModel.DoesNotExist:
                    self.stdout.write(f"Warning: No revenue conversion modifier found for product {product_code}")
                    # Create default record with original logic
                    dress_mass = row['DressMass']
                    if pd.notnull(dress_mass) and dress_mass != 0:
                        tonnes = qty * dress_mass / 1000
                    else:
                        tonnes = 0
                    
                    objs.append(AggregatedForecast(
                        version=scenario,
                        tonnes=tonnes,
                        forecast_region=row['Forecast_Region'],
                        customer_code=row['Customer_code'],
                        period=row['Period_AU'],
                        product_id=product_code,
                        product_group_description=row['ProductGroupDescription'],
                        parent_product_group_description=row['ParentProductGroupDescription'],
                        cogs_aud=0.0,
                        qty=qty,
                        revenue_aud=qty,
                    ))

            except Exception as e:
                self.stdout.write(f"Error processing Revenue Forecast record: {e}")
                continue

        self.stdout.write(f"Created {len(objs)} Revenue Forecast objects")
        return objs


    def _validate_products(self, scenario):
        """Validate products and show any data quality issues."""
        # Get all products from SMART_Forecast_Model for this version
        forecast_products = set(
            SMART_Forecast_Model.objects.filter(version=scenario)
            .values_list('Product', flat=True)
            .distinct()
        )

        # Get all valid products from MasterDataProductModel
        valid_products = set(
            MasterDataProductModel.objects.values_list('Product', flat=True)
        )

        # Find invalid products
        invalid_products = forecast_products - valid_products

        self.stdout.write(f"\nProduct validation:")
        self.stdout.write(f"  - Total products in SMART forecast: {len(forecast_products)}")
        self.stdout.write(f"  - Valid products in master data: {len(valid_products)}")
        self.stdout.write(f"  - Invalid products found: {len(invalid_products)}")

        if invalid_products:
            self.stdout.write(self.style.WARNING("\nInvalid products (exist in SMART forecast but not in master data):"))
            for product in sorted(invalid_products):
                count = SMART_Forecast_Model.objects.filter(
                    version=scenario, 
                    Product=product
                ).count()
                self.stdout.write(f"  - '{product}' ({count} records)")
            
            self.stdout.write(self.style.WARNING(f"\nAction required: These {len(invalid_products)} products need to be either:"))
            self.stdout.write("  1. Added to MasterDataProductModel, or")
            self.stdout.write("  2. Removed/corrected in SMART_Forecast_Model")
        else:
            self.stdout.write(self.style.SUCCESS("  ✓ All products in SMART forecast are valid!"))

        # Check for products with zero tonnes
        zero_tonnes_count = AggregatedForecast.objects.filter(version=scenario, tonnes=0).count()
        if zero_tonnes_count > 0:
            self.stdout.write(self.style.WARNING(f"\nWarning: {zero_tonnes_count} records have zero tonnes"))
            
            # Show breakdown by product
            from django.db.models import Count
            zero_by_product = (
                AggregatedForecast.objects
                .filter(version=scenario, tonnes=0)
                .values('product_id')
                .annotate(count=Count('id'))
                .order_by('-count')[:5]
            )
            
            if zero_by_product:
                self.stdout.write("  Top products with zero tonnes:")
                for item in zero_by_product:
                    self.stdout.write(f"    - {item['product_id']}: {item['count']} records")

        # Show Fixed Plant specific issues
        self._validate_fixed_plant_modifiers(scenario)

    def _validate_fixed_plant_modifiers(self, scenario):
        """Validate Fixed Plant conversion modifiers."""
        # Get all Fixed Plant products from SMART forecast
        fixed_plant_products = set(
            SMART_Forecast_Model.objects.filter(
                version=scenario,
                Data_Source='Fixed Plant'
            ).values_list('Product', flat=True).distinct()
        )

        if not fixed_plant_products:
            return

        self.stdout.write(f"\nFixed Plant validation:")
        self.stdout.write(f"  - Fixed Plant products in forecast: {len(fixed_plant_products)}")

        # Check which products have conversion modifiers (no site filtering)
        products_with_modifiers = set(
            FixedPlantConversionModifiersModel.objects.filter(
                version=scenario
            ).values_list('Product__Product', flat=True)
        )

        # Check missing modifiers
        missing_modifiers = fixed_plant_products - products_with_modifiers

        if missing_modifiers:
            self.stdout.write(self.style.WARNING("  - Missing Fixed Plant conversion modifiers:"))
            for product in sorted(missing_modifiers):
                self.stdout.write(f"    - {product}")
            self.stdout.write("  - These products will use DressMass fallback calculation")
        else:
            self.stdout.write(self.style.SUCCESS("  ✓ All Fixed Plant products have conversion modifiers!"))

        # Show Revenue Forecast validation
        self._validate_revenue_forecast_modifiers(scenario)

    def _validate_revenue_forecast_modifiers(self, scenario):
        """Validate Revenue Forecast conversion modifiers."""
        # Get all Revenue Forecast products from SMART forecast
        revenue_products = set(
            SMART_Forecast_Model.objects.filter(
                version=scenario,
                Data_Source='Revenue Forecast'
            ).values_list('Product', flat=True).distinct()
        )

        if not revenue_products:
            return

        self.stdout.write(f"\nRevenue Forecast validation:")
        self.stdout.write(f"  - Revenue Forecast products in forecast: {len(revenue_products)}")

        # Check which products have conversion modifiers
        products_with_revenue_modifiers = set(
            RevenueToCogsConversionModel.objects.filter(
                version=scenario
            ).values_list('Product__Product', flat=True)
        )

        # Check which products have site allocations
        products_with_allocations = set(
            SiteAllocationModel.objects.filter(
                version=scenario
            ).values_list('Product__Product', flat=True)
        )

        # Check missing modifiers
        missing_revenue_modifiers = revenue_products - products_with_revenue_modifiers
        missing_allocations = revenue_products - products_with_allocations

        if missing_revenue_modifiers:
            self.stdout.write(self.style.WARNING("  - Missing Revenue-to-COGS conversion modifiers:"))
            for product in sorted(missing_revenue_modifiers):
                self.stdout.write(f"    - {product}")

        if missing_allocations:
            self.stdout.write(self.style.WARNING("  - Missing site allocations:"))
            for product in sorted(missing_allocations):
                self.stdout.write(f"    - {product}")

        if not missing_revenue_modifiers and not missing_allocations:
            self.stdout.write(self.style.SUCCESS("  ✓ All Revenue Forecast products have conversion modifiers and site allocations!"))
