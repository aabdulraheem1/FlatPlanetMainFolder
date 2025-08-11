import pandas as pd
from sqlalchemy import create_engine
from datetime import date
from django.core.management.base import BaseCommand
from website.models import (
    scenarios, ProductSiteCostModel, MasterDataProductModel, MasterDataPlantModel, SMART_Forecast_Model
)
import math

def extract_site_code(location):
    if not location:
        return location
    if '-' in location:
        return location.split('-')[-1]
    if '_' in location:
        return location.split('_')[-1]
    return location

def safe_float(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
        return float(val)
    except Exception:
        return None

class Command(BaseCommand):
    help = "Populate ProductSiteCostModel with all product-site combinations for a version"

    def add_arguments(self, parser):
        parser.add_argument('version', type=str)

    def handle(self, *args, **options):
        version_str = options['version']
        try:
            version = scenarios.objects.get(version=version_str)
            self.stdout.write(f"Found version: {version_str}")
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Version {version_str} does not exist."))
            return

        combos = SMART_Forecast_Model.objects.filter(version=version).values_list('Product', 'Location').distinct()
        self.stdout.write(f"Found {combos.count()} product-location combinations")

        # Prepare unique product and site codes for bulk SQL fetch
        product_keys = set()
        site_names = set()
        for product_str, site_str in combos:
            if product_str and site_str:
                product_keys.add(product_str.strip())
                site_names.add(extract_site_code(site_str.strip()))

        self.stdout.write(f"Unique products: {len(product_keys)}, Unique sites: {len(site_names)}")

        # Bulk fetch costs from SQL (batched to avoid SQL Server 2100 param limit)
        cost_lookup = {}
        
        if product_keys and site_names:
            try:
                Server = 'bknew-sql02'
                Database = 'Bradken_Data_Warehouse'
                Driver = 'ODBC Driver 17 for SQL Server'
                Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
                engine = create_engine(Database_Con)
                
                self.stdout.write("Connected to SQL Server successfully")
                
                site_names_list = list(site_names)
                product_keys_list = list(product_keys)
                MAX_PARAMS = 2000  # Stay under SQL Server's 2100 param limit
                max_products_per_batch = max(1, (MAX_PARAMS - len(site_names_list)))
                num_batches = math.ceil(len(product_keys_list) / max_products_per_batch)

                self.stdout.write(f"Processing {num_batches} batches")

                for i in range(num_batches):
                    batch_products = product_keys_list[i * max_products_per_batch : (i + 1) * max_products_per_batch]
                    placeholders_sites = ','.join(['?'] * len(site_names_list))
                    placeholders_products = ','.join(['?'] * len(batch_products))
                    params = tuple(site_names_list) + tuple(batch_products)
                    query = f"""
                        SELECT
                            pp.ProductKey,
                            ps.SiteName,
                            pit.UnitPriceAUD,
                            pd.DateValue
                        FROM [PowerBI].[Inventory Transaction] pit
                        LEFT JOIN [PowerBI].[Inventory Transaction Type] pitt ON pit.skInventoryTransactionTypeId = pitt.skInventoryTransactionTypeId
                        LEFT JOIN [PowerBI].[Site] ps ON pit.skSiteId = ps.skSiteId
                        LEFT JOIN [PowerBI].[Products] pp ON pit.skProductId = pp.skProductId
                        LEFT JOIN [PowerBI].[Dates] pd ON pit.skInventoryTransactionDateId = pd.skDateId
                        WHERE pitt.InventoryTransactionTypeDesc = 'Receipt To Stock'
                        AND ps.SiteName IN ({placeholders_sites})
                        AND pp.ProductKey IN ({placeholders_products})
                        ORDER BY pd.DateValue DESC
                    """
                    try:
                        df = pd.read_sql_query(query, engine, params=params)
                        self.stdout.write(f"Batch {i+1}: Retrieved {len(df)} cost records")
                        
                        # Build lookup: (product_key.lower(), site_name.lower()) -> (cost, date)
                        for _, row in df.iterrows():
                            if pd.notna(row['ProductKey']) and pd.notna(row['SiteName']):
                                key = (str(row['ProductKey']).strip().lower(), str(row['SiteName']).strip().lower())
                                if key not in cost_lookup:
                                    cost_lookup[key] = (row['UnitPriceAUD'], row['DateValue'])
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error in batch {i+1}: {str(e)}"))
                    
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Database connection failed: {str(e)}"))
                cost_lookup = {}
        else:
            self.stdout.write("No products or sites found, skipping SQL fetch")
            cost_lookup = {}

        # Build forecast lookup once, but keep the max PriceAUD for each (product, site)
        forecast_lookup = {}
        forecasts = SMART_Forecast_Model.objects.filter(
            version=version,
            Data_Source__in=['SMART', 'Not in SMART']
        ).exclude(PriceAUD=None)
        
        self.stdout.write(f"Processing {forecasts.count()} forecast records")
        
        for f in forecasts:
            site_code = extract_site_code(f.Location.strip()) if f.Location else ''
            key = (f.Product.strip().lower(), site_code.lower())
            # Always keep the max PriceAUD for each key
            if key not in forecast_lookup or (f.PriceAUD and f.PriceAUD > forecast_lookup[key]):
                forecast_lookup[key] = f.PriceAUD

        created = 0
        updated = 0
        skipped = 0
        for product_str, site_str in combos:
            if not product_str or not site_str:
                self.stdout.write(f"Skipping blank product or site: Product='{product_str}', Site='{site_str}'")
                skipped += 1
                continue
            try:
                product = MasterDataProductModel.objects.get(Product__iexact=product_str.strip())
                site_code = extract_site_code(site_str.strip())
                site = MasterDataPlantModel.objects.get(SiteName__iexact=site_code)
            except MasterDataProductModel.DoesNotExist:
                self.stdout.write(f"Product not found: '{product_str}'")
                skipped += 1
                continue
            except MasterDataPlantModel.DoesNotExist:
                self.stdout.write(f"Site not found: '{site_str}' (looked for '{site_code}')")
                skipped += 1
                continue

            key = (product.Product.strip().lower(), site_code.lower())
            cost_aud, cost_date = cost_lookup.get(key, (None, None))

            # Use the max PriceAUD for this product/site, if available
            price_aud = forecast_lookup.get(key)
            revenue_cost_aud = price_aud * 0.65 if price_aud is not None else None

            obj, created_flag = ProductSiteCostModel.objects.update_or_create(
                version=version,
                product=product,
                site=site,
                defaults={
                    'cost_aud': safe_float(cost_aud),
                    'cost_date': cost_date,
                    'revenue_cost_aud': safe_float(revenue_cost_aud)
                }
            )
            if created_flag:
                created += 1
            else:
                updated += 1

        self.stdout.write(self.style.SUCCESS(
            f"Completed for version {version_str}: "
            f"Created: {created}, Updated: {updated}, Skipped: {skipped}"
        ))