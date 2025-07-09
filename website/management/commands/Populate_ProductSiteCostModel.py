import pandas as pd
from sqlalchemy import create_engine
from datetime import date
from django.core.management.base import BaseCommand
from website.models import (
    scenarios, ProductSiteCostModel, MasterDataProductModel, MasterDataPlantModel, SMART_Forecast_Model
)

def extract_site_code(location):
    if not location:
        return location
    if '-' in location:
        return location.split('-')[-1]
    if '_' in location:
        return location.split('_')[-1]
    return location

class Command(BaseCommand):
    help = "Populate ProductSiteCostModel with all product-site combinations for a version"

    def add_arguments(self, parser):
        parser.add_argument('version', type=str)

    def handle(self, *args, **options):
        version_str = options['version']
        try:
            version = scenarios.objects.get(version=version_str)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Version {version_str} does not exist."))
            return

        combos = SMART_Forecast_Model.objects.filter(version=version).values_list('Product', 'Location').distinct()

        # Prepare unique product and site codes for bulk SQL fetch
        product_keys = set()
        site_names = set()
        for product_str, site_str in combos:
            if product_str and site_str:
                product_keys.add(product_str.strip())
                site_names.add(extract_site_code(site_str.strip()))

        # Bulk fetch costs from SQL
        Server = 'bknew-sql02'
        Database = 'Bradken_Data_Warehouse'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        engine = create_engine(Database_Con)

        if product_keys and site_names:
            placeholders_products = ','.join(['?'] * len(product_keys))
            placeholders_sites = ','.join(['?'] * len(site_names))
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
            params = list(site_names) + list(product_keys)
            df = pd.read_sql_query(query, engine, params=params)
            # Build lookup: (product_key.lower(), site_name.lower()) -> (cost, date)
            cost_lookup = {}
            for _, row in df.iterrows():
                key = (str(row['ProductKey']).strip().lower(), str(row['SiteName']).strip().lower())
                if key not in cost_lookup:
                    cost_lookup[key] = (row['UnitPriceAUD'], row['DateValue'])
        else:
            cost_lookup = {}

        # Build forecast lookup once
        forecast_lookup = {}
        forecasts = SMART_Forecast_Model.objects.filter(
            version=version,
            Data_Source__in=['SMART', 'Not in SMART']
        ).exclude(PriceAUD=None)
        for f in forecasts:
            site_code = extract_site_code(f.Location.strip()) if f.Location else ''
            key = (f.Product.strip().lower(), site_code.lower())
            if key not in forecast_lookup or (f.PriceAUD and f.PriceAUD > forecast_lookup[key]):
                forecast_lookup[key] = f.PriceAUD

        created = 0
        for product_str, site_str in combos:
            if not product_str or not site_str:
                print(f"Skipping blank product or site: Product='{product_str}', Site='{site_str}'")
                continue
            try:
                product = MasterDataProductModel.objects.get(Product__iexact=product_str.strip())
                site_code = extract_site_code(site_str.strip())
                site = MasterDataPlantModel.objects.get(SiteName__iexact=site_code)
            except MasterDataProductModel.DoesNotExist:
                print(f"Product not found: '{product_str}'")
                continue
            except MasterDataPlantModel.DoesNotExist:
                print(f"Site not found: '{site_str}' (looked for '{site_code}')")
                continue

            key = (product.Product.strip().lower(), site_code.lower())
            cost_aud, cost_date = cost_lookup.get(key, (None, None))
            revenue_cost_aud = None

            if cost_aud is None:
                price_aud = forecast_lookup.get(key)
                if price_aud is not None:
                    revenue_cost_aud = price_aud * 0.65

            obj, created_flag = ProductSiteCostModel.objects.update_or_create(
                version=version,
                product=product,
                site=site,
                defaults={
                    'cost_aud': cost_aud,
                    'cost_date': cost_date,
                    'revenue_cost_aud': revenue_cost_aud
                }
            )
            if created_flag:
                created += 1

        self.stdout.write(self.style.SUCCESS(f"Populated/updated {created} product-site cost records for version {version_str}."))