from django.core.management.base import BaseCommand
from website.models import AggregatedForecast, scenarios, MasterDataProductModel
from django.conf import settings
import pandas as pd
from sqlalchemy import create_engine


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
    help = 'Populate the AggregatedForecast model with aggregated data from related models (fast pandas version)'
    
    def add_arguments(self, parser):
        parser.add_argument(
            'version',
            type=str,
            help="The version of the scenario to populate data for.",
        )

    def handle(self, *args, **kwargs):
        version = kwargs['version']

        # Delete old records for this version
        AggregatedForecast.objects.filter(version=version).delete()

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

        # Load all MasterDataProductModel rows
        product_df = pd.read_sql_query(
            "SELECT Product, DressMass, ProductGroupDescription, ParentProductGroupDescription FROM website_masterdataproductmodel",
            engine
        )

        # Load ProductSiteCostModel
        product_site_cost_df = pd.read_sql_query(
            "SELECT version_id, product_id, site_id, cost_aud, revenue_cost_aud FROM website_productsitecostmodel",
            engine
        )

        # Load MasterDataInventory
        inventory_df = pd.read_sql_query(
            "SELECT version_id, product, site_id, cost_aud FROM website_masterdatainventory",
            engine
        )

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
            # Only print for T690EP
            if str(row['product_id']) == 'T690EP':
                print(f"Checking cost for T690EP with key: {key}")
                print("cost_lookup:", cost_lookup.get(key))
                print("inv_cost_lookup:", inv_cost_lookup.get(key))
                print("revenue_cost_lookup:", revenue_cost_lookup.get(key))
            cost = cost_lookup.get(key)
            if cost is not None:
                if str(row['product_id']) == 'T690EP':
                    print(f"Found cost_aud for T690EP: {cost}, qty: {qty}")
                return cost * qty
            cost = inv_cost_lookup.get(key)
            if cost is not None:
                if str(row['product_id']) == 'T690EP':
                    print(f"Found inv_cost_aud for T690EP: {cost}, qty: {qty}")
                return cost * qty
            cost = revenue_cost_lookup.get(key)
            if cost is not None:
                if str(row['product_id']) == 'T690EP':
                    print(f"Found revenue_cost_aud for T690EP: {cost}, qty: {qty}")
                return cost * qty
            if str(row['product_id']) == 'T690EP':
                print(f"No cost found for T690EP with key: {key}")
            return 0

        print("Sample agg_df rows:")
        print(agg_df[['version_id', 'product_id', 'site_id', 'Qty']].head())
        print("Sample cost_lookup keys:", list(cost_lookup.keys())[:5])
        print("Sample inv_cost_lookup keys:", list(inv_cost_lookup.keys())[:5])
        print("Sample revenue_cost_lookup keys:", list(revenue_cost_lookup.keys())[:5])

        agg_df['cogs_aud'] = agg_df.apply(fast_cogs_aud, axis=1)
        for col in ['cogs_aud', 'revenue_aud', 'Qty', 'tonnes']:
            agg_df[col] = pd.to_numeric(agg_df[col], errors='coerce').fillna(0)

        objs = [
            AggregatedForecast(
                version_id=row['version_id'],
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
        AggregatedForecast.objects.bulk_create(objs, batch_size=1000)

        self.stdout.write(self.style.SUCCESS(f"AggregatedForecast populated for version {version} (fast pandas version)."))