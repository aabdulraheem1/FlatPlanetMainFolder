from django.core.management.base import BaseCommand
from website.models import (
    scenarios, ProductSiteCostModel, MasterDataProductModel, MasterDataPlantModel, SMART_Forecast_Model
)
from datetime import date
import pandas as pd
from sqlalchemy import create_engine

def fetch_cost_for_product_site(product_key, site_name):
    # Set up your SQL Server connection
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)

    query = """
    SELECT TOP 1
        pit.UnitPriceAUD,
        pd.DateValue
    FROM [PowerBI].[Inventory Transaction] pit
    LEFT JOIN [PowerBI].[Inventory Transaction Type] pitt ON pit.skInventoryTransactionTypeId = pitt.skInventoryTransactionTypeId
    LEFT JOIN [PowerBI].[Site] ps ON pit.skSiteId = ps.skSiteId
    LEFT JOIN [PowerBI].[Products] pp ON pit.skProductId = pp.skProductId
    LEFT JOIN [PowerBI].[Dates] pd ON pit.skInventoryTransactionDateId = pd.skDateId
    WHERE pitt.InventoryTransactionTypeDesc = 'Receipt To Stock'
      AND ps.SiteName = ?
      AND pp.ProductKey = ?
    ORDER BY pd.DateValue DESC
    """
    try:
        df = pd.read_sql_query(query, engine, params=(site_name, product_key))
        if not df.empty:
            return df.iloc[0]['UnitPriceAUD'], df.iloc[0]['DateValue']
    except Exception as e:
        print(f"Error fetching cost for {product_key} at {site_name}: {e}")
    return None, None

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

        # Get all unique product-site combinations from SMART_Forecast_Model for this version
        combos = SMART_Forecast_Model.objects.filter(version=version).values_list('Product', 'Location').distinct()

        created = 0
        for product_str, site_str in combos:
            try:
                product = MasterDataProductModel.objects.get(Product=product_str)
                site = MasterDataPlantModel.objects.get(SiteName=site_str)
            except (MasterDataProductModel.DoesNotExist, MasterDataPlantModel.DoesNotExist):
                continue

            # Fetch cost and date from external SQL
            cost_aud, cost_date = fetch_cost_for_product_site(product.Product, site.SiteName)

            obj, created_flag = ProductSiteCostModel.objects.update_or_create(
                version=version,
                product=product,
                site=site,
                defaults={'cost_aud': cost_aud, 'cost_date': cost_date}
            )
            if created_flag:
                created += 1

        self.stdout.write(self.style.SUCCESS(f"Populated/updated {created} product-site cost records for version {version_str}."))