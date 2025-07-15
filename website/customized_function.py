from django.core.management.base import BaseCommand
from website.models import (
    scenarios, ProductSiteCostModel, MasterDataProductModel, MasterDataPlantModel, SMART_Forecast_Model, AggregatedForecast,
        CalculatedProductionModel
)
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
from django.db.models import Sum
from django.db.models.functions import TruncMonth

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


def get_forecast_data_by_parent_product_group(scenario_version):
        queryset = (
            AggregatedForecast.objects
            .filter(version=scenario_version)
            .values('parent_product_group_description', 'period')
            .annotate(total_tonnes=Sum('tonnes'))
            .order_by('parent_product_group_description', 'period')
        )
        # Build data structure: {ParentGroup: {period: total}}, labels: [periods]
        data = {}
        labels_set = set()
        for entry in queryset:
            group = entry['parent_product_group_description'] or 'Unknown'
            period = entry['period'].strftime('%Y-%m')
            labels_set.add(period)
            data.setdefault(group, {})[period] = entry['total_tonnes']
        labels = sorted(labels_set)
        chart_data = {}
        for group, period_dict in data.items():
            chart_data[group] = {
                'labels': labels,
                'tons': [period_dict.get(label, 0) for label in labels]
            }
        return chart_data

from website.models import AggregatedForecast
from django.db.models import Sum

def get_monthly_cogs_and_revenue(version):
    data = (
        AggregatedForecast.objects
        .filter(version=version)
        .annotate(month=TruncMonth('period'))
        .values('month')
        .annotate(
            total_cogs=Sum('cogs_aud'),
            total_revenue=Sum('revenue_aud')
        )
        .order_by('month')
    )
    months = [d['month'].strftime('%b %Y') for d in data]
    cogs = [d['total_cogs'] for d in data]
    revenue = [d['total_revenue'] for d in data]
    return months, cogs, revenue

# Add other aggregation or chart helper functions here as needed

from django.db.models import Sum

def get_monthly_production_cogs(scenario):
    data = (
        CalculatedProductionModel.objects
        .filter(version=scenario)
        .annotate(month=TruncMonth('pouring_date'))
        .values('month')
        .annotate(total_production_cogs=Sum('cogs_aud'))
        .order_by('month')
    )
    months = [d['month'].strftime('%b %Y') for d in data]
    production_cogs = [d['total_production_cogs'] for d in data]
    return months, production_cogs

def get_monthly_production_cogs_by_group(scenario):
    data = (
        CalculatedProductionModel.objects
        .filter(version=scenario)
        .annotate(month=TruncMonth('pouring_date'))
        .values('month', 'product_group')
        .annotate(total_cogs_aud=Sum('cogs_aud'))
        .order_by('month', 'product_group')
    )
    # Build data structure: {group: {month: total}}, labels: [months]
    groups = set()
    months_set = set()
    group_month_data = {}
    for entry in data:
        month = entry['month'].strftime('%b %Y')
        group = entry['product_group'] or 'Unknown'
        groups.add(group)
        months_set.add(month)
        group_month_data.setdefault(group, {})[month] = entry['total_cogs_aud']
    months = sorted(months_set, key=lambda d: pd.to_datetime(d, format='%b %Y'))
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)'
    ]
    for idx, group in enumerate(sorted(groups)):
        data = [group_month_data[group].get(m, 0) for m in months]
        datasets.append({
            'label': group,
            'data': data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1
        })
    return {'labels': months, 'datasets': datasets}

def get_monthly_production_cogs_by_parent_group(scenario):
    data = (
        CalculatedProductionModel.objects
        .filter(version=scenario)
        .annotate(month=TruncMonth('pouring_date'))
        .values('month', 'parent_product_group')
        .annotate(total_cogs_aud=Sum('cogs_aud'))
        .order_by('month', 'parent_product_group')
    )
    # Build data structure: {group: {month: total}}, labels: [months]
    groups = set()
    months_set = set()
    group_month_data = {}
    for entry in data:
        month = entry['month'].strftime('%b %Y')
        group = entry['parent_product_group'] or 'Unknown'
        groups.add(group)
        months_set.add(month)
        group_month_data.setdefault(group, {})[month] = entry['total_cogs_aud']
    months = sorted(months_set, key=lambda d: pd.to_datetime(d, format='%b %Y'))
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)'
    ]
    for idx, group in enumerate(sorted(groups)):
        data = [group_month_data[group].get(m, 0) for m in months]
        datasets.append({
            'label': group,
            'data': data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1
        })
    return {'labels': months, 'datasets': datasets}