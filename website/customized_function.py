from django.core.management.base import BaseCommand
from website.models import (
    scenarios, ProductSiteCostModel, MasterDataProductModel, MasterDataPlantModel, SMART_Forecast_Model, AggregatedForecast,
        CalculatedProductionModel
)
from collections import defaultdict
from django.db.models.functions import TruncMonth
from django.db.models import Sum
from datetime import datetime, date
import json
import pandas as pd
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
from django.db.models import Sum
from django.db.models.functions import TruncMonth
from django.utils.safestring import mark_safe
from googletrans import Translator
import langdetect

TRANSLATION_CACHE = {}

MANUAL_TRANSLATIONS = {
    'Coulée': 'Casting',
    'Usinage': 'Machining',
    'Fonderie': 'Foundry',
    'Assemblage': 'Assembly',
    'Contrôle qualité': 'Quality Control',
    'Finition': 'Finishing',
    'Nettoyage': 'Cleaning',
    'Inspection': 'Inspection',
    'Polissage': 'Polishing',
    'Soudage': 'Welding',
    'Découpage': 'Cutting',
    'Perçage': 'Drilling',
    'Tournage': 'Turning',
    'Fraisage': 'Milling',
    'Grenaillage': 'Shot Blasting',
    'Traitement thermique': 'Heat Treatment',
    'Meulage': 'Grinding',
    'Ébavurage': 'Deburring',
    'Prêt à expédier': 'Ready to ship',
    'Noyaux Cold Box': 'Cold Box Cores',
    'Passage à la couche': 'Coating Application',
    'Moulage': 'Molding',
    'Ajustage des noyaux': 'Core Fitting',
    'Démoulage': 'Mold Release',
    'Marteaupiqueur': 'Hammering',
}

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

def get_monthly_cogs_and_revenue(scenario, start_date=None):
    """
    Get monthly COGS and revenue data for a scenario, optionally filtered by start_date.
    """
    queryset = AggregatedForecast.objects.filter(version=scenario)
    
    # Apply start_date filter if provided
    if start_date:
        queryset = queryset.filter(period__gte=start_date)
    
    queryset = (
        queryset
        .annotate(month=TruncMonth('period'))
        .values('month')
        .annotate(
            total_cogs=Sum('cogs_aud'),
            total_revenue=Sum('revenue_aud')
        )
        .order_by('month')
    )
    
    months = [item['month'].strftime('%b %Y') for item in queryset]
    cogs = [item['total_cogs'] or 0 for item in queryset]
    revenue = [item['total_revenue'] or 0 for item in queryset]
    
    return months, cogs, revenue

from django.db.models import Sum

def get_monthly_production_cogs(scenario, start_date=None):
    """
    Get monthly production COGS data for a scenario, optionally filtered by start_date.
    """
    queryset = CalculatedProductionModel.objects.filter(version=scenario)
    
    # Apply start_date filter if provided
    if start_date:
        queryset = queryset.filter(pouring_date__gte=start_date)
    
    queryset = (
        queryset
        .annotate(month=TruncMonth('pouring_date'))
        .values('month')
        .annotate(total_production_cogs=Sum('cogs_aud'))
        .order_by('month')
    )
    
    months = [item['month'].strftime('%b %Y') for item in queryset]
    production_cogs = [item['total_production_cogs'] or 0 for item in queryset]
    
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

def get_monthly_production_cogs_by_parent_group(scenario, start_date=None):
    """
    Get monthly production COGS data grouped by parent product group, optionally filtered by start_date.
    """
    queryset = CalculatedProductionModel.objects.filter(version=scenario)
    
    # Apply start_date filter if provided
    if start_date:
        queryset = queryset.filter(pouring_date__gte=start_date)
    
    queryset = (
        queryset
        .annotate(month=TruncMonth('pouring_date'))
        .values('month', 'parent_product_group')
        .annotate(total_production_cogs=Sum('cogs_aud'))
        .order_by('month', 'parent_product_group')
    )
    
    # Build data structure for Chart.js
    data = {}
    labels_set = set()
    
    for item in queryset:
        month_str = item['month'].strftime('%b %Y')
        group = item['parent_product_group']
        value = item['total_production_cogs'] or 0
        
        labels_set.add(month_str)
        
        if group not in data:
            data[group] = {}
        data[group][month_str] = value
    
    labels = sorted(labels_set, key=lambda d: pd.to_datetime(d, format='%b %Y'))
    
    # Convert to Chart.js format
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)'
    ]
    
    for idx, (group, month_dict) in enumerate(data.items()):
        dataset_data = [month_dict.get(month, 0) for month in labels]
        datasets.append({
            'label': group,
            'data': dataset_data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)].replace('0.6', '1'),
            'borderWidth': 1
        })
    
    return {'labels': labels, 'datasets': datasets}



from sqlalchemy import create_engine, text
from datetime import date, datetime
import pandas as pd
from django.db.models import Sum
from website.models import MasterDataInventory, scenarios

def get_poured_data_by_fy_and_site(scenario, user_inventory_date=None):
    """
    Get poured data from PowerBI database for fiscal years, filtered by inventory date.
    Returns data grouped by fiscal year and site.
    """
    # Database connection
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)

    # Get the inventory date from MasterDataInventory if not provided
    if user_inventory_date is None:
        first_inventory = MasterDataInventory.objects.filter(version=scenario).order_by('date_of_snapshot').first()
        if first_inventory:
            user_inventory_date = first_inventory.date_of_snapshot
        else:
            # If no inventory date, return empty data
            return {}

    # CORRECTED: Define fiscal year ranges to match your system
    fy_ranges = {
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),  # FIXED
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),  # FIXED
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),  # FIXED
    }

    poured_data = {}
    
    with engine.connect() as connection:
        query = text("""
            SELECT 
                hp.CastQty,
                hp.TapTime,
                p.ProductKey AS ProductCode,
                p.DressMass,
                s.SiteName,
                s.Location
            FROM PowerBI.HeatProducts hp
            INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
            INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
            WHERE hp.TapTime IS NOT NULL 
                AND p.DressMass IS NOT NULL 
                AND s.SiteName IN ('MTJ1', 'COI2', 'XUZ1', 'MER1', 'WUN1', 'WOD1', 'CHI1')
                AND hp.TapTime >= :start_date
                AND hp.TapTime <= :end_date
        """)

        for fy, (fy_start, fy_end) in fy_ranges.items():
            # Determine the end date for filtering
            if fy == "FY25":
                # For FY25, use the earlier of user_inventory_date or FY end
                filter_end_date = min(user_inventory_date, fy_end)
                filter_start_date = fy_start
            else:
                # For future FYs, if user_inventory_date is before FY start, skip this FY
                if user_inventory_date < fy_start:
                    poured_data[fy] = {}
                    continue
                # Use the earlier of user_inventory_date or FY end
                filter_end_date = min(user_inventory_date, fy_end)
                filter_start_date = fy_start

            print(f"DEBUG: Querying poured data for {fy}: {filter_start_date} to {filter_end_date}")

            # Execute query for this fiscal year
            result = connection.execute(query, {
                'start_date': filter_start_date,
                'end_date': filter_end_date
            })

            # Process results
            fy_data = {}
            for row in result:
                try:
                    cast_qty = float(row.CastQty) if row.CastQty else 0
                    dress_mass = float(row.DressMass) if row.DressMass else 0
                    site_name = row.SiteName
                    
                    # Calculate tonnes (CastQty * DressMass / 1000)
                    tonnes = (cast_qty * dress_mass) / 1000
                    
                    if site_name not in fy_data:
                        fy_data[site_name] = 0
                    
                    fy_data[site_name] += tonnes
                    
                except (ValueError, TypeError):
                    # Skip rows with invalid data
                    continue

            poured_data[fy] = fy_data
            print(f"DEBUG: Poured data for {fy}: {fy_data}")

    return poured_data

def get_combined_demand_and_poured_data(scenario):
    """
    Get combined demand plan and poured data for control tower.
    Returns demand_plan with poured data added to it.
    """
    # Get the existing demand plan data (from your existing logic)
    sites = ["MTJ1", "COI2", "XUZ1", "MER1", "WUN1", "WOD1", "CHI1"]
    
    # CORRECTED: Use consistent fiscal year ranges
    fy_ranges = {
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),  # FIXED
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),  # FIXED
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),  # FIXED
    }

    # Get demand plan data
    demand_plan = {}
    for fy, (start, end) in fy_ranges.items():
        demand_plan[fy] = {}
        for site in sites:
            total_tonnes = (
                CalculatedProductionModel.objects
                .filter(version=scenario, site__SiteName=site, pouring_date__range=[start, end])
                .aggregate(total=Sum('tonnes'))['total'] or 0
            )
            demand_plan[fy][site] = round(total_tonnes)

    # Get poured data
    poured_data = get_poured_data_by_fy_and_site(scenario)

    # Combine demand plan with poured data
    combined_data = {}
    for fy in fy_ranges.keys():
        combined_data[fy] = {}
        for site in sites:
            demand_tonnes = demand_plan.get(fy, {}).get(site, 0)
            poured_tonnes = poured_data.get(fy, {}).get(site, 0)
            combined_data[fy][site] = round(demand_tonnes + poured_tonnes)

    return combined_data, poured_data



def get_production_data_by_group(site_name, scenario_version):
    """Get production data grouped by product group for a specific site."""
    queryset = (
        CalculatedProductionModel.objects
        .filter(site__SiteName=site_name, version=scenario_version)
        .annotate(month=TruncMonth('pouring_date'))
        .values('month', 'product__ProductGroup')
        .annotate(total_tonnes=Sum('tonnes'))
        .order_by('month', 'product__ProductGroup')
    )
    
    # Build data structure: {group: {month: total}}, labels: [months]
    data = {}
    labels_set = set()
    for entry in queryset:
        month = entry['month'].strftime('%Y-%m')
        group = entry['product__ProductGroup'] or 'Unknown'
        labels_set.add(month)
        data.setdefault(group, {})[month] = entry['total_tonnes']
    labels = sorted(labels_set)
    
    # Filter out product groups with all zero totals
    filtered_data = {}
    for group, month_dict in data.items():
        values = [month_dict.get(label, 0) for label in labels]
        if any(v != 0 for v in values):
            filtered_data[group] = month_dict
    
    # Convert to chart.js format
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)'
    ]
    for idx, (group, month_dict) in enumerate(filtered_data.items()):
        datasets.append({
            'label': group,
            'data': [month_dict.get(label, 0) for label in labels],
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1,
            'stack': 'tonnes'
        })
    
    return {'labels': labels, 'datasets': datasets}

def get_top_products_per_month_by_group(site_name, scenario_version):
    """Get top 10 products per month by group for a specific site."""
    queryset = (
        CalculatedProductionModel.objects
        .filter(site__SiteName=site_name, version=scenario_version)
        .annotate(month=TruncMonth('pouring_date'))
        .values('month', 'product__ProductGroup', 'product__Product')
        .annotate(total_tonnes=Sum('tonnes'))
        .order_by('month', 'product__ProductGroup', '-total_tonnes')
    )
    
    # Structure: {month: {group: [(product, tonnes), ...]}}
    month_group_products = defaultdict(lambda: defaultdict(list))
    for entry in queryset:
        month = entry['month'].strftime('%Y-%m')
        group = entry['product__ProductGroup'] or 'Unknown'
        product = entry['product__Product']
        tonnes = entry['total_tonnes']
        month_group_products[month][group].append((product, tonnes))
    
    # Keep only top 10 per group
    for month in month_group_products:
        for group in month_group_products[month]:
            month_group_products[month][group] = sorted(
                month_group_products[month][group], key=lambda x: x[1], reverse=True
            )[:10]
    
    return month_group_products

def get_dress_mass_data(site_name, scenario_version):
    """Fetch Dress Mass data from MasterDataPlan for the given site and version."""
    from website.models import MasterDataPlan
    
    queryset = MasterDataPlan.objects.filter(
        Foundry__SiteName=site_name, 
        version=scenario_version
    ).order_by('Month')
    
    dress_mass_data = []
    for record in queryset:
        dress_mass = (
            record.AvailableDays
            * record.heatsperdays
            * record.TonsPerHeat
            * record.Yield
            * (1 - (record.WasterPercentage or 0) / 100)
        ) if record.AvailableDays and record.heatsperdays and record.TonsPerHeat and record.Yield else 0
        dress_mass_data.append({'month': record.Month, 'dress_mass': dress_mass})
    
    return dress_mass_data

def get_forecast_data_by_product_group(scenario_version):
    """Get forecast data grouped by product group."""
    queryset = (
        AggregatedForecast.objects
        .filter(version=scenario_version)
        .values('product_group_description', 'period')
        .annotate(total_tonnes=Sum('tonnes'))
        .order_by('product_group_description', 'period')
    )
    data = {}
    labels_set = set()
    for entry in queryset:
        group = entry['product_group_description'] or 'Unknown'
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

def get_forecast_data_by_region(scenario_version):
    """Get forecast data grouped by region."""
    queryset = (
        AggregatedForecast.objects
        .filter(version=scenario_version)
        .values('forecast_region', 'period')
        .annotate(total_tonnes=Sum('tonnes'))
        .order_by('forecast_region', 'period')
    )
    data = {}
    labels_set = set()
    for entry in queryset:
        region = entry['forecast_region'] or 'Unknown'
        period = entry['period'].strftime('%Y-%m')
        labels_set.add(period)
        data.setdefault(region, {})[period] = entry['total_tonnes']
    labels = sorted(labels_set)
    chart_data = {}
    for region, period_dict in data.items():
        chart_data[region] = {
            'labels': labels,
            'tons': [period_dict.get(label, 0) for label in labels]
        }
    return chart_data

def get_monthly_pour_plan_for_site(site_name, scenario_version, chart_labels):
    """Get monthly pour plan data for a specific site."""
    from website.models import MasterDataPlan
    
    monthly_pour_plan = []
    for month in chart_labels:
        month_date = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year + 1, month=1, day=1)
        else:
            next_month = month_date.replace(month=month_date.month + 1, day=1)
        
        plans = MasterDataPlan.objects.filter(
            version=scenario_version,
            Foundry__SiteName=site_name,
            Month__gte=month_date,
            Month__lt=next_month
        )
        value = sum(plan.PlanDressMass for plan in plans)
        monthly_pour_plan.append(round(value))
    
    return monthly_pour_plan

# ...existing code...

def build_detailed_monthly_table(fy, site, scenario_version):
    """Build detailed monthly table showing the actual breakdown that makes up the combined total."""
    fy_ranges = {
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),
    }
    
    start, end = fy_ranges[fy]
    
    # Get monthly demand plan data (CalculatedProductionModel)
    demand_monthly = (
        CalculatedProductionModel.objects
        .filter(
            version=scenario_version,
            site__SiteName=site,
            pouring_date__gte=start,
            pouring_date__lte=end
        )
        .annotate(month=TruncMonth('pouring_date'))
        .values('month')
        .annotate(total=Sum('tonnes'))
        .order_by('month')
    )
    
    # Convert to dict for easy lookup
    demand_dict = {row['month'].strftime('%b %Y'): row['total'] or 0 for row in demand_monthly}
    print(f"DEBUG: Demand dict for {fy}/{site}: {demand_dict}")
    
    # Get monthly poured data (from PowerBI database)
    poured_monthly = get_monthly_poured_data_for_site_and_fy(site, fy, scenario_version)
    print(f"DEBUG: Poured monthly for {fy}/{site}: {poured_monthly}")
    
    # Generate all months in the fiscal year
    current_month = start
    months_data = []
    total_demand = 0
    total_poured = 0
    total_combined = 0
    
    while current_month <= end:
        month_str = current_month.strftime('%b %Y')
        demand_qty = demand_dict.get(month_str, 0)
        poured_qty = poured_monthly.get(month_str, 0)
        combined_qty = demand_qty + poured_qty
        
        months_data.append({
            'month': month_str,
            'demand': round(demand_qty),
            'poured': round(poured_qty),
            'combined': round(combined_qty)
        })
        
        total_demand += demand_qty
        total_poured += poured_qty
        total_combined += combined_qty
        
        print(f"DEBUG: {month_str} - Demand: {demand_qty}, Poured: {poured_qty}, Combined: {combined_qty}")
        
        # Move to next month
        if current_month.month == 12:
            current_month = current_month.replace(year=current_month.year + 1, month=1)
        else:
            current_month = current_month.replace(month=current_month.month + 1)
    
    # Build HTML table
    table = """
    <table class='table table-sm table-bordered mb-0' style='font-size: 12px;'>
        <thead style='background-color: #f8f9fa;'>
            <tr>
                <th style='text-align: left; padding: 4px 8px;'>Month</th>
                <th style='text-align: right; padding: 4px 8px;'>Demand Plan</th>
                <th style='text-align: right; padding: 4px 8px;'>Actual Poured</th>
                <th style='text-align: right; padding: 4px 8px;'>Combined</th>
            </tr>
        </thead>
        <tbody>
    """
    
    for month_data in months_data:
        table += f"""
            <tr>
                <td style='text-align: left; padding: 4px 8px;'>{month_data['month']}</td>
                <td style='text-align: right; padding: 4px 8px;'>{month_data['demand']:,}</td>
                <td style='text-align: right; padding: 4px 8px;'>{month_data['poured']:,}</td>
                <td style='text-align: right; padding: 4px 8px;'><strong>{month_data['combined']:,}</strong></td>
            </tr>
        """
    
    # Add totals row
    table += f"""
        </tbody>
        <tfoot style='background-color: #e9ecef; font-weight: bold;'>
            <tr>
                <td style='text-align: left; padding: 4px 8px;'>Total</td>
                <td style='text-align: right; padding: 4px 8px;'>{round(total_demand):,}</td>
                <td style='text-align: right; padding: 4px 8px;'>{round(total_poured):,}</td>
                <td style='text-align: right; padding: 4px 8px;'><strong>{round(total_combined):,}</strong></td>
            </tr>
        </tfoot>
    </table>
    """
    
    print(f"DEBUG: Built table for {fy}/{site} - Demand: {total_demand}, Poured: {total_poured}, Combined: {total_combined}")
    
    # Clean up the HTML for JSON encoding
    table = table.replace('\n', '').replace('\r', '').replace('"', '&quot;')
    return mark_safe(table)

def calculate_control_tower_data(scenario_version):
    """Calculate control tower data including demand plan and pour plan."""
    print(f"DEBUG: Starting calculate_control_tower_data for scenario: {scenario_version}")
    
    from website.models import MasterDataPlan
    
    sites = ["MTJ1", "COI2", "XUZ1", "MER1", "WUN1", "WOD1", "CHI1"]
    
    # CORRECTED: Use consistent fiscal year ranges
    fy_ranges = {
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),  # FIXED
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),  # FIXED
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),  # FIXED
    }

    # Get combined demand and poured data (this gives you the 22,925)
    combined_demand_plan, poured_data = get_combined_demand_and_poured_data(scenario_version)

    # Calculate pour plan
    pour_plan = {}
    for fy, (start, end) in fy_ranges.items():
        pour_plan[fy] = {}
        for site in sites:
            plans = MasterDataPlan.objects.filter(
                version=scenario_version,
                Foundry__SiteName=site,
                Month__gte=start,
                Month__lte=end
            )
            total = sum(plan.PlanDressMass for plan in plans)
            pour_plan[fy][site] = round(total)

    # Build detailed monthly table HTML for all FY/site combinations
    detailed_monthly_table_html = {}
    for fy in fy_ranges.keys():
        detailed_monthly_table_html[fy] = {}
        for site in sites:
            try:
                detailed_monthly_table_html[fy][site] = build_detailed_monthly_table(fy, site, scenario_version)
                print(f"DEBUG: Created table for {fy}/{site}")
            except Exception as e:
                print(f"DEBUG ERROR: Failed to create table for {fy}/{site}: {e}")
                detailed_monthly_table_html[fy][site] = f"Error creating table for {site} in {fy}: {str(e)}"

    print(f"DEBUG: Detailed monthly tables created for {len(detailed_monthly_table_html)} fiscal years")

    return {
        'combined_demand_plan': combined_demand_plan,  # This is the 22,925
        'poured_data': poured_data,
        'pour_plan': pour_plan,
        'detailed_monthly_table_html': detailed_monthly_table_html  # This should now match the 22,925
    }

def get_monthly_poured_data_for_site_and_fy(site, fy, scenario_version):
    """Get monthly poured data for a specific site and fiscal year."""
    from website.models import MasterDataInventory
    from datetime import timedelta
    
    print(f"DEBUG: Getting poured data for {site} in {fy}")
    
    # Get inventory date for filtering
    first_inventory = MasterDataInventory.objects.filter(version=scenario_version).order_by('date_of_snapshot').first()
    if not first_inventory:
        print(f"DEBUG: No inventory data found for scenario {scenario_version}")
        return {}
    
    inventory_date = first_inventory.date_of_snapshot
    print(f"DEBUG: Inventory date: {inventory_date}")
    
    # CORRECTED: Define fiscal year ranges to match your system
    fy_ranges = {
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),  # FIXED
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),  # FIXED
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),  # FIXED
    }
    
    fy_start, fy_end = fy_ranges[fy]
    filter_end_date = min(inventory_date, fy_end)
    
    print(f"DEBUG: FY range: {fy_start} to {fy_end}")
    print(f"DEBUG: Filter end date: {filter_end_date}")
    
    if inventory_date < fy_start:
        print(f"DEBUG: Inventory date {inventory_date} is before FY start {fy_start}")
        return {}
    
    # Database connection and query
    try:
        Server = 'bknew-sql02'
        Database = 'Bradken_Data_Warehouse'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        engine = create_engine(Database_Con)
        print(f"DEBUG: Database connection created")
        
        monthly_data = {}
        
        with engine.connect() as connection:
            # Let's first test if we can find any data for this site
            test_query = text("""
                SELECT COUNT(*) as total_records
                FROM PowerBI.HeatProducts hp
                INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                WHERE s.SiteName = :site_name
            """)
            
            test_result = connection.execute(test_query, {'site_name': site})
            total_records = test_result.fetchone().total_records
            print(f"DEBUG: Total records found for site {site}: {total_records}")
            
            if total_records == 0:
                print(f"DEBUG: No records found for site {site}. Checking available sites...")
                
                # Check what sites are available
                sites_query = text("SELECT DISTINCT SiteName FROM PowerBI.Site ORDER BY SiteName")
                sites_result = connection.execute(sites_query)
                available_sites = [row.SiteName for row in sites_result]
                print(f"DEBUG: Available sites: {available_sites}")
                return {}
            
            # Main query with debugging
            query = text("""
                SELECT 
                    YEAR(hp.TapTime) as TapYear,
                    MONTH(hp.TapTime) as TapMonth,
                    COUNT(*) as RecordCount,
                    SUM(hp.CastQty * p.DressMass / 1000) as MonthlyTonnes,
                    MIN(hp.TapTime) as MinDate,
                    MAX(hp.TapTime) as MaxDate
                FROM PowerBI.HeatProducts hp
                INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                WHERE s.SiteName = :site_name
                    AND hp.TapTime >= :start_date
                    AND hp.TapTime <= :end_date
                    AND hp.TapTime IS NOT NULL 
                    AND p.DressMass IS NOT NULL
                    AND hp.CastQty IS NOT NULL
                GROUP BY YEAR(hp.TapTime), MONTH(hp.TapTime)
                ORDER BY TapYear, TapMonth
            """)
            
            print(f"DEBUG: Executing query for site {site} from {fy_start} to {filter_end_date}")
            
            result = connection.execute(query, {
                'site_name': site,
                'start_date': fy_start,
                'end_date': filter_end_date
            })
            
            row_count = 0
            for row in result:
                month_date = date(row.TapYear, row.TapMonth, 1)
                month_str = month_date.strftime('%b %Y')
                monthly_tonnes = round(row.MonthlyTonnes or 0)
                monthly_data[month_str] = monthly_tonnes
                
                print(f"DEBUG: {month_str}: {monthly_tonnes} tonnes (from {row.RecordCount} records, dates: {row.MinDate} to {row.MaxDate})")
                row_count += 1
            
            print(f"DEBUG: Query returned {row_count} months of data for {site}")
            
            # If no data found, let's check for any data in a wider date range
            if row_count == 0:
                print(f"DEBUG: No data found in specified range. Checking what data exists for {site}...")
                
                wider_query = text("""
                    SELECT 
                        MIN(hp.TapTime) as EarliestDate,
                        MAX(hp.TapTime) as LatestDate,
                        COUNT(*) as TotalRecords
                    FROM PowerBI.HeatProducts hp
                    INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                    INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                    WHERE s.SiteName = :site_name
                        AND hp.TapTime IS NOT NULL 
                        AND p.DressMass IS NOT NULL
                        AND hp.CastQty IS NOT NULL
                """)
                
                wider_result = connection.execute(wider_query, {'site_name': site})
                wider_row = wider_result.fetchone()
                if wider_row:
                    print(f"DEBUG: Available data for {site}: {wider_row.EarliestDate} to {wider_row.LatestDate} ({wider_row.TotalRecords} records)")
        
        print(f"DEBUG: Final monthly data for {site}: {monthly_data}")
        return monthly_data
        
    except Exception as e:
        print(f"DEBUG ERROR: Database query failed for {site}/{fy}: {e}")
        print(f"DEBUG ERROR: Error type: {type(e).__name__}")
        import traceback
        print(f"DEBUG ERROR: Traceback: {traceback.format_exc()}")
        return {}

def get_inventory_data_with_start_date(scenario_version):
    """Get inventory data with proper start date filtering."""
    from website.models import MasterDataInventory
    from datetime import timedelta
    from collections import defaultdict
    
    # Get the first inventory snapshot date and calculate start date
    first_inventory = MasterDataInventory.objects.filter(version=scenario_version).order_by('date_of_snapshot').first()
    if first_inventory:
        next_day = first_inventory.date_of_snapshot + timedelta(days=1)
        start_date = next_day.replace(day=1)
    else:
        start_date = None

    # Get months for each series with start_date filtering
    months_cogs, cogs, revenue = get_monthly_cogs_and_revenue(scenario_version, start_date=start_date)
    months_prod, production_cogs = get_monthly_production_cogs(scenario_version, start_date=start_date)

    # Find the last month in your data (from all series)
    all_dates = []
    if months_cogs:
        all_dates.extend([pd.to_datetime(m, format='%b %Y') for m in months_cogs])
    if months_prod:
        all_dates.extend([pd.to_datetime(m, format='%b %Y') for m in months_prod])
    
    if all_dates and start_date:
        # Build all months from start_date to end_date
        end_date = max(all_dates)
        all_months = pd.date_range(start=start_date, end=end_date, freq='MS').strftime('%b %Y').tolist()
        
        # Align COGS and production data to all_months
        cogs_map = dict(zip(months_cogs, cogs)) if months_cogs else {}
        revenue_map = dict(zip(months_cogs, revenue)) if months_cogs else {}
        prod_map = dict(zip(months_prod, production_cogs)) if months_prod else {}
        
        cogs_aligned = [cogs_map.get(m, 0) for m in all_months]
        revenue_aligned = [revenue_map.get(m, 0) for m in all_months]
        prod_aligned = [prod_map.get(m, 0) for m in all_months]
    else:
        all_months = months_cogs or months_prod or []
        cogs_aligned = cogs or []
        revenue_aligned = revenue or []
        prod_aligned = production_cogs or []

    # Get production data by parent group
    production_cogs_group_chart = get_monthly_production_cogs_by_parent_group(scenario_version, start_date=start_date)

    # Process group data
    parent_groups = AggregatedForecast.objects.filter(version=scenario_version).values_list('parent_product_group_description', flat=True).distinct()
    cogs_data_by_group = defaultdict(lambda: {'months': [], 'cogs': [], 'revenue': [], 'production_aud': []})

    for group in parent_groups:
        # Aggregate COGS and Revenue from AggregatedForecast with date filtering
        agg_qs = AggregatedForecast.objects.filter(version=scenario_version, parent_product_group_description=group)
        
        if start_date:
            agg_qs = agg_qs.filter(period__gte=start_date)
            
        agg_qs = (
            agg_qs
            .annotate(month=TruncMonth('period'))
            .values('month')
            .annotate(
                total_cogs=Sum('cogs_aud'),
                total_revenue=Sum('revenue_aud')
            )
            .order_by('month')
        )
        
        months = [d['month'].strftime('%b %Y') for d in agg_qs]
        cogs = [d['total_cogs'] for d in agg_qs]
        revenue = [d['total_revenue'] for d in agg_qs]

        # Aggregate Production AUD from CalculatedProductionModel with date filtering
        prod_qs = CalculatedProductionModel.objects.filter(version=scenario_version, parent_product_group=group)
        
        if start_date:
            prod_qs = prod_qs.filter(pouring_date__gte=start_date)
            
        prod_qs = (
            prod_qs
            .annotate(month=TruncMonth('pouring_date'))
            .values('month')
            .annotate(total_production_aud=Sum('cogs_aud'))
            .order_by('month')
        )
        
        prod_months = [d['month'].strftime('%b %Y') for d in prod_qs]
        production_aud = [d['total_production_aud'] for d in prod_qs]

        # Union of all months for this group
        all_months_group = sorted(set(months) | set(prod_months), key=lambda d: pd.to_datetime(d, format='%b %Y'))

        # Align all series to all_months_group
        cogs_map = dict(zip(months, cogs))
        revenue_map = dict(zip(months, revenue))
        prod_map = dict(zip(prod_months, production_aud))

        cogs_aligned_group = [cogs_map.get(m, 0) for m in all_months_group]
        revenue_aligned_group = [revenue_map.get(m, 0) for m in all_months_group]
        prod_aligned_group = [prod_map.get(m, 0) for m in all_months_group]

        cogs_data_by_group[group]['months'] = all_months_group
        cogs_data_by_group[group]['cogs'] = cogs_aligned_group
        cogs_data_by_group[group]['revenue'] = revenue_aligned_group
        cogs_data_by_group[group]['production_aud'] = prod_aligned_group

    # Collect all unique months across all groups
    all_unique_months = set()
    for group_data in cogs_data_by_group.values():
        all_unique_months.update(group_data['months'])
    
    all_unique_months = sorted(all_unique_months, key=lambda d: pd.to_datetime(d, format='%b %Y'))

    # Re-align every group's data to all_unique_months
    for group, group_data in cogs_data_by_group.items():
        months = group_data['months']
        cogs_map = dict(zip(months, group_data['cogs']))
        revenue_map = dict(zip(months, group_data['revenue']))
        prod_map = dict(zip(months, group_data['production_aud']))
        group_data['months'] = all_unique_months
        group_data['cogs'] = [cogs_map.get(m, 0) for m in all_unique_months]
        group_data['revenue'] = [revenue_map.get(m, 0) for m in all_unique_months]
        group_data['production_aud'] = [prod_map.get(m, 0) for m in all_unique_months]

    return {
        'inventory_months': all_months,
        'inventory_cogs': cogs_aligned,
        'inventory_revenue': revenue_aligned,
        'production_aud': prod_aligned,
        'production_cogs_group_chart': production_cogs_group_chart,
        'parent_product_groups': list(parent_groups),
        'cogs_data_by_group': cogs_data_by_group,
    }

def get_foundry_chart_data(scenario_version):
    """Get chart data for all foundries."""
    foundries = ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1']
    foundry_data = {}
    
    for foundry in foundries:
        # Special handling for WUN1 to show products instead of product groups
        if foundry == 'WUN1':
            chart_data = get_production_data_by_product_for_wun1(foundry, scenario_version)
            # For WUN1, top_products should just be the product names from the chart
            top_products = [dataset['label'] for dataset in chart_data['datasets']]
        else:
            chart_data = get_production_data_by_group(foundry, scenario_version)
            top_products = get_top_products_per_month_by_group(foundry, scenario_version)
        
        monthly_pour_plan = get_monthly_pour_plan_for_site(foundry, scenario_version, chart_data['labels'])
        
        foundry_data[foundry] = {
            'chart_data': chart_data,
            'top_products': json.dumps(top_products),
            'monthly_pour_plan': monthly_pour_plan
        }
    
    return foundry_data

# ...existing code...



def get_forecast_data_by_customer(scenario_version):
    """Get forecast data grouped by customer code."""
    print(f"DEBUG: Getting customer data for scenario: {scenario_version}")
    
    # Try AggregatedForecast first
    queryset = (
        AggregatedForecast.objects
        .filter(version=scenario_version)
        .values('customer_code', 'period')
        .annotate(total_tonnes=Sum('tonnes'))
        .order_by('customer_code', 'period')
    )
    
    print(f"DEBUG: AggregatedForecast customer query returned {queryset.count()} records")
    
    if queryset.exists():
        # Build data structure from AggregatedForecast
        data = {}
        labels_set = set()
        for entry in queryset:
            customer = entry['customer_code'] or 'Unknown'
            period = entry['period'].strftime('%Y-%m')
            labels_set.add(period)
            data.setdefault(customer, {})[period] = entry['total_tonnes']
        
        labels = sorted(labels_set)
        chart_data = {}
        for customer, period_dict in data.items():
            chart_data[customer] = {
                'labels': labels,
                'tons': [period_dict.get(label, 0) for label in labels]
            }
    else:
        print(f"DEBUG: No data in AggregatedForecast, trying SMART_Forecast_Model")
        
        # Fall back to SMART_Forecast_Model
        queryset = (
            SMART_Forecast_Model.objects
            .filter(version=scenario_version)
            .values('Customer_code', 'Period_AU')
            .annotate(total_qty=Sum('Qty'))
            .order_by('Customer_code', 'Period_AU')
        )
        
        print(f"DEBUG: SMART_Forecast_Model customer query returned {queryset.count()} records")
        
        data = {}
        labels_set = set()
        for entry in queryset:
            customer = entry['Customer_code'] or 'Unknown'
            period = entry['Period_AU'].strftime('%Y-%m') if entry['Period_AU'] else 'Unknown'
            qty = entry['total_qty'] or 0
            labels_set.add(period)
            data.setdefault(customer, {})[period] = qty
        
        labels = sorted(labels_set)
        chart_data = {}
        for customer, period_dict in data.items():
            chart_data[customer] = {
                'labels': labels,
                'tons': [period_dict.get(label, 0) for label in labels]
            }
    
    print(f"DEBUG: Customer chart data: {len(chart_data)} customers, {len(labels) if 'labels' in locals() else 0} periods")
    for customer, customer_data in list(chart_data.items())[:3]:  # Show first 3
        total_tons = sum(customer_data['tons'])
        print(f"DEBUG: Customer '{customer}': {total_tons} total tons")
    
    return chart_data

def get_forecast_data_by_data_source(scenario_version):
    """Get forecast data grouped by data source."""
    print(f"DEBUG: Getting data source data for scenario: {scenario_version}")
    
    # Get data from SMART_Forecast_Model grouped by Data_Source
    queryset = (
        SMART_Forecast_Model.objects
        .filter(version=scenario_version)
        .values('Data_Source', 'Period_AU')
        .annotate(total_qty=Sum('Qty'))
        .order_by('Data_Source', 'Period_AU')
    )
    
    print(f"DEBUG: SMART_Forecast_Model data source query returned {queryset.count()} records")
    
    data = {}
    labels_set = set()
    for entry in queryset:
        data_source = entry['Data_Source'] or 'Unknown'
        period = entry['Period_AU'].strftime('%Y-%m') if entry['Period_AU'] else 'Unknown'
        qty = entry['total_qty'] or 0
        labels_set.add(period)
        data.setdefault(data_source, {})[period] = qty
    
    labels = sorted(labels_set)
    chart_data = {}
    for data_source, period_dict in data.items():
        chart_data[data_source] = {
            'labels': labels,
            'tons': [period_dict.get(label, 0) for label in labels]
        }
    
    print(f"DEBUG: Data source chart data: {len(chart_data)} data sources, {len(labels)} periods")
    for data_source, ds_data in chart_data.items():
        total_tons = sum(ds_data['tons'])
        print(f"DEBUG: Data Source '{data_source}': {total_tons} total tons")
    
    return chart_data



def get_production_data_by_product_for_wun1(site_name, scenario_version):
    """Get production data grouped by individual products for WUN1 only."""
    queryset = (
        CalculatedProductionModel.objects
        .filter(site__SiteName=site_name, version=scenario_version)
        .annotate(month=TruncMonth('pouring_date'))
        .values('month', 'product__Product')  # Group by individual product
        .annotate(total_tonnes=Sum('tonnes'))
        .order_by('month', 'product__Product')
    )
    
    # Build data structure: {product: {month: total}}, labels: [months]
    data = {}
    labels_set = set()
    for entry in queryset:
        month = entry['month'].strftime('%Y-%m')
        product = entry['product__Product'] or 'Unknown'
        labels_set.add(month)
        data.setdefault(product, {})[month] = entry['total_tonnes']
    labels = sorted(labels_set)
    
    # Filter out products with all zero totals
    filtered_data = {}
    for product, month_dict in data.items():
        values = [month_dict.get(label, 0) for label in labels]
        if any(v != 0 for v in values):
            filtered_data[product] = month_dict
    
    # Convert to chart.js format
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)',
        'rgba(255,159,64,0.6)', 'rgba(201,203,207,0.6)', 'rgba(75,192,192,0.6)'
    ]
    for idx, (product, month_dict) in enumerate(filtered_data.items()):
        datasets.append({
            'label': product,  # Individual product name
            'data': [month_dict.get(label, 0) for label in labels],
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1,
            'stack': 'tonnes'
        })
    
    return {'labels': labels, 'datasets': datasets}

def translate_to_english_cached(text):
    """
    Translate text to English with caching and manual translations.
    """
    if not text or str(text).strip() == '':
        return text
    
    # Convert to string and strip
    text_str = str(text).strip()
    
    # Check manual translations first
    if text_str in MANUAL_TRANSLATIONS:
        print(f"DEBUG: Manual translation: '{text_str}' -> '{MANUAL_TRANSLATIONS[text_str]}'")
        return MANUAL_TRANSLATIONS[text_str]
    
    # Check cache
    if text_str in TRANSLATION_CACHE:
        return TRANSLATION_CACHE[text_str]
    
    try:
        # Detect the language
        detected_lang = langdetect.detect(text_str)
        
        # If it's French, translate to English
        if detected_lang == 'fr':
            translator = Translator()
            translated = translator.translate(text_str, src='fr', dest='en')
            result = translated.text
        else:
            # Return original text if not French
            result = text_str
            
        # Cache the result
        TRANSLATION_CACHE[text_str] = result
        return result
        
    except Exception as e:
        print(f"Translation error for '{text_str}': {e}")
        # Cache the original text as fallback
        TRANSLATION_CACHE[text_str] = text_str
        return text_str
    
def search_detailed_view_data(scenario_version, product=None, location=None, site=None):
    """
    Search for specific detailed inventory and production data based on filters.
    Uses ACTUAL production data from CalculatedProductionModel and replenishment data from CalcualtedReplenishmentModel
    """
    from website.models import (
        MasterDataInventory, SMART_Forecast_Model, CalcualtedReplenishmentModel,
        CalculatedProductionModel, MasterDataProductModel, MasterDataPlantModel,
        MasterdataIncoTermsModel, MasterDataFreightModel, MasterDataCustomersModel,
        MasterDataCastToDespatchModel, MasterDataOrderBook, MasterDataHistoryOfProductionModel,
        MasterDataEpicorSupplierMasterDataModel, MasterDataEpicorMethodOfManufacturingModel
    )
    from django.db.models import Sum, Q
    from collections import defaultdict
    from datetime import timedelta
    
    print(f"DEBUG: Starting OPTIMIZED search with scenario_version: {scenario_version}")
    
    inventory_data = []
    production_data = []
    
    # Early exit if no search criteria
    if not product and not location:
        print("DEBUG: No search criteria provided, returning empty results")
        return {'inventory_data': inventory_data, 'production_data': production_data}
    
    # Define location transformation function
    def _transform_location(location):
        """Transform location strings by extracting the site code."""
        if location:
            if "_" in location:
                return location.split("_", 1)[1][:4]
            elif "-" in location:
                return location.split("-", 1)[1][:4]
        return location
    
    # SIMPLIFIED site assignment - just get the specific product we're searching for
    def _get_assigned_site_for_product(product):
        """Get assigned site for ONE specific product only."""
        print(f"DEBUG: Getting site assignment for {product}")
        
        try:
            # Check Order Book first
            order_book = MasterDataOrderBook.objects.filter(
                version=scenario_version,
                productkey=product
            ).exclude(site__isnull=True).exclude(site__exact='').first()
            
            if order_book:
                print(f"DEBUG: Found in order book: {order_book.site}")
                return order_book.site
            
            # Check Production History
            production = MasterDataHistoryOfProductionModel.objects.filter(
                version=scenario_version,
                Product=product
            ).exclude(Foundry__isnull=True).exclude(Foundry__exact='').first()
            
            if production:
                print(f"DEBUG: Found in production history: {production.Foundry}")
                return production.Foundry
            
            # Check Supplier
            supplier = MasterDataEpicorSupplierMasterDataModel.objects.filter(
                version=scenario_version,
                PartNum=product
            ).exclude(VendorID__isnull=True).exclude(VendorID__exact='').first()
            
            if supplier:
                print(f"DEBUG: Found in supplier: {supplier.VendorID}")
                return supplier.VendorID
            
            print(f"DEBUG: No site assignment found for {product}")
            return None
            
        except Exception as e:
            print(f"DEBUG ERROR: Site assignment failed for {product}: {e}")
            return None
    
    # Build inventory data with filters (ONLY if location is specified)
    if product and location:
        try:
            # Get forecast data - FILTERED BY PRODUCT AND LOCATION
            forecast_query = SMART_Forecast_Model.objects.filter(version=scenario_version)
            forecast_query = forecast_query.exclude(Data_Source__in=['Fixed Plant', 'Revenue Forecast'])
            forecast_query = forecast_query.filter(Product=product)
            forecast_query = forecast_query.filter(Location__icontains=location)
            
            forecast_data = forecast_query.values('Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code', 'Qty')
            print(f"DEBUG: Found {len(forecast_data)} forecast records for {product} at {location}")
            
            if forecast_data:
                # Get the specific assigned site for the product
                assigned_site = _get_assigned_site_for_product(product)
                print(f"DEBUG: Assigned site: {assigned_site}")
                
                # [SAME FORECAST PROCESSING LOGIC AS BEFORE FOR INVENTORY TABLE...]
                # Load freight and incoterm data
                forecast_regions = [f['Forecast_Region'] for f in forecast_data if f['Forecast_Region']]
                unique_regions = list(set(forecast_regions))
                
                freight_data = {}
                if assigned_site and unique_regions:
                    try:
                        freight_models = MasterDataFreightModel.objects.filter(
                            version=scenario_version,
                            ForecastRegion__Forecast_region__in=unique_regions,
                            ManufacturingSite__SiteName=assigned_site
                        )
                        
                        for freight in freight_models:
                            key = (scenario_version.version, freight.ForecastRegion.Forecast_region, freight.ManufacturingSite.SiteName)
                            freight_data[key] = freight
                        
                        print(f"DEBUG: Loaded {len(freight_data)} freight mappings for site {assigned_site}")
                    except Exception as freight_error:
                        print(f"DEBUG ERROR: Freight data loading failed: {freight_error}")
                
                customer_codes = [f['Customer_code'] for f in forecast_data if f['Customer_code']]
                unique_customers = list(set(customer_codes))
                
                incoterm_data = {}
                if unique_customers:
                    try:
                        incoterm_models = MasterdataIncoTermsModel.objects.filter(
                            version=scenario_version,
                            CustomerCode__in=unique_customers
                        ).exclude(Incoterm__isnull=True)
                        
                        for incoterm in incoterm_models:
                            key = (scenario_version.version, incoterm.CustomerCode)
                            incoterm_data[key] = incoterm
                        
                        print(f"DEBUG: Loaded {len(incoterm_data)} incoterm mappings")
                    except Exception as incoterm_error:
                        print(f"DEBUG ERROR: Incoterm data loading failed: {incoterm_error}")
                
                # Process forecast data for inventory table
                product_locations = {}
                adjusted_shipping_by_product_location = {}
                
                for forecast in forecast_data:
                    
                    prod = forecast['Product']
                    orig_loc = forecast['Location']
                    customer_code = forecast['Customer_code']
                    period_au = forecast['Period_AU']
                    forecast_region = forecast['Forecast_Region']
                    qty = forecast['Qty'] or 0
                    
                    # Transform location
                    transformed_loc = _transform_location(orig_loc)
                    
                    # Calculate adjusted shipping date
                    adjusted_shipping_date = period_au
                    
                    # Get freight lead time
                    freight_info = {
                        'total_days': 0,
                        'plant_to_port': 0,
                        'ocean_freight': 0,
                        'port_to_customer': 0,
                        'manufacturing_site': assigned_site or 'Unknown'
                    }
                    
                    if assigned_site and customer_code and forecast_region:
                        # Get incoterm
                        incoterm_key = (scenario_version.version, customer_code)
                        incoterm_obj = incoterm_data.get(incoterm_key)
                        
                        if incoterm_obj:
                            incoterm_category = incoterm_obj.Incoterm.IncoTermCaregory
                            
                            # Get freight data
                            freight_key = (scenario_version.version, forecast_region, assigned_site)
                            freight_obj = freight_data.get(freight_key)
                            
                            if freight_obj:
                                lead_time_days = 0
                                
                                if incoterm_category == "NO FREIGHT":
                                    lead_time_days = 0
                                elif incoterm_category == "PLANT TO DOMESTIC PORT":
                                    lead_time_days = freight_obj.PlantToDomesticPortDays or 0
                                elif incoterm_category == "PLANT TO DOMESTIC PORT + INT FREIGHT":
                                    lead_time_days = (freight_obj.PlantToDomesticPortDays or 0) + (freight_obj.OceanFreightDays or 0)
                                elif incoterm_category == "PLANT TO DOMESTIC PORT + INT FREIGHT + DOM FREIGHT":
                                    lead_time_days = ((freight_obj.PlantToDomesticPortDays or 0) + 
                                                    (freight_obj.OceanFreightDays or 0) + 
                                                    (freight_obj.PortToCustomerDays or 0))
                                
                                if lead_time_days > 0:
                                    adjusted_shipping_date = period_au - timedelta(days=lead_time_days)
                                
                                freight_info = {
                                    'total_days': lead_time_days,
                                    'plant_to_port': freight_obj.PlantToDomesticPortDays or 0,
                                    'ocean_freight': freight_obj.OceanFreightDays or 0,
                                    'port_to_customer': freight_obj.PortToCustomerDays or 0,
                                    'manufacturing_site': assigned_site
                                }
                    
                    # Store the data
                    if prod not in product_locations:
                        product_locations[prod] = {}
                    if transformed_loc not in product_locations[prod]:
                        product_locations[prod][transformed_loc] = {
                            'customer_codes': set(),
                            'original_locations': set(),
                            'freight_info': freight_info
                        }
                    product_locations[prod][transformed_loc]['customer_codes'].add(customer_code)
                    product_locations[prod][transformed_loc]['original_locations'].add(orig_loc)
                    
                    # Store adjusted shipping quantities
                    key = (prod, transformed_loc)
                    if key not in adjusted_shipping_by_product_location:
                        adjusted_shipping_by_product_location[key] = {}
                    if adjusted_shipping_date not in adjusted_shipping_by_product_location[key]:
                        adjusted_shipping_by_product_location[key][adjusted_shipping_date] = 0
                    adjusted_shipping_by_product_location[key][adjusted_shipping_date] += qty
                
                # Get replenishment data for THIS SPECIFIC LOCATION
                replenishment_query = CalcualtedReplenishmentModel.objects.filter(
                    version=scenario_version,
                    Product__Product=product,
                    Location=location
                )
                
                replenishment_data = replenishment_query.values(
                    'Product__Product', 'Location', 'ShippingDate', 'Site__SiteName', 'ReplenishmentQty'
                )
                
                # Get cast to despatch data
                sites_needed = [repl['Site__SiteName'] for repl in replenishment_data if repl['Site__SiteName']]
                unique_sites = list(set(sites_needed))
                
                cast_to_despatch_lookup = {}
                if unique_sites:
                    cast_entries = MasterDataCastToDespatchModel.objects.filter(
                        version=scenario_version,
                        Foundry__SiteName__in=unique_sites
                    )
                    for entry in cast_entries:
                        cast_to_despatch_lookup[entry.Foundry.SiteName] = entry.CastToDespatchDays
                
                # Build inventory table data
                for prod, locations in product_locations.items():
                    for loc, loc_info in locations.items():
                        # Get opening stock
                        opening_stock = MasterDataInventory.objects.filter(
                            version=scenario_version,
                            product=prod,
                            site_id=loc
                        ).aggregate(
                            onhand=Sum('onhandstock_qty'),
                            intransit=Sum('intransitstock_qty'),
                            wip=Sum('wip_stock_qty')
                        )
                        
                        # Get adjusted shipping quantities
                        key = (prod, loc)
                        adjusted_shipping_periods = adjusted_shipping_by_product_location.get(key, {})
                        
                        # Get replenishment data for this location
                        replenishment_by_shipping_date = {}
                        for repl in replenishment_data:
                            if repl['Product__Product'] == prod and repl['Location'] == loc:
                                shipping_date = repl['ShippingDate']
                                site_name = repl['Site__SiteName']
                                
                                cast_to_despatch_days = cast_to_despatch_lookup.get(site_name, 0)
                                pouring_date = shipping_date - timedelta(days=cast_to_despatch_days)
                                
                                if shipping_date not in replenishment_by_shipping_date:
                                    replenishment_by_shipping_date[shipping_date] = []
                                replenishment_by_shipping_date[shipping_date].append({
                                    'site': site_name,
                                    'qty': repl['ReplenishmentQty'],
                                    'shipping_date': shipping_date,
                                    'pouring_date': pouring_date,
                                    'cast_to_despatch_days': cast_to_despatch_days,
                                    'source_location': loc
                                })

                        # Build period data for inventory
                        all_shipping_dates = set(adjusted_shipping_periods.keys()) | set(replenishment_by_shipping_date.keys())
                        period_data = []
                        running_balance = (opening_stock['onhand'] or 0) + (opening_stock['intransit'] or 0)

                        for shipping_date in sorted(all_shipping_dates):
                            original_shipping_qty = adjusted_shipping_periods.get(shipping_date, 0)
                            replenishment_list = replenishment_by_shipping_date.get(shipping_date, [])
                            total_replenishment = sum(r['qty'] for r in replenishment_list)
                            running_balance = running_balance + total_replenishment - original_shipping_qty
                            
                            if original_shipping_qty > 0 or total_replenishment > 0:
                                # GROUP REPLENISHMENTS BY SITE AND SUM QUANTITIES
                                replenishment_summary = {}
                                for r in replenishment_list:
                                    site = r['site']
                                    if site not in replenishment_summary:
                                        replenishment_summary[site] = 0
                                    replenishment_summary[site] += r['qty']
                                
                                # CREATE SUMMARIZED REPLENISHMENT TEXT
                                replenishment_text = ""
                                if replenishment_summary:
                                    site_summaries = []
                                    for site, total_qty in replenishment_summary.items():
                                        site_summaries.append(f"{total_qty} → {site}")
                                    replenishment_text = " | ".join(site_summaries)
                                
                                period_data.append({
                                    'date': shipping_date,
                                    'original_shipping_qty': original_shipping_qty,
                                    'stock_used': max(0, original_shipping_qty - total_replenishment),
                                    'replenishments': replenishment_list,
                                    'total_replenishment': total_replenishment,
                                    'replenishment_summary': replenishment_text,
                                    'balance': running_balance
                                })
                        
                        # ADD INVENTORY DATA TO RESULTS
                        if period_data or opening_stock['onhand'] or opening_stock['intransit'] or opening_stock['wip']:
                            inventory_data.append({
                                'product': prod,
                                'location': loc,
                                'opening_onhand': opening_stock['onhand'] or 0,
                                'opening_intransit': opening_stock['intransit'] or 0,
                                'opening_wip': opening_stock['wip'] or 0,
                                'periods': period_data,
                                'incoterm': None,
                                'freight_info': loc_info['freight_info']
                            })
                            print(f"DEBUG: Added inventory data for {prod} at {loc}: {len(period_data)} periods")
            
        except Exception as main_error:
            print(f"DEBUG ERROR: Inventory processing failed: {main_error}")
            import traceback
            print(f"DEBUG ERROR: Full traceback: {traceback.format_exc()}")
    
    # BUILD PRODUCTION DATA - ALWAYS SHOW FOR THE PRODUCT (not filtered by location)
    if product:
        assigned_site = _get_assigned_site_for_product(product)
        
        if assigned_site:
            print(f"DEBUG: Building production data for {product} at {assigned_site}")
            
            try:
                # Get cast-to-despatch days for this site
                cast_to_despatch_days = 0
                try:
                    cast_to_despatch = MasterDataCastToDespatchModel.objects.filter(
                        version=scenario_version,
                        Foundry__SiteName=assigned_site
                    ).first()
                    
                    if cast_to_despatch:
                        cast_to_despatch_days = cast_to_despatch.CastToDespatchDays or 0
                        print(f"DEBUG: Cast to despatch days for {assigned_site}: {cast_to_despatch_days}")
                    else:
                        print(f"DEBUG: No cast to despatch data found for {assigned_site}, using 0 days")
                except Exception as cast_error:
                    print(f"DEBUG ERROR: Failed to get cast to despatch days: {cast_error}")
                
                # Get opening stock for this product at this site
                opening_stock = MasterDataInventory.objects.filter(
                    version=scenario_version,
                    product=product,
                    site_id=assigned_site
                ).aggregate(
                    onhand=Sum('onhandstock_qty'),
                    intransit=Sum('intransitstock_qty'),
                    wip=Sum('wip_stock_qty')
                )
                
                opening_onhand = opening_stock['onhand'] or 0
                opening_intransit = opening_stock['intransit'] or 0
                opening_wip = opening_stock['wip'] or 0
                
                print(f"DEBUG: Opening stock - OnHand: {opening_onhand}, InTransit: {opening_intransit}, WIP: {opening_wip}")
                
                # GET ALL REPLENISHMENT DATA FOR THIS PRODUCT (FROM ALL LOCATIONS)
                all_replenishments = CalcualtedReplenishmentModel.objects.filter(
                    version=scenario_version,
                    Product__Product=product,
                    Site__SiteName=assigned_site  # Only replenishments going to this production site
                ).values('Location', 'ShippingDate', 'ReplenishmentQty').order_by('ShippingDate')
                
                print(f"DEBUG: Found {len(all_replenishments)} replenishment records for {product} → {assigned_site}")
                
                # Convert replenishments to pouring dates and consolidate by date
                # Convert replenishments to pouring dates and consolidate by date AND source
                replenishment_by_pouring_date = {}
                for repl in all_replenishments:
                    shipping_date = repl['ShippingDate']
                    location = repl['Location']
                    qty = repl['ReplenishmentQty']
                    
                    # Convert shipping date to pouring date
                    pouring_date = shipping_date - timedelta(days=cast_to_despatch_days)
                    
                    if pouring_date not in replenishment_by_pouring_date:
                        replenishment_by_pouring_date[pouring_date] = {
                            'total_qty': 0,
                            'sources': {}  # CHANGED: Use dict to consolidate by source
                        }
                    
                    # Consolidate quantities by source location
                    if location not in replenishment_by_pouring_date[pouring_date]['sources']:
                        replenishment_by_pouring_date[pouring_date]['sources'][location] = 0
                    
                    replenishment_by_pouring_date[pouring_date]['sources'][location] += qty
                    replenishment_by_pouring_date[pouring_date]['total_qty'] += qty
                    
                    print(f"DEBUG: Replenishment {shipping_date} → {pouring_date}: {qty} from {location}")
                
                # GET ACTUAL PRODUCTION DATA FROM CalculatedProductionModel
                production_query = CalculatedProductionModel.objects.filter(
                    version=scenario_version,
                    product_id=product,
                    site_id=assigned_site
                ).order_by('pouring_date')
                
                print(f"DEBUG: Found {production_query.count()} production records")
                
                # Build production periods
                production_periods = []
                running_balance = opening_onhand + opening_intransit + opening_wip
                print(f"DEBUG: Starting balance: {opening_onhand} + {opening_intransit} + {opening_wip} = {running_balance}")
                
                # Get all dates (both replenishment and production)
                all_pouring_dates = set(replenishment_by_pouring_date.keys())
                
                # Add production dates
                for prod_record in production_query:
                    if prod_record.pouring_date:
                        all_pouring_dates.add(prod_record.pouring_date)
                
                # Process each date
                for pouring_date in sorted(all_pouring_dates):
                    # Get replenishment for this date
                    replenishment_info = replenishment_by_pouring_date.get(pouring_date, {'total_qty': 0, 'sources': {}})
                    replenishment_qty = replenishment_info['total_qty']
                    sources_dict = replenishment_info['sources']
                    
                    # Get production for this date
                    production_qty = 0
                    prod_record = production_query.filter(pouring_date=pouring_date).first()
                    if prod_record:
                        production_qty = prod_record.production_quantity or 0
                    
                    # Calculate balance (same logic as populate_calculated_production)
                    running_balance = running_balance + production_qty - replenishment_qty
                    print(f"DEBUG: Balance calculation: {running_balance - production_qty + replenishment_qty} + {production_qty} - {replenishment_qty} = {running_balance}")
                    
                    # Create CONSOLIDATED source location description
                    if sources_dict:
                        # Sort sources by quantity (descending) for consistent display
                        sorted_sources = sorted(sources_dict.items(), key=lambda x: x[1], reverse=True)
                        source_summaries = []
                        for location, total_qty in sorted_sources:
                            source_summaries.append(f"{total_qty} from {location}")
                        source_location_text = " | ".join(source_summaries)
                    else:
                        source_location_text = "Production Only" if production_qty > 0 else "No Activity"
                    
                    # Add period if there's any activity
                    if replenishment_qty > 0 or production_qty > 0:
                        production_periods.append({
                            'date': pouring_date,
                            'requested_replenishment': replenishment_qty,
                            'production_reqmt': production_qty,
                            'balance': running_balance,
                            'source_location': source_location_text,
                            'period': pouring_date.strftime('%b %Y') if pouring_date else 'Unknown'
                        })
                        
                        print(f"DEBUG: Added period - Date: {pouring_date}, Repl: {replenishment_qty}, Prod: {production_qty}, Balance: {running_balance}")
                        print(f"DEBUG: Consolidated sources: {source_location_text}")
                # Sort periods by date
                production_periods.sort(key=lambda x: x['date'] if x['date'] else date.min)
                
                # Create production data entry
                production_data.append({
                    'product': product,
                    'site': assigned_site,
                    'opening_onhand': opening_onhand,
                    'opening_intransit': opening_intransit,
                    'opening_wip': opening_wip,
                    'cast_to_despatch_days': cast_to_despatch_days,
                    'periods': production_periods,
                    'period_count': len(production_periods)
                })
                
                print(f"DEBUG: Added production site data for {product} at {assigned_site}: {len(production_periods)} periods")
                    
            except Exception as prod_error:
                print(f"DEBUG ERROR: Production site data query failed: {prod_error}")
                import traceback
                print(f"DEBUG ERROR: Traceback: {traceback.format_exc()}")
    
    print(f"DEBUG: Search completed - found {len(inventory_data)} inventory records and {len(production_data)} production records")
    
    return {
        'inventory_data': inventory_data,
        'production_data': production_data,
    }


def detailed_view_scenario_inventory(scenario):
    """
    Get detailed inventory view data combining multiple models.
    Returns empty data - use search_detailed_view_data for actual searches.
    """
    # Return empty data structure - don't load anything by default
    return {
        'inventory_data': [],
        'production_data': [],
    }

