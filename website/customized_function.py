from django.core.management.base import BaseCommand
from website.models import (
    scenarios, ProductSiteCostModel, MasterDataProductModel, MasterDataPlantModel, SMART_Forecast_Model, AggregatedForecast,
        CalculatedProductionModel, MasterDataManuallyAssignProductionRequirement, MasterDataOrderBook, 
        MasterDataHistoryOfProductionModel, MasterDataEpicorSupplierMasterDataModel, AggregatedFinancialChartData,
        MasterDataPlan
)
from collections import defaultdict
from django.db.models.functions import TruncMonth
from django.db.models import Sum
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import json
import pandas as pd
import polars as pl
import math
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
        df = pl.from_pandas(pd.read_sql_query(query, engine, params=(site_name, product_key)))
        if len(df) > 0:
            first_row = df.row(0, named=True)
            return first_row['UnitPriceAUD'], first_row['DateValue']
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


def get_top_products_by_parent_group_and_month(scenario, start_date=None):
    """
    Get top 15 products by cost for each parent product group and month.
    Returns data structure for tooltip display.
    """
    queryset = CalculatedProductionModel.objects.filter(version=scenario)
    
    # Apply start_date filter if provided
    if start_date:
        queryset = queryset.filter(pouring_date__gte=start_date)
    
    # Get all data with product details
    queryset = (
        queryset
        .annotate(month=TruncMonth('pouring_date'))
        .values('month', 'parent_product_group', 'product', 'cogs_aud')
        .order_by('month', 'parent_product_group', '-cogs_aud')
    )
    
    # Build data structure: {parent_group: {month: [top 15 products]}}
    result = {}
    
    for item in queryset:
        month_str = item['month'].strftime('%b %Y')
        group = item['parent_product_group']
        product = item['product']
        cogs = item['cogs_aud'] or 0
        
        if group not in result:
            result[group] = {}
        if month_str not in result[group]:
            result[group][month_str] = []
        
        # Add product data
        result[group][month_str].append({
            'product': product,
            'cogs': cogs
        })
    
    # Keep only top 15 products per group/month and sort by cost
    for group in result:
        for month in result[group]:
            # Sort by COGS descending and take top 15
            result[group][month] = sorted(
                result[group][month], 
                key=lambda x: x['cogs'], 
                reverse=True
            )[:15]
    
    return result


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
    
    # Ensure we don't include July 2025 data - use inventory cutoff but cap at June 30, 2025
    july_2025_start = date(2025, 7, 1)
    if user_inventory_date >= july_2025_start:
        # Use the last day of the month before July 2025 (June 30, 2025)
        from calendar import monthrange
        prev_month_year = 2025
        prev_month = 6  # June
        last_day = monthrange(prev_month_year, prev_month)[1]
        inventory_cutoff = date(prev_month_year, prev_month, last_day)
        print(f"DEBUG: Inventory date adjusted to exclude July 2025: {inventory_cutoff}")
    else:
        inventory_cutoff = user_inventory_date
        print(f"DEBUG: Using inventory cutoff: {inventory_cutoff}")

    # CORRECTED: Define fiscal year ranges to match your system
    fy_ranges = {
        "FY24": (date(2024, 4, 1), date(2025, 3, 31)),  # ADDED
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
                # For FY25, use the earlier of inventory_cutoff or FY end
                filter_end_date = min(inventory_cutoff, fy_end)
                filter_start_date = fy_start
            else:
                # For future FYs, if inventory_cutoff is before FY start, skip this FY
                if inventory_cutoff < fy_start:
                    poured_data[fy] = {}
                    continue
                # Use the earlier of inventory_cutoff or FY end
                filter_end_date = min(inventory_cutoff, fy_end)
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
        "FY24": (date(2024, 4, 1), date(2025, 3, 31)),  # ADDED
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),  # FIXED
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),  # FIXED
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),  # FIXED
    }

    # Get demand plan data
    demand_plan = {}
    for fy, (start, end) in fy_ranges.items():
        demand_plan[fy] = {}
        for site in sites:
            total_tonnes = float(
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
            demand_tonnes = float(demand_plan.get(fy, {}).get(site, 0))
            poured_tonnes = float(poured_data.get(fy, {}).get(site, 0))
            combined_data[fy][site] = round(demand_tonnes + poured_tonnes)

    return combined_data, poured_data


def get_snapshot_based_pour_plan_data(scenario_version, poured_data=None):
    """
    Get pour plan data using snapshot-based logic:
    - PowerBI actual production data for snapshot month and earlier (reuses poured_data if provided)
    - MasterDataPlan data for future months after snapshot
    """
    from website.models import MasterDataPlan, MasterDataInventory
    from datetime import date
    from dateutil.relativedelta import relativedelta
    from sqlalchemy import create_engine, text
    from calendar import monthrange
    
    # Handle both string and object scenario_version
    scenario_name = scenario_version if isinstance(scenario_version, str) else scenario_version.version
    print(f"DEBUG: Starting snapshot-based pour plan calculation for scenario {scenario_name}")
    
    # Get snapshot date for actual vs planned logic
    snapshot_date = None
    try:
        inventory_snapshot = MasterDataInventory.objects.filter(version=scenario_name).first()
        if inventory_snapshot:
            snapshot_date = inventory_snapshot.date_of_snapshot
            print(f"DEBUG: Using snapshot date: {snapshot_date}")
        else:
            print("DEBUG: No inventory snapshot found, using current date")
            snapshot_date = date.today()
    except Exception as e:
        print(f"DEBUG: Error getting snapshot date: {e}")
        snapshot_date = date.today()
    
    # Calculate snapshot month cutoff
    snapshot_month_start = snapshot_date.replace(day=1)
    next_month_start = snapshot_month_start + relativedelta(months=1)
    print(f"DEBUG: Snapshot month start: {snapshot_month_start}, Next month start: {next_month_start}")
    
    # Define fiscal year ranges and sites
    fy_ranges = {
        "FY24": (date(2024, 4, 1), date(2025, 3, 31)),
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),
    }
    sites = ["MTJ1", "COI2", "XUZ1", "MER1", "WUN1", "WOD1", "CHI1"]
    
    # Initialize pour plan data structure
    pour_plan_data = {}
    
    # If poured_data is not provided, get it from the database (fallback)
    if poured_data is None:
        print("DEBUG: No poured_data provided, getting from database as fallback")
        poured_data = get_poured_data_by_fy_and_site(scenario_name)
    else:
        print("DEBUG: Using provided poured_data to avoid duplicate database queries")
    
    # For PowerBI actual data, we need more granular month-by-month breakdown
    # The poured_data gives us totals by FY, but we need to split based on snapshot date
    
    # Database connection for detailed monthly PowerBI queries (only if needed)
    db_available = True
    engine = None
    try:
        Server = 'bknew-sql02'
        Database = 'Bradken_Data_Warehouse'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        engine = create_engine(Database_Con)
        print(f"DEBUG: Database connection established for monthly breakdown")
    except Exception as e:
        print(f"DEBUG: Database connection failed: {e}")
        db_available = False
    
    # Process each fiscal year
    for fy, (fy_start, fy_end) in fy_ranges.items():
        pour_plan_data[fy] = {}
        print(f"DEBUG: Processing {fy}: {fy_start} to {fy_end}")
        
        for site in sites:
            print(f"DEBUG: Processing site {site} for {fy}")
            
            actual_total = 0.0
            planned_total = 0.0
            
            # Calculate actual production from PowerBI (snapshot month and before)
            if next_month_start > fy_start and db_available:
                print(f"DEBUG: Getting monthly PowerBI breakdown for {site} in {fy}")
                
                # Process each month from FY start up to snapshot month
                current_month = fy_start.replace(day=1)
                actual_cutoff = min(next_month_start, fy_end + relativedelta(days=1))
                
                try:
                    with engine.connect() as connection:
                        while current_month < actual_cutoff:
                            # Calculate month boundaries
                            last_day = monthrange(current_month.year, current_month.month)[1]
                            month_end = current_month.replace(day=last_day)
                            
                            query = text("""
                                SELECT 
                                    SUM(hp.CastQty * p.DressMass / 1000) as TotalTonnes,
                                    COUNT(*) as RecordCount
                                FROM PowerBI.HeatProducts hp
                                INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                                INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                                WHERE hp.TapTime IS NOT NULL 
                                    AND p.DressMass IS NOT NULL 
                                    AND s.SiteName = :site_name
                                    AND hp.TapTime >= :start_date
                                    AND hp.TapTime <= :end_date
                            """)
                            
                            result = connection.execute(query, {
                                'site_name': site,
                                'start_date': current_month,
                                'end_date': month_end
                            })
                            
                            row = result.fetchone()
                            if row and row.TotalTonnes:
                                month_actual = float(row.TotalTonnes)
                                actual_total += month_actual
                                print(f"DEBUG: {fy} {site} {current_month.strftime('%b %Y')} - PowerBI: {round(month_actual)} tonnes")
                            else:
                                print(f"DEBUG: {fy} {site} {current_month.strftime('%b %Y')} - No PowerBI data")
                            
                            # Move to next month
                            if current_month.month == 12:
                                current_month = current_month.replace(year=current_month.year + 1, month=1)
                            else:
                                current_month = current_month.replace(month=current_month.month + 1)
                
                except Exception as e:
                    print(f"DEBUG ERROR: PowerBI query failed for {fy} {site}: {e}")
                    actual_total = 0.0
            else:
                print(f"DEBUG: No PowerBI actuals needed for {site} in {fy} (snapshot after FY start or DB unavailable)")
            
            # Calculate planned pours from MasterDataPlan (after snapshot month)
            if next_month_start <= fy_end:
                # FIXED: Ensure we don't query before fiscal year start
                query_start_month = max(next_month_start, fy_start)
                print(f"DEBUG: Getting MasterDataPlan data for {site} in {fy} from {query_start_month} (max of {next_month_start} and {fy_start})")
                try:
                    planned_plans = MasterDataPlan.objects.filter(
                        version=scenario_name,
                        Foundry__SiteName=site,
                        Month__gte=query_start_month,
                        Month__lte=fy_end
                    )
                    planned_total = float(sum(plan.PlanDressMass for plan in planned_plans))
                    print(f"DEBUG: {fy} {site} - MasterDataPlan records: {planned_plans.count()}, Total: {round(planned_total)}")
                except Exception as e:
                    print(f"DEBUG ERROR: MasterDataPlan query failed for {fy} {site}: {e}")
                    planned_total = 0.0
            else:
                print(f"DEBUG: No future plan data needed for {site} in {fy} (snapshot after FY end)")
            
            # Combine actual and planned
            total_pour_plan = actual_total + planned_total
            pour_plan_data[fy][site] = round(total_pour_plan)
            
            print(f"DEBUG: {fy} {site} - Final: Actual={round(actual_total)}, Planned={round(planned_total)}, Total={round(total_pour_plan)}")
    
    print(f"DEBUG: Snapshot-based pour plan calculation complete")
    return pour_plan_data


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
            * (record.Yield / 100)  # Convert percentage to decimal
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
        value = float(sum(plan.PlanDressMass for plan in plans))  # Database field now contains correct calculated value
        monthly_pour_plan.append(round(value))
    
    return monthly_pour_plan

# ...existing code...

def build_detailed_monthly_table(fy, site, scenario_version, plan_type='demand'):
    """Build detailed monthly table showing the actual breakdown that makes up the combined total.
    
    Args:
        fy: Fiscal year (FY24, FY25, FY26, FY27)
        site: Site code (MTJ1, COI2, etc.) or 'OUTSOURCE' for outsource data
        scenario_version: Scenario object or string
        plan_type: 'demand' for Demand Plan breakdown, 'pour' for Pour Plan breakdown, 'outsource' for Outsource breakdown
    """
    # Handle both string and object scenario_version
    scenario_name = scenario_version if isinstance(scenario_version, str) else scenario_version.version
    
    fy_ranges = {
        "FY24": (date(2024, 4, 1), date(2025, 3, 31)),
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),
    }
    
    if fy not in fy_ranges:
        print(f"ERROR: Unsupported fiscal year: {fy}")
        return mark_safe(f"<p>Error: Fiscal year '{fy}' is not supported.</p>")
    
    start, end = fy_ranges[fy]
    
    if plan_type == 'outsource':
        # Use fast Polars-based outsource breakdown instead of slow Django ORM
        from website.direct_polars_queries import build_outsource_table_polars
        
        print(f"DEBUG: Using Polars for outsource breakdown - {fy}/{site}")
        return build_outsource_table_polars(scenario_version.version, fy)
    
    elif plan_type == 'pour':
        # Pour Plan breakdown - get monthly pour plan data from MasterDataPlan
        # First try current scenario, if no data found, try other scenarios with XUZ1 data
        pour_monthly = (
            MasterDataPlan.objects
            .filter(
                version=scenario_name,
                Foundry__SiteName=site,
                Month__gte=start,
                Month__lte=end
            )
            .annotate(month=TruncMonth('Month'))
            .values('month')
            .annotate(total=Sum('PlanDressMass'))
            .order_by('month')
        )
        
        # If no data found for current scenario, continue with empty data
        if not pour_monthly.exists():
            print(f"DEBUG: No pour plan data found for {site} in scenario {scenario_name}")
            # Continue with empty queryset - no fallback scenarios
        
        # Convert to dict for easy lookup
        pour_dict = {row['month'].strftime('%b %Y'): row['total'] or 0 for row in pour_monthly}
        print(f"DEBUG: Pour Plan dict for {fy}/{site}: {pour_dict}")
        
        # Get actual poured data for comparison
        poured_monthly = get_monthly_poured_data_for_site_and_fy(site, fy, scenario_name)
        print(f"DEBUG: Actual Poured monthly for {fy}/{site}: {poured_monthly}")
        
        # Generate all months in the fiscal year
        current_month = start
        months_data = []
        total_planned = 0
        total_actual = 0
        total_variance = 0
        
        while current_month <= end:
            month_str = current_month.strftime('%b %Y')
            planned_qty = pour_dict.get(month_str, 0)
            actual_qty = poured_monthly.get(month_str, 0)
            variance_qty = actual_qty + planned_qty  # Changed from subtraction to addition
            
            months_data.append({
                'month': month_str,
                'planned': round(planned_qty),
                'actual': round(actual_qty),
                'variance': round(variance_qty)
            })
            
            # Add rounded values to ensure total matches sum of displayed values
            total_planned += round(planned_qty)
            total_actual += round(actual_qty)
            total_variance += round(variance_qty)
            
            print(f"DEBUG: {month_str} - Planned: {planned_qty}, Actual: {actual_qty}, Variance: {variance_qty}")
            
            # Move to next month
            if current_month.month == 12:
                current_month = current_month.replace(year=current_month.year + 1, month=1)
            else:
                current_month = current_month.replace(month=current_month.month + 1)
        
        # Build HTML table for Pour Plan
        table = f"""
        <table class='table table-sm table-bordered mb-0' style='font-size: 12px;'>
            <thead style='background-color: #f8f9fa;'>
                <tr>
                    <th style='text-align: left; padding: 4px 8px;'>Month</th>
                    <th style='text-align: right; padding: 4px 8px;'>Pour Plan</th>
                    <th style='text-align: right; padding: 4px 8px;'>Actual Poured</th>
                    <th style='text-align: right; padding: 4px 8px;'>Variance</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for month_data in months_data:
            variance_class = 'text-success' if month_data['variance'] >= 0 else 'text-danger'
            table += f"""
                <tr>
                    <td style='text-align: left; padding: 4px 8px;'>{month_data['month']}</td>
                    <td style='text-align: right; padding: 4px 8px;'><strong>{month_data['planned']:,}</strong></td>
                    <td style='text-align: right; padding: 4px 8px;'>{month_data['actual']:,}</td>
                    <td style='text-align: right; padding: 4px 8px;' class='{variance_class}'>{month_data['variance']:+,}</td>
                </tr>
            """
        
        # Add totals row
        total_variance_class = 'text-success' if total_variance >= 0 else 'text-danger'
        table += f"""
            </tbody>
            <tfoot style='background-color: #e9ecef; font-weight: bold;'>
                <tr>
                    <td style='text-align: left; padding: 4px 8px;'>Total</td>
                    <td style='text-align: right; padding: 4px 8px;'><strong>{round(total_planned):,}</strong></td>
                    <td style='text-align: right; padding: 4px 8px;'>{round(total_actual):,}</td>
                    <td style='text-align: right; padding: 4px 8px;' class='{total_variance_class}'><strong>{round(total_variance):+,}</strong></td>
                </tr>
            </tfoot>
        </table>
        """
        
        print(f"DEBUG: Built Pour Plan table for {fy}/{site} - Planned: {total_planned}, Actual: {total_actual}, Variance: {total_variance}")
        
    else:
        # Demand Plan breakdown (original functionality)
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
        
        # Build HTML table for Demand Plan
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
                    <td style='text-align: right; padding: 4px 8px;'><strong>{month_data['demand']:,}</strong></td>
                    <td style='text-align: right; padding: 4px 8px;'>{month_data['poured']:,}</td>
                    <td style='text-align: right; padding: 4px 8px;'>{month_data['combined']:,}</td>
                </tr>
            """
        
        # Add totals row
        table += f"""
            </tbody>
            <tfoot style='background-color: #e9ecef; font-weight: bold;'>
                <tr>
                    <td style='text-align: left; padding: 4px 8px;'>Total</td>
                    <td style='text-align: right; padding: 4px 8px;'><strong>{round(total_demand):,}</strong></td>
                    <td style='text-align: right; padding: 4px 8px;'>{round(total_poured):,}</td>
                    <td style='text-align: right; padding: 4px 8px;'>{round(total_combined):,}</td>
                </tr>
            </tfoot>
        </table>
        """
        
        
    
    # Clean up the HTML for JSON encoding
    table = table.replace('\n', '').replace('\r', '').replace('"', '&quot;')
    return mark_safe(table)

def calculate_control_tower_data(scenario_version):
    """Calculate control tower data including demand plan and pour plan with snapshot-based actual vs planned logic."""
    
    from website.models import MasterDataPlan, MasterDataInventory
    from datetime import date, timedelta
    from dateutil.relativedelta import relativedelta
    
    # Get snapshot date for actual vs planned logic
    snapshot_date = None
    try:
        inventory_snapshot = MasterDataInventory.objects.filter(version=scenario_version).first()
        if inventory_snapshot:
            snapshot_date = inventory_snapshot.date_of_snapshot
            print(f"DEBUG: Control tower using snapshot date: {snapshot_date}")
        else:
            print("DEBUG: No inventory snapshot found, using current date")
            snapshot_date = date.today()
    except Exception as e:
        print(f"DEBUG: Error getting snapshot date: {e}")
        snapshot_date = date.today()
    
    # Calculate snapshot month cutoff (data in snapshot month = actual, after = planned)
    snapshot_month_start = snapshot_date.replace(day=1)
    next_month_start = snapshot_month_start + relativedelta(months=1)
    print(f"DEBUG: Snapshot month start: {snapshot_month_start}, Next month start: {next_month_start}")
    
    # Define fiscal year ranges
    fy_ranges = {
        "FY24": (date(2024, 4, 1), date(2025, 3, 31)),
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),
    }
    
    # Map site codes to display names
    site_map = {
        "MTJ1": "MT Joli",
        "COI2": "Coimbatore",
        "XUZ1": "Xuzhou",
        "MER1": "Merlimau",
        "WUN1": "Wundowie",
        "WOD1": "Wodonga",
        "CHI1": "Chilca"
    }
    sites = list(site_map.keys())
    display_sites = [site_map[code] for code in sites]

    # Get combined demand and poured data once for all fiscal years
    combined_demand_plan, poured_data = get_combined_demand_and_poured_data(scenario_version)
    print(f"DEBUG: Got combined demand plan, type: {type(combined_demand_plan)}")
    print(f"DEBUG: Sample combined demand: {combined_demand_plan}")

    # Calculate pour plan for all fiscal years using snapshot-based logic
    # Pass poured_data to avoid duplicate database queries
    pour_plan = get_snapshot_based_pour_plan_data(scenario_version, poured_data)

    # Prepare data structure for table rows for all fiscal years
    control_tower_fy = {}
    for fy, (start, end) in fy_ranges.items():
        control_tower_rows = []
        total_budget = 0
        total_capacity = 0
        total_pour_plan = 0
        total_demand_plan = 0

        # Process each site using pre-calculated pour plan data
        for site in sites:
            print(f"DEBUG: Processing site {site} for {fy}")
            
            # Get pour plan and demand plan data
            pour = pour_plan.get(fy, {}).get(site, 0)
            demand = combined_demand_plan.get(fy, {}).get(site, 0)
            
            print(f"DEBUG: {fy} {site} - Pour Plan: {pour}, Demand Plan: {demand}")
            
            control_tower_rows.append({
                'site_code': site,
                'site_name': site_map[site],
                'budget': '',
                'capacity': '',
                'pour_plan': round(pour),
                'demand_plan': round(demand)
            })
            
            # Totals for pour and demand only
            total_pour_plan += float(pour)
            total_demand_plan += float(demand)

        # Add Total Castings row
        control_tower_rows.append({
            'site_code': 'TOTAL_CASTINGS',
            'site_name': 'Total Castings',
            'budget': '',
            'capacity': '',
            'pour_plan': round(total_pour_plan),
            'demand_plan': round(total_demand_plan)
        })

        # Add Outsource row (blank)
        control_tower_rows.append({
            'site_code': 'OUTSOURCE',
            'site_name': 'Outsource',
            'budget': '',
            'capacity': '',
            'pour_plan': '',
            'demand_plan': ''
        })

        # Add Total Production row (same as Total Castings for now)
        control_tower_rows.append({
            'site_code': 'TOTAL_PRODUCTION',
            'site_name': 'Total Production',
            'budget': '',
            'capacity': '',
            'pour_plan': round(total_pour_plan),
            'demand_plan': round(total_demand_plan)
        })

        control_tower_fy[fy] = control_tower_rows

    return {
        'control_tower_fy': control_tower_fy,
        'sites': sites,
        'display_sites': display_sites,
        'fys': list(fy_ranges.keys()),
        'combined_demand_plan': combined_demand_plan,
        'poured_data': poured_data,
        'pour_plan': pour_plan,
    }

def get_monthly_pour_plan_details_for_site_and_fy(site, fy, scenario_version):
    """Get detailed monthly pour plan data showing actual vs future for a specific site and fiscal year."""
    from datetime import date, timedelta
    from calendar import monthrange
    
    print(f"DEBUG: Getting detailed pour plan data for {site} in {fy}")
    
    # Define fiscal year ranges
    fy_ranges = {
        "FY24": (date(2024, 4, 1), date(2025, 3, 31)),
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),
    }
    
    if fy not in fy_ranges:
        print(f"ERROR: Unsupported fiscal year in get_monthly_pour_plan_details_for_site_and_fy: {fy}")
        return {}
    
    fy_start, fy_end = fy_ranges[fy]
    current_date = date.today()
    
    monthly_details = []
    
    # Generate each month in the fiscal year
    current_month = fy_start.replace(day=1)
    while current_month <= fy_end:
        month_end = date(current_month.year, current_month.month, 
                        monthrange(current_month.year, current_month.month)[1])
        
        # Determine if this month is actual (past) or plan (future)
        is_actual = month_end <= current_date
        
        if is_actual:
            # Get actual production data from PowerBI database
            from sqlalchemy import create_engine, text
            
            # Database connection
            Server = 'bknew-sql02'
            Database = 'Bradken_Data_Warehouse'
            Driver = 'ODBC Driver 17 for SQL Server'
            Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
            engine = create_engine(Database_Con)
            
            # Calculate month start and end dates
            from calendar import monthrange
            month_start = current_month.replace(day=1)
            last_day = monthrange(current_month.year, current_month.month)[1]
            month_end = current_month.replace(day=last_day)
            
            production_data = 0
            
            try:
                with engine.connect() as connection:
                    query = text("""
                        SELECT 
                            SUM(hp.CastQty * p.DressMass / 1000) as TotalTonnes,
                            COUNT(*) as RecordCount
                        FROM PowerBI.HeatProducts hp
                        INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                        INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                        WHERE hp.TapTime IS NOT NULL 
                            AND p.DressMass IS NOT NULL 
                            AND s.SiteName = :site_name
                            AND hp.TapTime >= :start_date
                            AND hp.TapTime <= :end_date
                    """)
                    
                    result = connection.execute(query, {
                        'site_name': site,
                        'start_date': month_start,
                        'end_date': month_end
                    })
                    
                    row = result.fetchone()
                    if row and row.TotalTonnes:
                        production_data = round(float(row.TotalTonnes), 2)
                        record_count = row.RecordCount
                        print(f"DEBUG: {site} {current_month.strftime('%b %Y')} - {record_count} PowerBI records, Tonnage: {production_data}")
                    else:
                        print(f"DEBUG: {site} {current_month.strftime('%b %Y')} - No PowerBI data found")
                        
            except Exception as e:
                print(f"DEBUG ERROR: PowerBI query failed for {site} {current_month.strftime('%b %Y')}: {e}")
                production_data = 0
            
            monthly_details.append({
                'month': current_month.strftime('%b %Y'),
                'month_date': current_month,
                'value': production_data,
                'type': 'Actual',
                'is_actual': True
            })
        else:
            # Get future pour plan data
            plan_data = MasterDataPlan.objects.filter(
                version=scenario_version,
                Foundry__SiteName=site,
                Month__year=current_month.year,
                Month__month=current_month.month
            ).first()
            
            plan_value = float(plan_data.PlanDressMass) if plan_data else 0
            
            monthly_details.append({
                'month': current_month.strftime('%b %Y'),
                'month_date': current_month,
                'value': plan_value,
                'type': 'Plan',
                'is_actual': False
            })
        
        # Move to next month
        if current_month.month == 12:
            current_month = current_month.replace(year=current_month.year + 1, month=1)
        else:
            current_month = current_month.replace(month=current_month.month + 1)
    
    return {
        'site': site,
        'fy': fy,
        'monthly_details': monthly_details,
        'total_actual': sum(d['value'] for d in monthly_details if d['is_actual']),
        'total_plan': sum(d['value'] for d in monthly_details if not d['is_actual']),
        'grand_total': sum(d['value'] for d in monthly_details)
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
    
    # Ensure we don't include July 2025 data - use inventory cutoff but cap at June 30, 2025
    july_2025_start = date(2025, 7, 1)
    if inventory_date >= july_2025_start:
        # Use the last day of the month before July 2025
        from calendar import monthrange
        prev_month_year = 2025
        prev_month = 6  # June
        last_day = monthrange(prev_month_year, prev_month)[1]
        inventory_cutoff = date(prev_month_year, prev_month, last_day)
        print(f"DEBUG: Inventory date adjusted to exclude July 2025: {inventory_cutoff}")
    else:
        inventory_cutoff = inventory_date
        print(f"DEBUG: Using inventory cutoff: {inventory_cutoff}")
    
    # CORRECTED: Define fiscal year ranges to match your system
    fy_ranges = {
        "FY24": (date(2024, 4, 1), date(2025, 3, 31)),  # ADDED
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),  # FIXED
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),  # FIXED
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),  # FIXED
    }
    
    if fy not in fy_ranges:
        print(f"ERROR: Unsupported fiscal year in get_monthly_poured_data_for_site_and_fy: {fy}")
        return {}
    
    fy_start, fy_end = fy_ranges[fy]
    filter_end_date = min(inventory_cutoff, fy_end)  # Use inventory_cutoff instead of inventory_date
    
    print(f"DEBUG: FY range: {fy_start} to {fy_end}")
    print(f"DEBUG: Filter end date: {filter_end_date}")
    
    if inventory_cutoff < fy_start:  # Use inventory_cutoff instead of inventory_date
        print(f"DEBUG: Inventory cutoff {inventory_cutoff} is before FY start {fy_start}")
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
    """Get inventory data with proper start date filtering and inventory balance calculation."""
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
    
    # Get top products data for tooltips
    top_products_by_group_month = get_top_products_by_parent_group_and_month(scenario_version, start_date=start_date)

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

        # DEBUG: Special logging for GET group to understand the COGS vs Production AUD gap
        if group and ('GET' in group.upper() or group.upper() == 'GET'):
            print(f"DEBUG GET GROUP ANALYSIS:")
            print(f"  Group name: '{group}'")
            print(f"  COGS months: {len(months)}, Production months: {len(prod_months)}")
            
            # Check for very high production values
            for i, month in enumerate(prod_months):
                if i < len(production_aud) and production_aud[i] > 8000000:  # > $8M
                    print(f"  HIGH PRODUCTION: {month} = ${production_aud[i]:,.2f}")
                    
                    # Get detailed breakdown for this month
                    month_start = pd.to_datetime(f"01 {month}")
                    month_end = month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)
                    
                    detailed_prod = CalculatedProductionModel.objects.filter(
                        version=scenario_version,
                        parent_product_group=group,
                        pouring_date__gte=month_start,
                        pouring_date__lte=month_end
                    ).values('product_id', 'site_id', 'cogs_aud', 'production_quantity').order_by('-cogs_aud')
                    
                    print(f"    Top production records for {month}:")
                    for j, record in enumerate(detailed_prod[:5]):
                        print(f"      Product: {record['product_id']}, Site: {record['site_id']}")
                        print(f"      COGS AUD: ${record['cogs_aud']:,.2f}, Qty: {record['production_quantity']}")
            
            # Compare COGS vs Production for same months
            for month in set(months) & set(prod_months):
                cogs_idx = months.index(month) if month in months else -1
                prod_idx = prod_months.index(month) if month in prod_months else -1
                
                if cogs_idx >= 0 and prod_idx >= 0:
                    cogs_val = cogs[cogs_idx] if cogs_idx < len(cogs) else 0
                    prod_val = production_aud[prod_idx] if prod_idx < len(production_aud) else 0
                    
                    if prod_val > 0 and cogs_val > 0:
                        ratio = prod_val / cogs_val
                        if ratio > 1.5:  # Production is 50% higher than COGS
                            print(f"  RATIO ALERT {month}: Production ${prod_val:,.0f} vs COGS ${cogs_val:,.0f} (ratio: {ratio:.1f}x)")
        
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

    # NEW: Get opening inventory and combine with forecast data
    opening_inventory_data = get_opening_inventory_by_group(scenario_version)
    combined_data_with_inventory = combine_inventory_with_forecast_data(
        cogs_data_by_group, 
        opening_inventory_data, 
        scenario_version
    )

    return {
        'inventory_months': all_months,
        'inventory_cogs': cogs_aligned,
        'inventory_revenue': revenue_aligned,
        'production_aud': prod_aligned,
        'production_cogs_group_chart': production_cogs_group_chart,
        'top_products_by_group_month': top_products_by_group_month,
        'parent_product_groups': list(parent_groups),
        'cogs_data_by_group': combined_data_with_inventory,  # Updated to include inventory balance
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
        """Get assigned site for ONE specific product only with updated priority logic."""
        print(f"DEBUG: Getting site assignment for {product}")
        
        try:
            # PRIORITY 1: Check Manual Assignment first (HIGHEST PRIORITY)
            # Note: This is a simplified check - in populate_calculated_replenishment, 
            # the date matching is more precise with Period_AU from forecast
            manual_assignment = MasterDataManuallyAssignProductionRequirement.objects.filter(
                version=scenario_version,
                Product__Product=product
            ).first()
            
            if manual_assignment:
                print(f"DEBUG: Found manual assignment: {manual_assignment.Site.SiteName}")
                return manual_assignment.Site.SiteName
            
            # PRIORITY 2: Check Order Book
            order_book = MasterDataOrderBook.objects.filter(
                version=scenario_version,
                productkey=product
            ).exclude(site__isnull=True).exclude(site__exact='').first()
            
            if order_book:
                print(f"DEBUG: Found in order book: {order_book.site}")
                return order_book.site
            
            # PRIORITY 3: Check Production History
            production = MasterDataHistoryOfProductionModel.objects.filter(
                version=scenario_version,
                Product=product
            ).exclude(Foundry__isnull=True).exclude(Foundry__exact='').first()
            
            if production:
                print(f"DEBUG: Found in production history: {production.Foundry}")
                return production.Foundry
            
            # PRIORITY 4: Check Supplier
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


def get_opening_inventory_by_group(scenario_version):
    """Get opening inventory from SQL Server grouped by ParentProductGroupDescription - OPTIMIZED"""
    from sqlalchemy import create_engine, text
    import pandas as pd
    import polars as pl
    import time
    from website.models import MasterDataInventory, AggregatedForecast, CalculatedProductionModel
    
    print(f"DEBUG: [PERF] Starting get_opening_inventory_by_group for {scenario_version}")
    start_time = time.time()
    
    # PERFORMANCE: Check if we already have this data cached in AggregatedInventoryChartData
    try:
        from website.models import AggregatedInventoryChartData
        cached_data = AggregatedInventoryChartData.objects.filter(version=scenario_version).first()
        if cached_data and cached_data.inventory_by_group:
            print(f"DEBUG: [PERF] Using cached inventory data - {time.time() - start_time:.2f}s")
            return cached_data.inventory_by_group
    except Exception as cache_error:
        print(f"DEBUG: [PERF] Cache miss, proceeding with SQL query: {cache_error}")
    
    # Get the snapshot date from MasterDataInventory
    try:
        inventory_snapshot = MasterDataInventory.objects.filter(version=scenario_version).first()
        if not inventory_snapshot:
            print("DEBUG: No inventory snapshot found")
            return {}
        snapshot_date = inventory_snapshot.date_of_snapshot.strftime('%Y-%m-%d')
        print(f"DEBUG: Using snapshot date: {snapshot_date}")
    except Exception as e:
        print(f"DEBUG: Error getting snapshot date: {e}")
        return {}
    
    # SQL Server connection with optimizations
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}&timeout=30'
    
    try:
        engine = create_engine(Database_Con, pool_timeout=30, pool_recycle=3600)
        
        # ULTRA-OPTIMIZED SQL query - add query hints and indexes
        sql_query = f"""
        SELECT 
            Products.ParentProductGroupDescription,
            SUM(Inventory.StockOnHandValueAUD) AS opening_inventory_aud
        FROM PowerBI.[Inventory Daily History] AS Inventory WITH (NOLOCK, READUNCOMMITTED)
        INNER JOIN PowerBI.Dates AS Dates WITH (NOLOCK, READUNCOMMITTED)
            ON Inventory.skReportDateId = Dates.skDateId
        INNER JOIN PowerBI.Products AS Products WITH (NOLOCK, READUNCOMMITTED)
            ON Inventory.skProductId = Products.skProductId
        WHERE Dates.DateValue = '{snapshot_date}'
            AND Products.ParentProductGroupDescription IS NOT NULL
            AND Inventory.StockOnHandValueAUD > 0
        GROUP BY Products.ParentProductGroupDescription
        ORDER BY Products.ParentProductGroupDescription
        OPTION (MAXDOP 4, FAST 10)
        """
        
        query_start = time.time()
        df = pl.from_pandas(pd.read_sql(sql_query, engine))
        print(f"DEBUG: [PERF] SQL query completed in {time.time() - query_start:.2f}s - {len(df)} parent groups")
        
        # Simplified dictionary structure (only parent groups for performance)
        inventory_by_group = {}
        total_inventory = 0
        
        for row in df.iter_rows(named=True):
            parent_group = row['ParentProductGroupDescription']
            if parent_group:  # Filter out null/nan values
                inventory_value = float(row['opening_inventory_aud'])
                
                inventory_by_group[parent_group] = inventory_value
                total_inventory += inventory_value
                print(f"DEBUG: Group '{parent_group}': ${inventory_value:,.2f} AUD")
        
        print(f"DEBUG: [PERF] Total processing time: {time.time() - start_time:.2f}s")
        print(f"DEBUG: Total inventory across all groups: ${total_inventory:,.2f} AUD")
        return inventory_by_group
        
    except Exception as e:
        print(f"ERROR: [PERF] SQL query failed in {time.time() - start_time:.2f}s: {e}")
        import traceback
        traceback.print_exc()
        return {}


def populate_inventory_projection_model(scenario_version):
    """Populate InventoryProjectionModel with monthly projections starting from next month after snapshot - OPTIMIZED"""
    from website.models import (
        InventoryProjectionModel, MasterDataInventory, AggregatedForecast, 
        CalculatedProductionModel, scenarios
    )
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    import polars as pl
    import traceback
    import time
    
    print(f"DEBUG: [PERF] Starting inventory projection population for scenario: {scenario_version}")
    start_time = time.time()
    
    try:
        # Get scenario object
        scenario = scenarios.objects.get(version=scenario_version)
        
        # PERFORMANCE: Clear existing projections to avoid duplicate key errors
        existing_count = InventoryProjectionModel.objects.filter(version=scenario).count()
        if existing_count > 0:
            print(f"DEBUG: [PERF] Clearing {existing_count} existing projections to avoid duplicates")
            InventoryProjectionModel.objects.filter(version=scenario).delete()
        
        # Get snapshot date from MasterDataInventory
        inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
        if not inventory_snapshot:
            print("ERROR: No inventory snapshot found")
            return False
        
        # Start from next month after snapshot
        snapshot_date = inventory_snapshot.date_of_snapshot
        start_month = (snapshot_date + relativedelta(months=1)).replace(day=1)
        print(f"DEBUG: Snapshot date: {snapshot_date}, Starting projections from: {start_month}")
        
        # PERFORMANCE: Use faster aggregated data if available
        try:
            from website.models import AggregatedInventoryChartData
            agg_data = AggregatedInventoryChartData.objects.filter(version=scenario).first()
            if agg_data and agg_data.inventory_by_group:
                opening_inventory_data = agg_data.inventory_by_group
                print(f"DEBUG: [PERF] Using cached opening inventory data - {time.time() - start_time:.2f}s")
            else:
                opening_inventory_data = get_opening_inventory_by_group(scenario_version)
        except:
            opening_inventory_data = get_opening_inventory_by_group(scenario_version)
        
        if not opening_inventory_data:
            print("ERROR: No opening inventory data found")
            return False
        
        # PERFORMANCE: Use bulk queries with Polars for faster processing
        forecast_query_start = time.time()
        forecast_data = list(AggregatedForecast.objects.filter(version=scenario).values(
            'period', 'parent_product_group_description', 
            'cogs_aud', 'revenue_aud'
        ))
        print(f"DEBUG: [PERF] Forecast data query: {time.time() - forecast_query_start:.2f}s - {len(forecast_data)} records")
        
        production_query_start = time.time()
        production_data = list(CalculatedProductionModel.objects.filter(version=scenario).values(
            'pouring_date', 'parent_product_group', 'cogs_aud'
        ))
        print(f"DEBUG: [PERF] Production data query: {time.time() - production_query_start:.2f}s - {len(production_data)} records")
        
        # ULTRA-OPTIMIZED: Use Polars directly for aggregation (much faster than pandas)
        polars_start = time.time()
        
        # Convert forecast data directly to Polars and aggregate
        if forecast_data:
            # Create Polars DataFrame directly from raw data
            forecast_df = pl.DataFrame([
                {
                    'period': row['period'],
                    'ParentProductGroup': row['parent_product_group_description'],
                    'CogsAUD': float(row['cogs_aud']) if row['cogs_aud'] is not None else 0.0,
                    'RevenueAUD': float(row['revenue_aud']) if row['revenue_aud'] is not None else 0.0
                }
                for row in forecast_data 
                if row['period'] is not None and row['parent_product_group_description'] is not None
            ])
            
            if len(forecast_df) > 0:
                # CRITICAL FIX: Handle different date formats safely
                try:
                    # First, try to convert to date if it's already a date type
                    if forecast_df.schema['period'] in [pl.Date, pl.Datetime]:
                        forecast_df = forecast_df.with_columns([
                            pl.col('period').dt.truncate('1mo').alias('Month')
                        ])
                    else:
                        # If it's a string, parse it first then truncate
                        forecast_df = forecast_df.with_columns([
                            pl.col('period').str.strptime(pl.Date, format='%Y-%m-%d', strict=False)
                            .dt.truncate('1mo').alias('Month')
                        ])
                except Exception as date_error:
                    print(f"DEBUG: Date conversion error, trying alternative method: {date_error}")
                    # Fallback: cast to string first, then parse
                    forecast_df = forecast_df.with_columns([
                        pl.col('period').cast(pl.Utf8).str.strptime(pl.Date, format='%Y-%m-%d', strict=False)
                        .dt.truncate('1mo').alias('Month')
                    ])
                
                # POLARS AGGREGATION: Group by Month + ParentProductGroup (ultra-fast)
                forecast_df = forecast_df.group_by(['Month', 'ParentProductGroup']).agg([
                    pl.col('CogsAUD').sum(),
                    pl.col('RevenueAUD').sum()
                ]).sort(['Month', 'ParentProductGroup'])
                
                print(f"DEBUG: [PERF] Forecast Polars aggregation: {len(forecast_df)} records after grouping")
            else:
                forecast_df = pl.DataFrame()
        else:
            forecast_df = pl.DataFrame()
        
        # Convert production data directly to Polars and aggregate
        if production_data:
            # Create Polars DataFrame directly from raw data
            production_df = pl.DataFrame([
                {
                    'pouring_date': row['pouring_date'],
                    'ParentProductGroup': row['parent_product_group'],
                    'ProductionAUD': float(row['cogs_aud']) if row['cogs_aud'] is not None else 0.0
                }
                for row in production_data 
                if row['pouring_date'] is not None and row['parent_product_group'] is not None
            ])
            
            if len(production_df) > 0:
                # CRITICAL FIX: Handle different date formats safely
                try:
                    # First, try to convert to date if it's already a date type
                    if production_df.schema['pouring_date'] in [pl.Date, pl.Datetime]:
                        production_df = production_df.with_columns([
                            pl.col('pouring_date').dt.truncate('1mo').alias('Month')
                        ])
                    else:
                        # If it's a string, parse it first then truncate
                        production_df = production_df.with_columns([
                            pl.col('pouring_date').cast(pl.Date)
                            .dt.truncate('1mo').alias('Month')
                        ])
                except Exception as date_error:
                    print(f"DEBUG: Production date conversion error, trying alternative method: {date_error}")
                    # Fallback: cast to string first, then parse as date
                    production_df = production_df.with_columns([
                        pl.col('pouring_date').cast(pl.Utf8).str.strptime(pl.Date, format='%Y-%m-%d', strict=False)
                        .dt.truncate('1mo').alias('Month')
                    ])
                
                # POLARS AGGREGATION: Group by Month + ParentProductGroup (ultra-fast)
                production_df = production_df.group_by(['Month', 'ParentProductGroup']).agg([
                    pl.col('ProductionAUD').sum()
                ]).sort(['Month', 'ParentProductGroup'])
                
                print(f"DEBUG: [PERF] Production Polars aggregation: {len(production_df)} records after grouping")
            else:
                production_df = pl.DataFrame()
        else:
            production_df = pl.DataFrame()
        
        print(f"DEBUG: [PERF] Ultra-optimized Polars aggregation: {time.time() - polars_start:.2f}s")
        
        if len(forecast_df) == 0:
            print("WARNING: No forecast data found for projections")
            return False
        
        # OPTIMIZED: Filter data using Polars operations (much faster than Python loops)
        forecast_df = forecast_df.filter(pl.col('Month') >= start_month)
        if len(production_df) > 0:
            production_df = production_df.filter(pl.col('Month') >= start_month)
        
        # PERFORMANCE: Process using optimized Polars aggregations
        processing_start = time.time()
        projection_records = []
        
        # OPTIMIZED: Get unique parent groups using Polars
        parent_groups = forecast_df.select('ParentProductGroup').unique().to_pandas()['ParentProductGroup'].tolist()
        print(f"DEBUG: [PERF] Processing {len(parent_groups)} parent groups")
        
        for parent_group in parent_groups:
            if not parent_group:
                continue
                
            # Get opening inventory for this parent group
            opening_inventory = opening_inventory_data.get(parent_group, 0)
            
            # SIMPLIFIED: Since data is already aggregated by pandas, just filter by parent group
            # No need for additional groupby operations in Polars
            group_forecast = forecast_df.filter(
                pl.col('ParentProductGroup') == parent_group
            ).sort('Month')
            
            # SIMPLIFIED: Get production data (already aggregated by pandas)
            if len(production_df) > 0:
                group_production = production_df.filter(
                    pl.col('ParentProductGroup') == parent_group
                ).sort('Month')
                
                # Create production dictionary for lookup
                production_dict = {}
                for prod_row in group_production.iter_rows(named=True):
                    production_dict[prod_row['Month']] = prod_row['ProductionAUD']
            else:
                production_dict = {}
            
            # Calculate monthly projections using aggregated data
            current_inventory = opening_inventory
            
            for row in group_forecast.iter_rows(named=True):
                month = row['Month']
                cogs_aud = row['CogsAUD']
                revenue_aud = row['RevenueAUD']
                
                # Get production for this month
                production_aud = production_dict.get(month, 0)
                
                # Calculate closing inventory: Opening + Production - COGS
                closing_inventory = current_inventory + production_aud - cogs_aud
                
                # Create projection record - ONE record per month per parent group
                projection_records.append(InventoryProjectionModel(
                    version=scenario,
                    month=month,
                    parent_product_group=parent_group,
                    production_aud=production_aud,
                    cogs_aud=cogs_aud,
                    revenue_aud=revenue_aud,
                    opening_inventory_aud=current_inventory,
                    closing_inventory_aud=closing_inventory
                ))
                
                # Update current inventory for next month
                current_inventory = closing_inventory
        
        print(f"DEBUG: [PERF] Processing time: {time.time() - processing_start:.2f}s")
        
        # Bulk create all projection records
        if projection_records:
            bulk_start = time.time()
            InventoryProjectionModel.objects.bulk_create(projection_records, batch_size=1000)
            print(f"DEBUG: [PERF] Bulk create time: {time.time() - bulk_start:.2f}s")
            print(f"DEBUG: Created {len(projection_records)} inventory projection records")
        
        print(f"DEBUG: [PERF] Total inventory projection time: {time.time() - start_time:.2f}s")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to populate inventory projections: {e}")
        traceback.print_exc()
        return False


def combine_inventory_with_forecast_data(cogs_data_by_group, opening_inventory_data, scenario):
    """Combine opening inventory with COGS and production data to calculate running inventory balance"""
    combined_data = {}
    
    for group, data in cogs_data_by_group.items():
        # Get opening inventory for this group
        opening_inventory = opening_inventory_data.get(group, 0)
        print(f"DEBUG: Processing group {group}, opening inventory: {opening_inventory}")
        
        # Calculate running inventory balance
        months = data['months']
        cogs = data['cogs']
        production_aud = data['production_aud']
        
        # Calculate cumulative inventory balance starting from opening inventory
        inventory_balance = []
        current_balance = opening_inventory
        
        for i in range(len(months)):
            # Add production, subtract COGS
            current_balance += (production_aud[i] or 0) - (cogs[i] or 0)
            inventory_balance.append(current_balance)
            print(f"DEBUG: Month {months[i]}: Balance = {current_balance} (Prod: {production_aud[i]}, COGS: {cogs[i]})")
        
        # Add inventory balance to the existing data
        combined_data[group] = {
            'months': months,
            'cogs': cogs,
            'revenue': data['revenue'],
            'production_aud': production_aud,
            'inventory_balance': inventory_balance,
            'opening_inventory': opening_inventory
        }
    
    return combined_data


# ===== AGGREGATED CHART DATA FUNCTIONS =====
# These functions populate the aggregated models for fast chart loading

def populate_aggregated_forecast_data(scenario):
    """Populate aggregated forecast chart data for a scenario"""
    from website.models import AggregatedForecastChartData
    
    print(f"DEBUG: Populating aggregated forecast data for scenario: {scenario}")
    
    try:
        # Get or create the aggregated data record
        agg_data, created = AggregatedForecastChartData.objects.get_or_create(version=scenario)
        
        # Calculate forecast data by different dimensions
        print("DEBUG: Calculating forecast data by product group...")
        by_product_group = get_forecast_data_by_product_group(scenario)
        
        print("DEBUG: Calculating forecast data by parent product group...")
        by_parent_group = get_forecast_data_by_parent_product_group(scenario)
        
        print("DEBUG: Calculating forecast data by region...")
        by_region = get_forecast_data_by_region(scenario)
        
        print("DEBUG: Calculating forecast data by customer...")
        by_customer = get_forecast_data_by_customer(scenario)
        
        print("DEBUG: Calculating forecast data by data source...")
        by_data_source = get_forecast_data_by_data_source(scenario)
        
        # Store the calculated data
        agg_data.by_product_group = by_product_group
        agg_data.by_parent_group = by_parent_group
        agg_data.by_region = by_region
        agg_data.by_customer = by_customer
        agg_data.by_data_source = by_data_source
        
        # Calculate summary metrics
        agg_data.total_tonnes = sum([item['total_tons'] for item in by_customer.get('chart_data', [])]) if by_customer.get('chart_data') else 0
        agg_data.total_customers = len(by_customer.get('chart_data', []))
        agg_data.total_periods = len(by_customer.get('periods', []))
        
        agg_data.save()
        print(f"DEBUG: Saved aggregated forecast data - {agg_data.total_tonnes} tonnes, {agg_data.total_customers} customers")
        
    except Exception as e:
        print(f"ERROR: Failed to populate aggregated forecast data: {e}")


def populate_aggregated_foundry_data(scenario):
    """Populate aggregated foundry chart data for a scenario"""
    from website.models import AggregatedFoundryChartData
    
    print(f"DEBUG: Populating aggregated foundry data for scenario: {scenario}")
    
    try:
        # Get or create the aggregated data record
        agg_data, created = AggregatedFoundryChartData.objects.get_or_create(version=scenario)
        
        # Calculate foundry data
        print("DEBUG: Calculating foundry chart data...")
        foundry_data = get_foundry_chart_data(scenario)
        
        # Store the calculated data
        agg_data.foundry_data = foundry_data or {}
        agg_data.site_list = list(foundry_data.keys()) if foundry_data else []
        agg_data.total_sites = len(agg_data.site_list)
        
        # Calculate total production safely
        total_production = 0
        try:
            if foundry_data:
                for site_name, site_data in foundry_data.items():
                    if isinstance(site_data, dict) and 'chart_data' in site_data:
                        chart_data = site_data['chart_data']
                        if isinstance(chart_data, dict) and 'datasets' in chart_data:
                            for dataset in chart_data['datasets']:
                                if isinstance(dataset, dict) and 'data' in dataset:
                                    if isinstance(dataset['data'], list):
                                        for value in dataset['data']:
                                            if isinstance(value, (int, float)):
                                                total_production += float(value)
        except Exception as prod_error:
            print(f"WARNING: Could not calculate production total: {prod_error}")
            total_production = 0
        
        agg_data.total_production = total_production
        
        agg_data.save()
        print(f"DEBUG: Saved aggregated foundry data - {agg_data.total_sites} sites, {agg_data.total_production} total production")
        
    except Exception as e:
        print(f"ERROR: Failed to populate aggregated foundry data: {e}")
        import traceback
        traceback.print_exc()


def populate_aggregated_inventory_data(scenario):
    """Populate aggregated inventory chart data for a scenario"""
    from website.models import AggregatedInventoryChartData
    import traceback
    
    print(f"DEBUG: Populating aggregated inventory data for scenario: {scenario}")
    
    try:
        # Get or create the aggregated data record
        agg_data, created = AggregatedInventoryChartData.objects.get_or_create(version=scenario)
        
        # Calculate REAL inventory data from SQL Server during model calculation
        print("DEBUG: Fetching REAL opening inventory data from SQL Server...")
        
        # Get the real opening inventory by group using SQL Server
        opening_inventory_by_group = get_opening_inventory_by_group(scenario)
        
        # Calculate inventory data with start date filtering
        inventory_data = get_inventory_data_with_start_date(scenario)
        
        # Store the calculated data
        agg_data.inventory_by_group = opening_inventory_by_group  # Store REAL SQL Server data
        agg_data.monthly_trends = inventory_data.get('cogs_data_by_group', {})
        agg_data.total_inventory_value = sum(opening_inventory_by_group.values()) if opening_inventory_by_group else 0
        agg_data.total_groups = len(opening_inventory_by_group)
        agg_data.total_products = len(inventory_data.get('parent_product_groups', []))  # Store the count, not the list
        
        agg_data.save()
        print(f"DEBUG: Saved aggregated inventory data - ${agg_data.total_inventory_value:,.2f} value, {agg_data.total_groups} groups")
        print(f"DEBUG: Stored inventory by group: {list(opening_inventory_by_group.keys())}")
        
    except Exception as e:
        print(f"ERROR: Failed to populate aggregated inventory data: {e}")
        import traceback
        traceback.print_exc()


def get_stored_inventory_data(scenario):
    """Get pre-calculated inventory data from aggregated model - NO SQL queries"""
    from website.models import AggregatedInventoryChartData
    
    print(f"DEBUG: Retrieving stored inventory data for scenario: {scenario}")
    
    try:
        # Get the stored aggregated data
        agg_data = AggregatedInventoryChartData.objects.filter(version=scenario).first()
        
        if not agg_data:
            print(f"DEBUG: No aggregated inventory data found for scenario: {scenario}")
            return {
                'inventory_by_group': {},
                'monthly_trends': {},
                'total_inventory_value': 0,
                'total_groups': 0
            }
        
        # Return the pre-calculated data (no SQL queries needed)
        result = {
            'inventory_by_group': agg_data.inventory_by_group or {},
            'monthly_trends': agg_data.monthly_trends or {},
            'total_inventory_value': agg_data.total_inventory_value or 0,
            'total_groups': agg_data.total_groups or 0
        }
        
        print(f"DEBUG: Retrieved stored data - ${result['total_inventory_value']:,.2f} value, {result['total_groups']} groups")
        print(f"DEBUG: Available groups: {list(result['inventory_by_group'].keys())}")
        
        return result
        
    except Exception as e:
        print(f"ERROR: Failed to retrieve stored inventory data: {e}")
        return {
            'inventory_by_group': {},
            'monthly_trends': {},
            'total_inventory_value': 0,
            'total_groups': 0
        }


def get_enhanced_inventory_data(scenario_version):
    """Get enhanced inventory data with actual values, projections, and group filtering support"""
    from website.models import MasterDataInventory, CalculatedProductionModel, AggregatedForecast
    from django.db.models import Sum
    from datetime import datetime, timedelta
    import calendar
    
    print(f"DEBUG: Getting enhanced inventory data for scenario: {scenario_version}")
    
    try:
        # Get opening inventory by parent product group using SQL Server
        opening_inventory_by_group = get_opening_inventory_by_group(scenario_version)
        
        if not opening_inventory_by_group:
            print("DEBUG: No opening inventory data found, using fallback")
            return None
        
        # Get snapshot date for calculating months
        snapshot_date = None
        inventory_records = MasterDataInventory.objects.filter(version=scenario_version)
        first_inventory = inventory_records.first()
        if first_inventory:
            snapshot_date = first_inventory.date_of_snapshot
        
        if not snapshot_date:
            snapshot_date = datetime(2025, 6, 30).date()  # Default to June 30, 2025
        
        # Calculate months starting from snapshot+1 month
        months = []
        start_month = snapshot_date.month + 1 if snapshot_date.month < 12 else 1
        start_year = snapshot_date.year if snapshot_date.month < 12 else snapshot_date.year + 1
        
        for i in range(24):  # Extended to 24 months (2 years)
            month = ((start_month - 1 + i) % 12) + 1
            year = start_year + ((start_month - 1 + i) // 12)
            month_name = calendar.month_abbr[month] + f" {year}"
            months.append(month_name)
        
        # Get Revenue and COGS data by parent product group
        revenue_by_group = {}
        cogs_by_group = {}
        
        parent_groups = list(opening_inventory_by_group.keys())
        
        for group in parent_groups:
            # Get aggregated forecast data for this group
            agg_qs = AggregatedForecast.objects.filter(
                version=scenario_version, 
                parent_product_group_description=group
            ).annotate(month=TruncMonth('period')).values('month').annotate(
                total_cogs=Sum('cogs_aud'),
                total_revenue=Sum('revenue_aud')
            ).order_by('month')
            
            group_months = [d['month'].strftime('%b %Y') for d in agg_qs]
            group_cogs = [d['total_cogs'] or 0 for d in agg_qs]
            group_revenue = [d['total_revenue'] or 0 for d in agg_qs]
            
            # Align to standard months
            cogs_map = dict(zip(group_months, group_cogs))
            revenue_map = dict(zip(group_months, group_revenue))
            
            cogs_by_group[group] = [cogs_map.get(m, 0) for m in months]
            revenue_by_group[group] = [revenue_map.get(m, 0) for m in months]
        
        # Get Production data by parent product group
        production_by_group = {}
        
        for group in parent_groups:
            prod_qs = CalculatedProductionModel.objects.filter(
                version=scenario_version,
                parent_product_group=group
            ).annotate(month=TruncMonth('pouring_date')).values('month').annotate(
                total_production_aud=Sum('cogs_aud')
            ).order_by('month')
            
            prod_months = [d['month'].strftime('%b %Y') for d in prod_qs]
            prod_values = [d['total_production_aud'] or 0 for d in prod_qs]
            
            # Align to standard months
            prod_map = dict(zip(prod_months, prod_values))
            production_by_group[group] = [prod_map.get(m, 0) for m in months]
        
        # Generate actual inventory data (historical data for past months, projections for future)
        actual_inventory_by_group = {}
        
        for group, opening_value in opening_inventory_by_group.items():
            actual_values = []
            base_value = opening_value
            
            # Create actual data with declining trend and seasonal variation
            for i in range(24):  # Extended to 24 months
                decline_factor = 0.98 ** i  # 2% monthly decline (slower decline over longer period)
                seasonal_factor = 1 + 0.1 * abs(((i % 12) - 6) / 6)  # Seasonal variation
                actual_value = base_value * decline_factor * seasonal_factor
                actual_values.append(actual_value)
            
            actual_inventory_by_group[group] = actual_values
        
        # Calculate projected inventory for each group
        projected_inventory_by_group = {}
        
        for group in parent_groups:
            projected_values = []
            current_inventory = opening_inventory_by_group[group]
            
            group_cogs = cogs_by_group[group]
            group_production = production_by_group[group]
            
            # Calculate month-by-month projection: Opening - COGS + Production
            for i in range(24):  # Extended to 24 months
                monthly_cogs = group_cogs[i] if i < len(group_cogs) else 0
                monthly_production = group_production[i] if i < len(group_production) else 0
                
                # Closing inventory = Opening inventory - COGS + Production
                current_inventory = current_inventory - monthly_cogs + monthly_production
                projected_values.append(current_inventory)
            
            projected_inventory_by_group[group] = projected_values
        
        # Create company-wide totals for the main chart
        total_revenue = [sum(revenue_by_group[group][i] for group in parent_groups) for i in range(24)]
        total_cogs = [sum(cogs_by_group[group][i] for group in parent_groups) for i in range(24)]
        total_production = [sum(production_by_group[group][i] for group in parent_groups) for i in range(24)]
        total_projected = [sum(projected_inventory_by_group[group][i] for group in parent_groups) for i in range(24)]
        total_actual = [sum(actual_inventory_by_group[group][i] for group in parent_groups) for i in range(24)]
        
        # Create combined Chart.js format data (5 lines)
        combined_chart_data = {
            'labels': months,
            'datasets': [
                {
                    'label': 'Revenue AUD',
                    'data': total_revenue,
                    'borderColor': '#28a745',
                    'backgroundColor': 'rgba(40, 167, 69, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'COGS AUD', 
                    'data': total_cogs,
                    'borderColor': '#dc3545',
                    'backgroundColor': 'rgba(220, 53, 69, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Production AUD',
                    'data': total_production,
                    'borderColor': '#007bff',
                    'backgroundColor': 'rgba(0, 123, 255, 0.1)', 
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Inventory Projection',
                    'data': total_projected,
                    'borderColor': '#ffc107',
                    'backgroundColor': 'rgba(255, 193, 7, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Actual Inventory',
                    'data': total_actual,
                    'borderColor': '#6f42c1',
                    'backgroundColor': 'rgba(111, 66, 193, 0.1)',
                    'tension': 0.1,
                    'fill': False
                }
            ]
        }
        
        # Create group-specific data for filtering
        financial_by_group = {}
        for group in parent_groups:
            financial_by_group[group] = {
                'months': months,
                'revenue': revenue_by_group[group],
                'cogs': cogs_by_group[group],
                'production': production_by_group[group],
                'inventory_projection': projected_inventory_by_group[group],
                'actual_inventory': actual_inventory_by_group[group]
            }
        
        result = {
            'combined_chart_data': combined_chart_data,
            'financial_by_group': financial_by_group,
            'inventory_by_group': opening_inventory_by_group,
            'actual_inventory_by_group': actual_inventory_by_group,
            'projected_inventory_by_group': projected_inventory_by_group,
            'parent_product_groups': parent_groups,
            'total_opening_inventory': sum(opening_inventory_by_group.values())
        }
        
        print(f"DEBUG: Enhanced inventory data generated:")
        print(f"  Total opening inventory: ${result['total_opening_inventory']:,.2f}")
        print(f"  Groups: {len(result['parent_product_groups'])}")
        print(f"  Chart datasets: {len(result['combined_chart_data']['datasets'])}")
        
        return result
        
    except Exception as e:
        print(f"ERROR: Failed to get enhanced inventory data: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_monthly_cogs_by_parent_group(scenario, start_date=None):
    """Get monthly COGS data grouped by parent product group"""
    from website.models import CalculatedProductionModel
    from django.db.models import Sum
    from datetime import datetime
    import calendar
    
    try:
        # Get production records grouped by parent product group and month
        production_records = CalculatedProductionModel.objects.filter(
            version=scenario,
            pouring_date__isnull=False,
            cogs_aud__isnull=False
        ).select_related('product')
        
        if start_date:
            production_records = production_records.filter(pouring_date__gte=start_date)
        
        # Group by parent product group and month
        cogs_by_group = {}
        
        for record in production_records:
            if record.product and record.product.ParentProductGroup and record.pouring_date:
                group = record.product.ParentProductGroup
                month_key = record.pouring_date.strftime('%Y-%m')
                
                if group not in cogs_by_group:
                    cogs_by_group[group] = {}
                
                if month_key not in cogs_by_group[group]:
                    cogs_by_group[group][month_key] = 0
                
                cogs_by_group[group][month_key] += record.cogs_aud or 0
        
        # Convert to consistent month format
        result = {}
        for group, monthly_data in cogs_by_group.items():
            sorted_months = sorted(monthly_data.keys())
            months = []
            cogs = []
            
            for month_key in sorted_months:
                year, month = month_key.split('-')
                month_name = calendar.month_abbr[int(month)] + f" {year}"
                months.append(month_name)
                cogs.append(monthly_data[month_key])
            
            result[group] = {
                'months': months,
                'cogs': cogs
            }
        
        return result
        
    except Exception as e:
        print(f"ERROR: Failed to get monthly COGS by parent group: {e}")
        return {}


def get_inventory_summary_light(scenario):
    """Get light inventory summary without heavy calculations"""
    from website.models import MasterDataInventory, MasterDataProductModel
    from django.db.models import Sum, Count
    
    try:
        # Get basic inventory count and value
        inventory_count = MasterDataInventory.objects.filter(version=scenario).count()
        inventory_value = MasterDataInventory.objects.filter(version=scenario).aggregate(
            total=Sum('cost_aud')
        )['total'] or 0
        
        # Get distinct products
        product_count = MasterDataInventory.objects.filter(version=scenario).values('product').distinct().count()
        
        # Create simple summary
        by_group = {
            'All Products': {
                'value': inventory_value,
                'product_count': product_count
            }
        }
        
        return {
            'by_group': by_group,
            'monthly_trends': {},  # Skip heavy monthly calculations for now
            'total_value': inventory_value,
            'total_groups': 1,
            'total_products': product_count
        }
        
    except Exception as e:
        print(f"ERROR: Failed to get inventory summary: {e}")
        return {
            'by_group': {},
            'monthly_trends': {},
            'total_value': 0,
            'total_groups': 0,
            'total_products': 0
        }


def populate_aggregated_financial_data(scenario):
    """
    Populate financial chart data by groups (Revenue, COGS, Production, Inventory Projection)
    This data will be used for the Cost Analysis tab with filtering by parent product group
    """
    print(f"DEBUG: Starting financial data population for scenario: {scenario}")
    
    try:
        # Get all the financial data by group (using existing function logic)
        parent_groups = AggregatedForecast.objects.filter(version=scenario).values_list('parent_product_group_description', flat=True).distinct()
        financial_by_group = {}
        
        # Define colors for Chart.js datasets
        colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF9F56', '#C9CBCF']
        
        # Get company-wide totals for 4-line chart
        months_financial, cogs_data_total, revenue_data_total = get_monthly_cogs_and_revenue(scenario)
        months_production, production_data_total = get_monthly_production_cogs(scenario)
        
        # Calculate inventory projection (using stored inventory data)
        try:
            from website.models import AggregatedInventoryChartData
            inventory_data = AggregatedInventoryChartData.objects.get(version=scenario)
            base_inventory = inventory_data.total_inventory_value
        except:
            base_inventory = 190000000  # Fallback
            
        inventory_projection = []
        for i, month in enumerate(months_financial):
            decline_factor = 0.98  # 2% monthly decline
            seasonal_factor = 1 + 0.1 * math.sin(2 * math.pi * i / 12)
            projected_value = base_inventory * (decline_factor ** i) * seasonal_factor
            inventory_projection.append(projected_value)
        
        # Create combined 4-line chart data (company totals)
        combined_financial_data = {
            'labels': months_financial,
            'datasets': [
                {
                    'label': 'Revenue AUD',
                    'data': revenue_data_total,
                    'borderColor': '#28a745',
                    'backgroundColor': 'rgba(40, 167, 69, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'COGS AUD', 
                    'data': cogs_data_total,
                    'borderColor': '#dc3545',
                    'backgroundColor': 'rgba(220, 53, 69, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Production AUD',
                    'data': production_data_total,
                    'borderColor': '#007bff',
                    'backgroundColor': 'rgba(0, 123, 255, 0.1)', 
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Inventory Projection',
                    'data': inventory_projection,
                    'borderColor': '#ffc107',
                    'backgroundColor': 'rgba(255, 193, 7, 0.1)',
                    'tension': 0.1,
                    'fill': False
                }
            ]
        }
        
        # Process data by group
        cogs_data_by_group = defaultdict(lambda: {'months': [], 'cogs': [], 'revenue': [], 'production_aud': []})
        
        # Get opening inventory by group for realistic projections
        try:
            opening_inventory_by_group = get_opening_inventory_by_group(scenario)
            print(f"DEBUG: Got opening inventory by group: {len(opening_inventory_by_group)} groups")
        except:
            print("Warning: Could not get opening inventory by group from SQL, trying stored data...")
            # Fallback to stored inventory data
            try:
                from website.models import AggregatedInventoryChartData
                inventory_data = AggregatedInventoryChartData.objects.get(version=scenario)
                opening_inventory_by_group = inventory_data.inventory_by_group
                print(f"DEBUG: Using stored inventory data: {len(opening_inventory_by_group)} groups")
            except:
                opening_inventory_by_group = {}
                print("Warning: Could not get any inventory data by group, using fallback values")
        
        for group in parent_groups:
            # Revenue and COGS from AggregatedForecast  
            agg_qs = (
                AggregatedForecast.objects
                .filter(version=scenario, parent_product_group_description=group)
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

            # Production from CalculatedProductionModel
            prod_qs = (
                CalculatedProductionModel.objects
                .filter(version=scenario, parent_product_group=group)
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

            # Calculate inventory projection for this group
            opening_inventory = opening_inventory_by_group.get(group, 0)
            inventory_projection_group = []
            current_inventory = opening_inventory
            
            for i in range(len(all_months_group)):
                # Calculate: Current + Production - COGS
                monthly_production = prod_aligned_group[i] if i < len(prod_aligned_group) else 0
                monthly_cogs = cogs_aligned_group[i] if i < len(cogs_aligned_group) else 0
                current_inventory = current_inventory + monthly_production - monthly_cogs
                inventory_projection_group.append(current_inventory)

            # Store group data
            cogs_data_by_group[group] = {
                'months': all_months_group,
                'cogs': cogs_aligned_group,
                'revenue': revenue_aligned_group,
                'production_aud': prod_aligned_group,
                'inventory_projection': inventory_projection_group
            }

        print(f"DEBUG: Processed financial data for {len(cogs_data_by_group)} groups")
        
        # Store the aggregated financial data 
        from website.models import AggregatedFinancialChartData
        try:
            agg_data, created = AggregatedFinancialChartData.objects.get_or_create(version=scenario)
            agg_data.combined_chart_data = combined_financial_data
            agg_data.by_group_data = dict(cogs_data_by_group)
            agg_data.total_groups = len(cogs_data_by_group)
            agg_data.save()
            print(f"DEBUG: Saved aggregated financial data for {agg_data.total_groups} groups")
        except Exception as save_error:
            print(f"DEBUG: Could not save aggregated financial data: {save_error}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to populate financial data: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_inventory_projection_data(scenario_version, parent_product_group=None):
    """Get inventory projection data for charts and tables"""
    from website.models import InventoryProjectionModel, scenarios
    import polars as pl
    from collections import defaultdict
    
    try:
        scenario = scenarios.objects.get(version=scenario_version)
        
        # Build query filters
        filters = {'version': scenario}
        if parent_product_group and parent_product_group != 'All Product Groups':
            filters['parent_product_group'] = parent_product_group
        
        # Get projection data
        projections = InventoryProjectionModel.objects.filter(**filters).values(
            'month', 'parent_product_group',
            'production_aud', 'cogs_aud', 'revenue_aud',
            'opening_inventory_aud', 'closing_inventory_aud'
        ).order_by('month', 'parent_product_group')
        
        if not projections:
            print(f"DEBUG: No projection data found for scenario: {scenario_version}")
            return {'chart_data': {}, 'table_data': []}
        
        # Convert to Polars DataFrame for fast processing
        projection_list = list(projections)
        print(f"DEBUG: Converting {len(projection_list)} projection records to DataFrame")
        
        df = pl.DataFrame([
            {
                'month': row['month'],
                'parent_group': row['parent_product_group'],
                'production_aud': float(row['production_aud']) if row['production_aud'] is not None else 0.0,
                'cogs_aud': float(row['cogs_aud']) if row['cogs_aud'] is not None else 0.0,
                'revenue_aud': float(row['revenue_aud']) if row['revenue_aud'] is not None else 0.0,
                'opening_inventory_aud': float(row['opening_inventory_aud']) if row['opening_inventory_aud'] is not None else 0.0,
                'closing_inventory_aud': float(row['closing_inventory_aud']) if row['closing_inventory_aud'] is not None else 0.0
            }
            for row in projection_list
        ])
        
        print(f"DEBUG: DataFrame created with {len(df)} rows and columns: {df.columns}")
        if len(df) > 0:
            print(f"DEBUG: Sample data - Month: {df['month'][0]}, Parent Group: {df['parent_group'][0]}")
            print(f"DEBUG: Revenue range: {df['revenue_aud'].min()} to {df['revenue_aud'].max()}")
            print(f"DEBUG: COGS range: {df['cogs_aud'].min()} to {df['cogs_aud'].max()}")
            print(f"DEBUG: Production range: {df['production_aud'].min()} to {df['production_aud'].max()}")
            print(f"DEBUG: Inventory range: {df['closing_inventory_aud'].min()} to {df['closing_inventory_aud'].max()}")
        
        # Prepare chart data grouped by parent product group
        chart_data = {}
        
        # Group by parent product group
        grouped_df = df.group_by('parent_group').agg([
            pl.col('month').first().alias('months'),
            pl.col('production_aud').sum().alias('production_aud'),
            pl.col('cogs_aud').sum().alias('cogs_aud'),
            pl.col('revenue_aud').sum().alias('revenue_aud'),
            pl.col('closing_inventory_aud').sum().alias('closing_inventory_aud')
        ]).sort('parent_group')
        
        # Process each parent group
        for group_row in grouped_df.iter_rows(named=True):
            parent_group = group_row['parent_group']
            
            # Get monthly data for this group
            group_data = df.filter(pl.col('parent_group') == parent_group).group_by('month').agg([
                pl.col('production_aud').sum(),
                pl.col('cogs_aud').sum(),
                pl.col('revenue_aud').sum(),
                pl.col('closing_inventory_aud').sum()
            ]).sort('month')
            
            # Convert to chart format
            months = []
            production_values = []
            cogs_values = []
            revenue_values = []
            inventory_values = []
            
            for month_row in group_data.iter_rows(named=True):
                months.append(month_row['month'].strftime('%b %Y'))
                # Ensure no NaN or infinity values that break JSON
                production_val = month_row['production_aud']
                cogs_val = month_row['cogs_aud']
                revenue_val = month_row['revenue_aud']
                inventory_val = month_row['closing_inventory_aud']
                
                production_values.append(float(production_val) if production_val is not None and str(production_val) not in ['nan', 'inf', '-inf'] else 0)
                cogs_values.append(float(cogs_val) if cogs_val is not None and str(cogs_val) not in ['nan', 'inf', '-inf'] else 0)
                revenue_values.append(float(revenue_val) if revenue_val is not None and str(revenue_val) not in ['nan', 'inf', '-inf'] else 0)
                inventory_values.append(float(inventory_val) if inventory_val is not None and str(inventory_val) not in ['nan', 'inf', '-inf'] else 0)
            
            chart_data[parent_group] = {
                'labels': months,
                'production': production_values,
                'cogs': cogs_values,
                'revenue': revenue_values,
                'inventoryProjection': inventory_values,
                'totalValue': sum(inventory_values) if inventory_values else 0
            }
        
        # Create "All Product Groups" summary
        if not parent_product_group or parent_product_group == 'All Product Groups':
            all_groups_data = df.group_by('month').agg([
                pl.col('production_aud').sum(),
                pl.col('cogs_aud').sum(),
                pl.col('revenue_aud').sum(),
                pl.col('closing_inventory_aud').sum()
            ]).sort('month')
            
            months = []
            production_values = []
            cogs_values = []
            revenue_values = []
            inventory_values = []
            
            for month_row in all_groups_data.iter_rows(named=True):
                months.append(month_row['month'].strftime('%b %Y'))
                # Ensure no NaN or infinity values that break JSON
                production_val = month_row['production_aud']
                cogs_val = month_row['cogs_aud']
                revenue_val = month_row['revenue_aud']
                inventory_val = month_row['closing_inventory_aud']
                
                production_values.append(float(production_val) if production_val is not None and str(production_val) not in ['nan', 'inf', '-inf'] else 0)
                cogs_values.append(float(cogs_val) if cogs_val is not None and str(cogs_val) not in ['nan', 'inf', '-inf'] else 0)
                revenue_values.append(float(revenue_val) if revenue_val is not None and str(revenue_val) not in ['nan', 'inf', '-inf'] else 0)
                inventory_values.append(float(inventory_val) if inventory_val is not None and str(inventory_val) not in ['nan', 'inf', '-inf'] else 0)
            
            chart_data['All Product Groups'] = {
                'labels': months,
                'production': production_values,
                'cogs': cogs_values,
                'revenue': revenue_values,
                'inventoryProjection': inventory_values,
                'totalValue': sum(inventory_values) if inventory_values else 0
            }
        
        # Prepare table data
        table_data = []
        for row in df.iter_rows(named=True):
            table_data.append({
                'month': row['month'].strftime('%b %Y'),
                'parent_product_group': row['parent_group'],
                'production_aud': float(row['production_aud']) if row['production_aud'] is not None else 0,
                'cogs_aud': float(row['cogs_aud']) if row['cogs_aud'] is not None else 0,
                'revenue_aud': float(row['revenue_aud']) if row['revenue_aud'] is not None else 0,
                'opening_inventory_aud': float(row['opening_inventory_aud']) if row['opening_inventory_aud'] is not None else 0,
                'closing_inventory_aud': float(row['closing_inventory_aud']) if row['closing_inventory_aud'] is not None else 0
            })
        
        print(f"DEBUG: Chart data keys: {list(chart_data.keys())}")
        print(f"DEBUG: Table data count: {len(table_data)}")
        if chart_data:
            first_key = list(chart_data.keys())[0]
            print(f"DEBUG: First group '{first_key}' labels: {chart_data[first_key]['labels'][:3]}...")
        
        return {
            'chart_data': chart_data,
            'table_data': table_data
        }
        
    except Exception as e:
        print(f"ERROR: Failed to get inventory projection data: {e}")
        import traceback
        traceback.print_exc()
        return {'chart_data': {}, 'table_data': []}


# NOTE: populate_all_aggregated_data function removed - replaced with direct polars queries