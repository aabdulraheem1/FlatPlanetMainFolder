"""
Fast Control Tower Data Calculation using Polars
This replaces the slow calculate_control_tower_data() function with fast polars queries.
"""

import polars as pl
import time
from datetime import date
from django.db import connection


def get_fast_control_tower_data(scenario_version):
    """
    Fast polars-based control tower data calculation.
    Direct database queries without aggregation for maximum speed.
    """
    start_time = time.time()
    
    # Handle both string and object scenario_version
    scenario_name = scenario_version if isinstance(scenario_version, str) else scenario_version.version
    print(f"🚀 Fast control tower calculation for scenario: {scenario_name}")
    
    # Get pour plan data (CalcualtedReplenishmentModel)
    forecast_start = time.time()
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 
                cr.Site_id as site,
                cr.ShippingDate as pouring_date,
                cr.ReplenishmentQty as tonnes
            FROM website_calcualtedreplenishmentmodel cr
            WHERE cr.version_id = %s 
                AND cr.ReplenishmentQty IS NOT NULL 
                AND cr.ReplenishmentQty > 0
                AND cr.Site_id IS NOT NULL
        """, [scenario_name])
        
        forecast_records = cursor.fetchall()
        forecast_df = pl.DataFrame({
            'site': [r[0] for r in forecast_records],
            'pouring_date': [r[1] for r in forecast_records],
            'tonnes': [float(r[2]) for r in forecast_records]
        })
    
    forecast_time = time.time() - forecast_start
    print(f"📊 Forecast records: {len(forecast_records):,} in {forecast_time:.3f}s")
    
    # Get demand plan data (CalculatedProductionModel)
    foundry_start = time.time()
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 
                cp.site_id as site,
                cp.pouring_date,
                cp.tonnes
            FROM website_calculatedproductionmodel cp
            WHERE cp.version_id = %s 
                AND cp.tonnes IS NOT NULL 
                AND cp.tonnes > 0
                AND cp.site_id IS NOT NULL
        """, [scenario_name])
        
        foundry_records = cursor.fetchall()
        foundry_df = pl.DataFrame({
            'site': [r[0] for r in foundry_records],
            'pouring_date': [r[1] for r in foundry_records],
            'tonnes': [float(r[2]) for r in foundry_records]
        })
    
    foundry_time = time.time() - foundry_start
    print(f"🏭 Production records: {len(foundry_records):,} in {foundry_time:.3f}s")
    
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
    
    # Fast polars aggregation for pour plan by fiscal year and site
    agg_start = time.time()
    pour_plan = {}
    demand_plan = {}
    
    for fy, (start_date, end_date) in fy_ranges.items():
        # Pour plan aggregation
        if len(forecast_df) > 0:
            fy_forecast = forecast_df.filter(
                (pl.col("pouring_date") >= start_date) & 
                (pl.col("pouring_date") <= end_date)
            )
            
            if len(fy_forecast) > 0:
                pour_totals = fy_forecast.group_by("site").agg(
                    pl.col("tonnes").sum().alias("total_tonnes")
                ).to_dict(as_series=False)
                
                pour_plan[fy] = {}
                for i, site in enumerate(pour_totals.get("site", [])):
                    tonnes = pour_totals.get("total_tonnes", [])[i]
                    pour_plan[fy][site] = round(float(tonnes))
            else:
                pour_plan[fy] = {}
        else:
            pour_plan[fy] = {}
        
        # Demand plan aggregation
        if len(foundry_df) > 0:
            fy_foundry = foundry_df.filter(
                (pl.col("pouring_date") >= start_date) & 
                (pl.col("pouring_date") <= end_date)
            )
            
            if len(fy_foundry) > 0:
                demand_totals = fy_foundry.group_by("site").agg(
                    pl.col("tonnes").sum().alias("total_tonnes")
                ).to_dict(as_series=False)
                
                demand_plan[fy] = {}
                for i, site in enumerate(demand_totals.get("site", [])):
                    tonnes = demand_totals.get("total_tonnes", [])[i]
                    demand_plan[fy][site] = round(float(tonnes))
            else:
                demand_plan[fy] = {}
        else:
            demand_plan[fy] = {}
        
        # Ensure all sites have values
        for site in sites:
            if site not in pour_plan[fy]:
                pour_plan[fy][site] = 0
            if site not in demand_plan[fy]:
                demand_plan[fy][site] = 0
    
    agg_time = time.time() - agg_start
    print(f"⚡ Polars aggregation: {agg_time:.3f}s")
    
    # Prepare data structure for template (same format as original function)
    control_tower_fy = {}
    for fy in fy_ranges.keys():
        control_tower_rows = []
        total_pour_plan = 0
        total_demand_plan = 0
        
        # Process each site
        for site in sites:
            pour = pour_plan.get(fy, {}).get(site, 0)
            demand = demand_plan.get(fy, {}).get(site, 0)
            
            control_tower_rows.append({
                'site_code': site,
                'site_name': site_map[site],
                'budget': '',
                'capacity': '',
                'pour_plan': round(pour),
                'demand_plan': round(demand)
            })
            
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
        
        # Add Outsource row
        control_tower_rows.append({
            'site_code': 'OUTSOURCE',
            'site_name': 'Outsource',
            'budget': '',
            'capacity': '',
            'pour_plan': '',
            'demand_plan': ''
        })
        
        # Add Total Production row
        control_tower_rows.append({
            'site_code': 'TOTAL_PRODUCTION',
            'site_name': 'Total Production',
            'budget': '',
            'capacity': '',
            'pour_plan': round(total_pour_plan),
            'demand_plan': round(total_demand_plan)
        })
        
        control_tower_fy[fy] = control_tower_rows
    
    total_time = time.time() - start_time
    print(f"⚡ Fast control tower calculation completed in {total_time:.3f}s")
    
    return {
        'control_tower_fy': control_tower_fy,
        'sites': sites,
        'display_sites': [site_map[code] for code in sites],
        'fys': list(fy_ranges.keys()),
        'pour_plan': pour_plan,
        'combined_demand_plan': demand_plan,  # Using same structure for compatibility
        'poured_data': {},  # Not needed for template rendering
    }


# Alias for backward compatibility
calculate_control_tower_data_fast = get_fast_control_tower_data
