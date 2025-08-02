"""
PROOF OF CONCEPT: Direct Polars Queries for Review Scenario
Skip caching entirely - read directly from CalculatedProductionModel and CalcualtedReplenishmentModel
"""

import polars as pl
import pandas as pd
from django.db import connection
import time

def get_review_scenario_data_direct_polars(scenario_version):
    """
    Get ALL review scenario data directly from calculated tables using polars
    NO CACHING - Direct queries only
    Should be sub-second performance for 60k records
    """
    print(f"DEBUG: Starting DIRECT polars queries for review scenario: {scenario_version}")
    start_time = time.time()
    
    results = {}
    
    try:
        # ===== 1. FOUNDRY CHART DATA =====
        foundry_start = time.time()
        foundry_data = get_foundry_data_direct_polars(scenario_version)
        foundry_time = time.time() - foundry_start
        results['foundry_data'] = foundry_data
        print(f"  ✅ Foundry data: {foundry_time:.2f}s")
        
        # ===== 2. INVENTORY ANALYSIS =====
        inventory_start = time.time()
        inventory_data = get_inventory_analysis_direct_polars(scenario_version)
        inventory_time = time.time() - inventory_start
        results['inventory_data'] = inventory_data
        print(f"  ✅ Inventory analysis: {inventory_time:.2f}s")
        
        # ===== 3. FORECAST BREAKDOWN =====
        forecast_start = time.time()
        forecast_data = get_forecast_breakdown_direct_polars(scenario_version)
        forecast_time = time.time() - forecast_start
        results['forecast_data'] = forecast_data
        print(f"  ✅ Forecast breakdown: {forecast_time:.2f}s")
        
        # ===== 4. CONTROL TOWER METRICS =====
        control_start = time.time()
        control_tower_data = get_control_tower_direct_polars(scenario_version)
        control_time = time.time() - control_start
        results['control_tower_data'] = control_tower_data
        print(f"  ✅ Control tower: {control_time:.2f}s")
        
        total_time = time.time() - start_time
        print(f"🚀 TOTAL DIRECT POLARS TIME: {total_time:.2f} seconds")
        print(f"💡 Compare to current caching: 12+ minutes → {total_time:.2f}s = {(12*60/total_time):.0f}x faster!")
        
        return results
        
    except Exception as e:
        print(f"ERROR in direct polars queries: {e}")
        import traceback
        traceback.print_exc()
        return {}

def get_foundry_data_direct_polars(scenario_version):
    """Get foundry production data directly from CalculatedProductionModel"""
    
    with connection.cursor() as cursor:
        # Get all production data for foundries in one query
        cursor.execute("""
            SELECT 
                site_id,
                parent_product_group,
                DATE_FORMAT(pouring_date, '%%Y-%%m') as month,
                SUM(production_quantity) as total_tonnes,
                SUM(cogs_aud) as total_cogs_aud
            FROM website_calculatedproductionmodel 
            WHERE version = %s
            GROUP BY site_id, parent_product_group, month
            ORDER BY site_id, month
        """, [scenario_version])
        
        data = cursor.fetchall()
        columns = ['site_id', 'parent_product_group', 'month', 'total_tonnes', 'total_cogs_aud']
    
    if not data:
        return {}
    
    # Convert to polars DataFrame for fast processing
    df = pl.DataFrame({col: [row[i] for row in data] for i, col in enumerate(columns)})
    
    # Process foundry data by site
    foundries = ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1']
    foundry_results = {}
    
    for foundry in foundries:
        foundry_df = df.filter(pl.col('site_id') == foundry)
        
        if len(foundry_df) > 0:
            # Create Chart.js format data
            chart_data = foundry_df.group_by(['parent_product_group', 'month']).agg([
                pl.col('total_tonnes').sum(),
                pl.col('total_cogs_aud').sum()
            ]).sort(['parent_product_group', 'month'])
            
            # Convert to format expected by frontend
            labels = sorted(chart_data['month'].unique().to_list())
            datasets = []
            
            for group in chart_data['parent_product_group'].unique().to_list():
                group_data = chart_data.filter(pl.col('parent_product_group') == group)
                tonnes_by_month = {row['month']: row['total_tonnes'] for row in group_data.iter_rows(named=True)}
                
                datasets.append({
                    'label': group,
                    'data': [tonnes_by_month.get(month, 0) for month in labels],
                    'backgroundColor': f'rgba({hash(group) % 255}, {(hash(group)*2) % 255}, {(hash(group)*3) % 255}, 0.6)'
                })
            
            foundry_results[foundry] = {
                'chart_data': {'labels': labels, 'datasets': datasets},
                'total_production': foundry_df['total_tonnes'].sum()
            }
    
    return foundry_results

def get_inventory_analysis_direct_polars(scenario_version):
    """Get inventory analysis directly from calculated tables"""
    
    # Real inventory data from SQL Server (already fast)
    from website.customized_function import get_opening_inventory_by_group
    opening_inventory = get_opening_inventory_by_group(scenario_version)
    
    with connection.cursor() as cursor:
        # Get production and consumption data
        cursor.execute("""
            SELECT 
                parent_product_group,
                DATE_FORMAT(pouring_date, '%%Y-%%m') as month,
                SUM(cogs_aud) as production_aud
            FROM website_calculatedproductionmodel 
            WHERE version = %s
            GROUP BY parent_product_group, month
            ORDER BY parent_product_group, month
        """, [scenario_version])
        
        prod_data = cursor.fetchall()
        prod_columns = ['parent_product_group', 'month', 'production_aud']
    
    if not prod_data:
        return {'opening_inventory': opening_inventory, 'monthly_trends': {}}
    
    # Convert to polars for fast processing
    prod_df = pl.DataFrame({col: [row[i] for row in prod_data] for i, col in enumerate(prod_columns)})
    
    # Calculate inventory projections by group
    inventory_projections = {}
    
    for group in opening_inventory.keys():
        group_prod = prod_df.filter(pl.col('parent_product_group') == group)
        
        if len(group_prod) > 0:
            # Simple projection: opening + cumulative production
            months = sorted(group_prod['month'].unique().to_list())
            production_by_month = {row['month']: row['production_aud'] for row in group_prod.iter_rows(named=True)}
            
            running_balance = opening_inventory[group]
            projections = []
            
            for month in months:
                running_balance += production_by_month.get(month, 0)
                projections.append(running_balance)
            
            inventory_projections[group] = {
                'months': months,
                'projections': projections,
                'opening': opening_inventory[group]
            }
    
    return {
        'opening_inventory': opening_inventory,
        'inventory_projections': inventory_projections,
        'total_opening_value': sum(opening_inventory.values())
    }

def get_forecast_breakdown_direct_polars(scenario_version):
    """Get forecast breakdown directly from AggregatedForecast"""
    
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 
                parent_product_group_description,
                customer_code,
                forecast_region,
                DATE_FORMAT(period, '%%Y-%%m') as month,
                SUM(tonnes) as total_tonnes,
                SUM(revenue_aud) as total_revenue,
                SUM(cogs_aud) as total_cogs
            FROM website_aggregatedforecast 
            WHERE version = %s
            GROUP BY parent_product_group_description, customer_code, forecast_region, month
        """, [scenario_version])
        
        data = cursor.fetchall()
        columns = ['parent_product_group', 'customer_code', 'forecast_region', 'month', 
                  'total_tonnes', 'total_revenue', 'total_cogs']
    
    if not data:
        return {}
    
    # Convert to polars for fast aggregations
    df = pl.DataFrame({col: [row[i] for row in data] for i, col in enumerate(columns)})
    
    # Multiple breakdowns in one go
    results = {}
    
    # By product group
    by_group = df.group_by('parent_product_group').agg([
        pl.col('total_tonnes').sum(),
        pl.col('total_revenue').sum(),
        pl.col('total_cogs').sum()
    ]).sort('total_tonnes', descending=True)
    
    results['by_product_group'] = [{
        'group': row['parent_product_group'],
        'tonnes': row['total_tonnes'],
        'revenue': row['total_revenue'],
        'cogs': row['total_cogs']
    } for row in by_group.iter_rows(named=True)]
    
    # By region
    by_region = df.group_by('forecast_region').agg([
        pl.col('total_tonnes').sum(),
        pl.col('total_revenue').sum()
    ]).sort('total_tonnes', descending=True)
    
    results['by_region'] = [{
        'region': row['forecast_region'],
        'tonnes': row['total_tonnes'],
        'revenue': row['total_revenue']
    } for row in by_region.iter_rows(named=True)]
    
    # Monthly trends
    monthly = df.group_by('month').agg([
        pl.col('total_tonnes').sum(),
        pl.col('total_revenue').sum(),
        pl.col('total_cogs').sum()
    ]).sort('month')
    
    results['monthly_trends'] = {
        'months': monthly['month'].to_list(),
        'tonnes': monthly['total_tonnes'].to_list(),
        'revenue': monthly['total_revenue'].to_list(),
        'cogs': monthly['total_cogs'].to_list()
    }
    
    return results

def get_control_tower_direct_polars(scenario_version):
    """Get control tower metrics directly from calculated tables"""
    
    with connection.cursor() as cursor:
        # Production by site and fiscal year
        cursor.execute("""
            SELECT 
                site_id,
                CASE 
                    WHEN pouring_date >= '2024-04-01' AND pouring_date <= '2025-03-31' THEN 'FY25'
                    WHEN pouring_date >= '2025-04-01' AND pouring_date <= '2026-03-31' THEN 'FY26'
                    WHEN pouring_date >= '2026-04-01' AND pouring_date <= '2027-03-31' THEN 'FY27'
                    ELSE 'Other'
                END as fiscal_year,
                SUM(production_quantity) as total_tonnes
            FROM website_calculatedproductionmodel 
            WHERE version = %s
              AND pouring_date IS NOT NULL
            GROUP BY site_id, fiscal_year
        """, [scenario_version])
        
        data = cursor.fetchall()
        columns = ['site_id', 'fiscal_year', 'total_tonnes']
    
    if not data:
        return {}
    
    df = pl.DataFrame({col: [row[i] for row in data] for i, col in enumerate(columns)})
    
    # Create control tower summary
    sites = ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1']
    fiscal_years = ['FY25', 'FY26', 'FY27']
    
    control_tower = {}
    
    for fy in fiscal_years:
        fy_data = df.filter(pl.col('fiscal_year') == fy)
        
        fy_summary = {}
        for site in sites:
            site_data = fy_data.filter(pl.col('site_id') == site)
            tonnes = site_data['total_tonnes'].sum() if len(site_data) > 0 else 0
            fy_summary[site] = tonnes
        
        fy_summary['total'] = sum(fy_summary.values())
        control_tower[fy] = fy_summary
    
    return control_tower

def benchmark_direct_vs_cached(scenario_version):
    """Benchmark direct polars queries vs current caching approach"""
    
    print("🔥 BENCHMARKING: Direct Polars vs Current Caching")
    print("=" * 60)
    
    # Test direct polars approach
    direct_start = time.time()
    direct_results = get_review_scenario_data_direct_polars(scenario_version)
    direct_time = time.time() - direct_start
    
    print(f"\n📊 BENCHMARK RESULTS:")
    print(f"Direct Polars Time: {direct_time:.2f} seconds")
    print(f"Current Cache Time: ~12+ minutes (720+ seconds)")
    print(f"Speed Improvement: {720/direct_time:.0f}x faster")
    print(f"Time Saved: {(720-direct_time)/60:.1f} minutes")
    
    return direct_results

if __name__ == "__main__":
    # Test with Jul 25 SPR scenario
    benchmark_direct_vs_cached("Jul 25 SPR")
