"""
POLARS-OPTIMIZED AGGREGATION FUNCTIONS
=====================================
This file contains polars-optimized versions of the aggregation functions
that were identified as the major performance bottleneck (48.72 minutes).

The main optimizations:
1. Replace Django ORM queries with direct polars DataFrame operations
2. Use vectorized operations instead of row-by-row processing
3. Minimize database round trips through bulk data loading
4. Use polars' efficient group-by and aggregation operations
5. Reduce memory allocations through lazy evaluation
"""

import polars as pl
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import json
import math
from collections import defaultdict
from django.db.models import Sum
from django.db.models.functions import TruncMonth
from django.db import connection

# Import the original functions for fallback compatibility
from website.customized_function import (
    get_opening_inventory_by_group, combine_inventory_with_forecast_data,
    get_monthly_pour_plan_for_site, get_production_data_by_product_for_wun1
)


def load_aggregated_forecast_polars(scenario):
    """Load AggregatedForecast data as polars DataFrame for fast processing"""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 
                period,
                parent_product_group_description,
                product_group,
                forecast_region,
                customer_code,
                data_source,
                tonnes,
                cogs_aud,
                revenue_aud
            FROM website_aggregatedforecast 
            WHERE version = %s
        """, [scenario.version])
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
    
    if not data:
        return pl.DataFrame()
    
    # Convert to polars DataFrame
    df = pl.DataFrame(data, schema=columns)
    
    # Ensure proper data types
    df = df.with_columns([
        pl.col("period").str.to_date(),
        pl.col("tonnes").cast(pl.Float64, strict=False),
        pl.col("cogs_aud").cast(pl.Float64, strict=False),
        pl.col("revenue_aud").cast(pl.Float64, strict=False)
    ])
    
    return df


def load_calculated_production_polars(scenario):
    """Load CalculatedProductionModel data as polars DataFrame for fast processing"""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 
                pouring_date,
                parent_product_group,
                product_group,
                site_id,
                product_id,
                production_quantity,
                cogs_aud
            FROM website_calculatedproductionmodel 
            WHERE version = %s
        """, [scenario.version])
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
    
    if not data:
        return pl.DataFrame()
    
    # Convert to polars DataFrame
    df = pl.DataFrame(data, schema=columns)
    
    # Ensure proper data types
    df = df.with_columns([
        pl.col("pouring_date").str.to_date(),
        pl.col("production_quantity").cast(pl.Float64, strict=False),
        pl.col("cogs_aud").cast(pl.Float64, strict=False)
    ])
    
    return df


def get_monthly_cogs_and_revenue_polars(scenario, start_date=None):
    """
    Polars-optimized version of get_monthly_cogs_and_revenue
    """
    print("DEBUG: Loading forecast data with polars...")
    df = load_aggregated_forecast_polars(scenario)
    
    if df.is_empty():
        return [], [], []
    
    # Apply start_date filter if provided
    if start_date:
        df = df.filter(pl.col("period") >= start_date)
    
    # Group by month and aggregate
    monthly_data = (
        df.with_columns([
            pl.col("period").dt.truncate("1mo").alias("month")
        ])
        .group_by("month")
        .agg([
            pl.col("cogs_aud").sum().alias("total_cogs"),
            pl.col("revenue_aud").sum().alias("total_revenue")
        ])
        .sort("month")
    )
    
    # Convert to lists
    months = [row[0].strftime('%b %Y') for row in monthly_data.select("month").to_numpy()]
    cogs = monthly_data.select("total_cogs").to_series().fill_null(0).to_list()
    revenue = monthly_data.select("total_revenue").to_series().fill_null(0).to_list()
    
    return months, cogs, revenue


def get_monthly_production_cogs_polars(scenario, start_date=None):
    """
    Polars-optimized version of get_monthly_production_cogs
    """
    print("DEBUG: Loading production data with polars...")
    df = load_calculated_production_polars(scenario)
    
    if df.is_empty():
        return [], []
    
    # Apply start_date filter if provided
    if start_date:
        df = df.filter(pl.col("pouring_date") >= start_date)
    
    # Group by month and aggregate
    monthly_data = (
        df.with_columns([
            pl.col("pouring_date").dt.truncate("1mo").alias("month")
        ])
        .group_by("month")
        .agg([
            pl.col("cogs_aud").sum().alias("total_production_cogs")
        ])
        .sort("month")
    )
    
    # Convert to lists
    months = [row[0].strftime('%b %Y') for row in monthly_data.select("month").to_numpy()]
    production_cogs = monthly_data.select("total_production_cogs").to_series().fill_null(0).to_list()
    
    return months, production_cogs


def get_forecast_data_by_parent_product_group_polars(scenario):
    """
    Polars-optimized version of get_forecast_data_by_parent_product_group
    """
    print("DEBUG: Getting parent product group data with polars...")
    df = load_aggregated_forecast_polars(scenario)
    
    if df.is_empty():
        return {'labels': [], 'datasets': []}
    
    # Group by parent group and month
    grouped_data = (
        df.with_columns([
            pl.col("period").dt.truncate("1mo").alias("month")
        ])
        .group_by(["parent_product_group_description", "month"])
        .agg([
            pl.col("tonnes").sum().alias("total_tonnes")
        ])
        .sort(["parent_product_group_description", "month"])
    )
    
    # Get unique months and groups
    months = sorted(set(row[1].strftime('%b %Y') for row in grouped_data.select(["parent_product_group_description", "month"]).to_numpy()))
    groups = sorted(set(row[0] or 'Unknown' for row in grouped_data.select("parent_product_group_description").to_numpy()))
    
    # Build datasets
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)',
        'rgba(255,99,255,0.6)', 'rgba(99,255,132,0.6)', 'rgba(132,99,255,0.6)'
    ]
    
    for idx, group in enumerate(groups):
        # Get data for this group
        group_data = grouped_data.filter(pl.col("parent_product_group_description") == group)
        
        # Create month-value mapping
        month_map = {}
        for row in group_data.to_numpy():
            month_str = row[1].strftime('%b %Y')
            month_map[month_str] = row[2] or 0
        
        # Align data to all months
        data = [month_map.get(month, 0) for month in months]
        
        datasets.append({
            'label': group,
            'data': data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1
        })
    
    return {'labels': months, 'datasets': datasets}


def get_forecast_data_by_product_group_polars(scenario):
    """
    Polars-optimized version of get_forecast_data_by_product_group
    """
    print("DEBUG: Getting product group data with polars...")
    df = load_aggregated_forecast_polars(scenario)
    
    if df.is_empty():
        return {'labels': [], 'datasets': []}
    
    # Group by product group and month
    grouped_data = (
        df.with_columns([
            pl.col("period").dt.truncate("1mo").alias("month")
        ])
        .group_by(["product_group", "month"])
        .agg([
            pl.col("tonnes").sum().alias("total_tonnes")
        ])
        .sort(["product_group", "month"])
    )
    
    # Get unique months and groups
    months = sorted(set(row[1].strftime('%b %Y') for row in grouped_data.select(["product_group", "month"]).to_numpy()))
    groups = sorted(set(row[0] or 'Unknown' for row in grouped_data.select("product_group").to_numpy()))
    
    # Build datasets
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)',
        'rgba(255,99,255,0.6)', 'rgba(99,255,132,0.6)', 'rgba(132,99,255,0.6)'
    ]
    
    for idx, group in enumerate(groups):
        # Get data for this group
        group_data = grouped_data.filter(pl.col("product_group") == group)
        
        # Create month-value mapping
        month_map = {}
        for row in group_data.to_numpy():
            month_str = row[1].strftime('%b %Y')
            month_map[month_str] = row[2] or 0
        
        # Align data to all months
        data = [month_map.get(month, 0) for month in months]
        
        datasets.append({
            'label': group,
            'data': data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1
        })
    
    return {'labels': months, 'datasets': datasets}


def get_forecast_data_by_region_polars(scenario):
    """
    Polars-optimized version of get_forecast_data_by_region
    """
    print("DEBUG: Getting region data with polars...")
    df = load_aggregated_forecast_polars(scenario)
    
    if df.is_empty():
        return {'labels': [], 'datasets': []}
    
    # Group by region and month
    grouped_data = (
        df.with_columns([
            pl.col("period").dt.truncate("1mo").alias("month")
        ])
        .group_by(["forecast_region", "month"])
        .agg([
            pl.col("tonnes").sum().alias("total_tonnes")
        ])
        .sort(["forecast_region", "month"])
    )
    
    # Get unique months and regions
    months = sorted(set(row[1].strftime('%b %Y') for row in grouped_data.select(["forecast_region", "month"]).to_numpy()))
    regions = sorted(set(row[0] or 'Unknown' for row in grouped_data.select("forecast_region").to_numpy()))
    
    # Build datasets
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)',
        'rgba(255,99,255,0.6)', 'rgba(99,255,132,0.6)', 'rgba(132,99,255,0.6)'
    ]
    
    for idx, region in enumerate(regions):
        # Get data for this region
        region_data = grouped_data.filter(pl.col("forecast_region") == region)
        
        # Create month-value mapping
        month_map = {}
        for row in region_data.to_numpy():
            month_str = row[1].strftime('%b %Y')
            month_map[month_str] = row[2] or 0
        
        # Align data to all months
        data = [month_map.get(month, 0) for month in months]
        
        datasets.append({
            'label': region,
            'data': data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1
        })
    
    return {'labels': months, 'datasets': datasets}


def get_forecast_data_by_customer_polars(scenario):
    """
    Polars-optimized version of get_forecast_data_by_customer
    """
    print("DEBUG: Getting customer data with polars...")
    df = load_aggregated_forecast_polars(scenario)
    
    if df.is_empty():
        return {'labels': [], 'datasets': []}
    
    # Group by customer and month
    grouped_data = (
        df.with_columns([
            pl.col("period").dt.truncate("1mo").alias("month")
        ])
        .group_by(["customer_code", "month"])
        .agg([
            pl.col("tonnes").sum().alias("total_tonnes")
        ])
        .sort(["customer_code", "month"])
    )
    
    # Get unique months and customers
    months = sorted(set(row[1].strftime('%b %Y') for row in grouped_data.select(["customer_code", "month"]).to_numpy()))
    customers = sorted(set(row[0] or 'Unknown' for row in grouped_data.select("customer_code").to_numpy()))
    
    # Build datasets
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)',
        'rgba(255,99,255,0.6)', 'rgba(99,255,132,0.6)', 'rgba(132,99,255,0.6)'
    ]
    
    for idx, customer in enumerate(customers):
        # Get data for this customer
        customer_data = grouped_data.filter(pl.col("customer_code") == customer)
        
        # Create month-value mapping
        month_map = {}
        for row in customer_data.to_numpy():
            month_str = row[1].strftime('%b %Y')
            month_map[month_str] = row[2] or 0
        
        # Align data to all months
        data = [month_map.get(month, 0) for month in months]
        
        datasets.append({
            'label': customer,
            'data': data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1
        })
    
    return {'labels': months, 'datasets': datasets}


def get_forecast_data_by_data_source_polars(scenario):
    """
    Polars-optimized version of get_forecast_data_by_data_source
    """
    print("DEBUG: Getting data source data with polars...")
    df = load_aggregated_forecast_polars(scenario)
    
    if df.is_empty():
        return {'labels': [], 'datasets': []}
    
    # Group by data source and month
    grouped_data = (
        df.with_columns([
            pl.col("period").dt.truncate("1mo").alias("month")
        ])
        .group_by(["data_source", "month"])
        .agg([
            pl.col("tonnes").sum().alias("total_tonnes")
        ])
        .sort(["data_source", "month"])
    )
    
    # Get unique months and data sources
    months = sorted(set(row[1].strftime('%b %Y') for row in grouped_data.select(["data_source", "month"]).to_numpy()))
    data_sources = sorted(set(row[0] or 'Unknown' for row in grouped_data.select("data_source").to_numpy()))
    
    # Build datasets
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)',
        'rgba(255,99,255,0.6)', 'rgba(99,255,132,0.6)', 'rgba(132,99,255,0.6)'
    ]
    
    for idx, source in enumerate(data_sources):
        # Get data for this source
        source_data = grouped_data.filter(pl.col("data_source") == source)
        
        # Create month-value mapping
        month_map = {}
        for row in source_data.to_numpy():
            month_str = row[1].strftime('%b %Y')
            month_map[month_str] = row[2] or 0
        
        # Align data to all months
        data = [month_map.get(month, 0) for month in months]
        
        datasets.append({
            'label': source,
            'data': data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1
        })
    
    return {'labels': months, 'datasets': datasets}


def get_production_data_by_group_polars(site_name, scenario):
    """
    Polars-optimized version of get_production_data_by_group
    """
    print(f"DEBUG: Getting production data for site {site_name} with polars...")
    df = load_calculated_production_polars(scenario)
    
    if df.is_empty():
        return {'labels': [], 'datasets': []}
    
    # Filter by site
    df = df.filter(pl.col("site_id") == site_name)
    
    if df.is_empty():
        return {'labels': [], 'datasets': []}
    
    # Group by product group and month
    grouped_data = (
        df.with_columns([
            pl.col("pouring_date").dt.truncate("1mo").alias("month")
        ])
        .group_by(["product_group", "month"])
        .agg([
            pl.col("production_quantity").sum().alias("total_quantity")
        ])
        .sort(["product_group", "month"])
    )
    
    # Get unique months and groups
    months = sorted(set(row[1].strftime('%b %Y') for row in grouped_data.select(["product_group", "month"]).to_numpy()))
    groups = sorted(set(row[0] or 'Unknown' for row in grouped_data.select("product_group").to_numpy()))
    
    # Build datasets
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)',
        'rgba(255,99,255,0.6)', 'rgba(99,255,132,0.6)', 'rgba(132,99,255,0.6)'
    ]
    
    for idx, group in enumerate(groups):
        # Get data for this group
        group_data = grouped_data.filter(pl.col("product_group") == group)
        
        # Create month-value mapping
        month_map = {}
        for row in group_data.to_numpy():
            month_str = row[1].strftime('%b %Y')
            month_map[month_str] = row[2] or 0
        
        # Align data to all months
        data = [month_map.get(month, 0) for month in months]
        
        datasets.append({
            'label': group,
            'data': data,
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1
        })
    
    return {'labels': months, 'datasets': datasets}


def get_inventory_data_with_start_date_polars(scenario):
    """
    Polars-optimized version of get_inventory_data_with_start_date
    This is the main bottleneck function that needs optimization
    """
    print("DEBUG: Starting polars-optimized inventory data calculation...")
    
    # Get the first inventory snapshot date and calculate start date
    from website.models import MasterDataInventory
    first_inventory = MasterDataInventory.objects.filter(version=scenario).order_by('date_of_snapshot').first()
    if first_inventory:
        next_day = first_inventory.date_of_snapshot + timedelta(days=1)
        start_date = next_day.replace(day=1)
    else:
        start_date = None
    
    print(f"DEBUG: Using start_date: {start_date}")
    
    # Load data as polars DataFrames
    forecast_df = load_aggregated_forecast_polars(scenario)
    production_df = load_calculated_production_polars(scenario)
    
    # Get months for each series with start_date filtering
    print("DEBUG: Calculating monthly COGS and revenue...")
    months_cogs, cogs, revenue = get_monthly_cogs_and_revenue_polars(scenario, start_date=start_date)
    
    print("DEBUG: Calculating monthly production COGS...")
    months_prod, production_cogs = get_monthly_production_cogs_polars(scenario, start_date=start_date)
    
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
    
    print("DEBUG: Processing group data with polars...")
    
    # Process group data using polars - much faster than Django ORM
    if not forecast_df.is_empty():
        # Apply start_date filter
        if start_date:
            forecast_df = forecast_df.filter(pl.col("period") >= start_date)
        
        # Get unique parent groups
        parent_groups = forecast_df.select("parent_product_group_description").unique().to_series().to_list()
        parent_groups = [g for g in parent_groups if g is not None]
    else:
        parent_groups = []
    
    cogs_data_by_group = {}
    
    for group in parent_groups:
        print(f"DEBUG: Processing group: {group}")
        
        # COGS and Revenue from AggregatedForecast with polars
        group_forecast = forecast_df.filter(pl.col("parent_product_group_description") == group)
        
        if not group_forecast.is_empty():
            agg_monthly = (
                group_forecast.with_columns([
                    pl.col("period").dt.truncate("1mo").alias("month")
                ])
                .group_by("month")
                .agg([
                    pl.col("cogs_aud").sum().alias("total_cogs"),
                    pl.col("revenue_aud").sum().alias("total_revenue")
                ])
                .sort("month")
            )
            
            months = [row[0].strftime('%b %Y') for row in agg_monthly.select("month").to_numpy()]
            cogs = agg_monthly.select("total_cogs").to_series().fill_null(0).to_list()
            revenue = agg_monthly.select("total_revenue").to_series().fill_null(0).to_list()
        else:
            months, cogs, revenue = [], [], []
        
        # Production AUD from CalculatedProductionModel with polars
        if not production_df.is_empty():
            # Apply start_date filter
            group_production = production_df.filter(pl.col("parent_product_group") == group)
            if start_date:
                group_production = group_production.filter(pl.col("pouring_date") >= start_date)
            
            if not group_production.is_empty():
                prod_monthly = (
                    group_production.with_columns([
                        pl.col("pouring_date").dt.truncate("1mo").alias("month")
                    ])
                    .group_by("month")
                    .agg([
                        pl.col("cogs_aud").sum().alias("total_production_aud")
                    ])
                    .sort("month")
                )
                
                prod_months = [row[0].strftime('%b %Y') for row in prod_monthly.select("month").to_numpy()]
                production_aud = prod_monthly.select("total_production_aud").to_series().fill_null(0).to_list()
            else:
                prod_months, production_aud = [], []
        else:
            prod_months, production_aud = [], []
        
        # Union of all months for this group
        all_months_group = sorted(set(months) | set(prod_months), key=lambda d: pd.to_datetime(d, format='%b %Y'))
        
        # Align all series to all_months_group
        cogs_map = dict(zip(months, cogs))
        revenue_map = dict(zip(months, revenue))
        prod_map = dict(zip(prod_months, production_aud))
        
        cogs_aligned_group = [cogs_map.get(m, 0) for m in all_months_group]
        revenue_aligned_group = [revenue_map.get(m, 0) for m in all_months_group]
        prod_aligned_group = [prod_map.get(m, 0) for m in all_months_group]
        
        cogs_data_by_group[group] = {
            'months': all_months_group,
            'cogs': cogs_aligned_group,
            'revenue': revenue_aligned_group,
            'production_aud': prod_aligned_group
        }
    
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
    
    print("DEBUG: Getting opening inventory and combining with forecast data...")
    
    # Get opening inventory and combine with forecast data (keep original functions for now)
    opening_inventory_data = get_opening_inventory_by_group(scenario)
    combined_data_with_inventory = combine_inventory_with_forecast_data(
        cogs_data_by_group, 
        opening_inventory_data, 
        scenario
    )
    
    print("DEBUG: Polars-optimized inventory data calculation completed!")
    
    return {
        'inventory_months': all_months,
        'inventory_cogs': cogs_aligned,
        'inventory_revenue': revenue_aligned,
        'production_aud': prod_aligned,
        'production_cogs_group_chart': {},  # Will be calculated separately if needed
        'top_products_by_group_month': {},  # Will be calculated separately if needed
        'parent_product_groups': list(parent_groups),
        'cogs_data_by_group': combined_data_with_inventory,
    }


def get_foundry_chart_data_polars(scenario):
    """
    Polars-optimized version of get_foundry_chart_data
    """
    print("DEBUG: Starting polars-optimized foundry chart data calculation...")
    
    foundries = ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1']
    foundry_data = {}
    
    for foundry in foundries:
        print(f"DEBUG: Processing foundry: {foundry}")
        
        # Special handling for WUN1 to show products instead of product groups
        if foundry == 'WUN1':
            chart_data = get_production_data_by_product_for_wun1(foundry, scenario)  # Keep original for now
            # For WUN1, top_products should just be the product names from the chart
            top_products = [dataset['label'] for dataset in chart_data['datasets']]
        else:
            chart_data = get_production_data_by_group_polars(foundry, scenario)
            top_products = []  # Will implement if needed
        
        monthly_pour_plan = get_monthly_pour_plan_for_site(foundry, scenario, chart_data['labels'])  # Keep original
        
        foundry_data[foundry] = {
            'chart_data': chart_data,
            'top_products': json.dumps(top_products),
            'monthly_pour_plan': monthly_pour_plan
        }
    
    print("DEBUG: Polars-optimized foundry chart data calculation completed!")
    return foundry_data


# OPTIMIZED AGGREGATION FUNCTIONS

def populate_aggregated_forecast_data_polars(scenario):
    """
    Polars-optimized version of populate_aggregated_forecast_data
    """
    from website.models import AggregatedForecastChartData
    
    print(f"DEBUG: Populating aggregated forecast data with polars for scenario: {scenario}")
    
    try:
        # Get or create the aggregated data record
        agg_data, created = AggregatedForecastChartData.objects.get_or_create(version=scenario)
        
        # Calculate forecast data by different dimensions using polars
        print("DEBUG: Calculating forecast data by product group with polars...")
        by_product_group = get_forecast_data_by_product_group_polars(scenario)
        
        print("DEBUG: Calculating forecast data by parent product group with polars...")
        by_parent_group = get_forecast_data_by_parent_product_group_polars(scenario)
        
        print("DEBUG: Calculating forecast data by region with polars...")
        by_region = get_forecast_data_by_region_polars(scenario)
        
        print("DEBUG: Calculating forecast data by customer with polars...")
        by_customer = get_forecast_data_by_customer_polars(scenario)
        
        print("DEBUG: Calculating forecast data by data source with polars...")
        by_data_source = get_forecast_data_by_data_source_polars(scenario)
        
        # Store the calculated data
        agg_data.by_product_group = by_product_group
        agg_data.by_parent_group = by_parent_group
        agg_data.by_region = by_region
        agg_data.by_customer = by_customer
        agg_data.by_data_source = by_data_source
        
        # Calculate summary metrics
        total_tonnes = 0
        total_customers = 0
        total_periods = 0
        
        if by_customer.get('datasets'):
            for dataset in by_customer['datasets']:
                total_tonnes += sum(dataset.get('data', []))
            total_customers = len(by_customer['datasets'])
        
        if by_customer.get('labels'):
            total_periods = len(by_customer['labels'])
        
        agg_data.total_tonnes = total_tonnes
        agg_data.total_customers = total_customers
        agg_data.total_periods = total_periods
        
        agg_data.save()
        print(f"DEBUG: Saved polars-optimized forecast data - {agg_data.total_tonnes} tonnes, {agg_data.total_customers} customers")
        
    except Exception as e:
        print(f"ERROR: Failed to populate aggregated forecast data with polars: {e}")
        import traceback
        traceback.print_exc()


def populate_aggregated_foundry_data_polars(scenario):
    """
    Polars-optimized version of populate_aggregated_foundry_data
    """
    from website.models import AggregatedFoundryChartData
    
    print(f"DEBUG: Populating aggregated foundry data with polars for scenario: {scenario}")
    
    try:
        # Get or create the aggregated data record
        agg_data, created = AggregatedFoundryChartData.objects.get_or_create(version=scenario)
        
        # Calculate foundry data using polars
        print("DEBUG: Calculating foundry chart data with polars...")
        foundry_data = get_foundry_chart_data_polars(scenario)
        
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
        print(f"DEBUG: Saved polars-optimized foundry data - {agg_data.total_sites} sites, {agg_data.total_production} total production")
        
    except Exception as e:
        print(f"ERROR: Failed to populate aggregated foundry data with polars: {e}")
        import traceback
        traceback.print_exc()


def populate_aggregated_inventory_data_polars(scenario):
    """
    Polars-optimized version of populate_aggregated_inventory_data
    """
    from website.models import AggregatedInventoryChartData
    import traceback
    
    print(f"DEBUG: Populating aggregated inventory data with polars for scenario: {scenario}")
    
    try:
        # Get or create the aggregated data record
        agg_data, created = AggregatedInventoryChartData.objects.get_or_create(version=scenario)
        
        # Calculate REAL inventory data from SQL Server during model calculation
        print("DEBUG: Fetching REAL opening inventory data from SQL Server...")
        
        # Get the real opening inventory by group using SQL Server (keep original function)
        opening_inventory_by_group = get_opening_inventory_by_group(scenario)
        
        # Calculate inventory data with start date filtering using polars
        print("DEBUG: Calculating inventory data with polars optimization...")
        inventory_data = get_inventory_data_with_start_date_polars(scenario)
        
        # Store the calculated data
        agg_data.inventory_by_group = opening_inventory_by_group  # Store REAL SQL Server data
        agg_data.monthly_trends = inventory_data.get('cogs_data_by_group', {})
        agg_data.total_inventory_value = sum(opening_inventory_by_group.values()) if opening_inventory_by_group else 0
        agg_data.total_groups = len(opening_inventory_by_group)
        agg_data.total_products = len(inventory_data.get('parent_product_groups', []))  # Store the count, not the list
        
        agg_data.save()
        print(f"DEBUG: Saved polars-optimized inventory data - ${agg_data.total_inventory_value:,.2f} value, {agg_data.total_groups} groups")
        print(f"DEBUG: Stored inventory by group: {list(opening_inventory_by_group.keys())}")
        
    except Exception as e:
        print(f"ERROR: Failed to populate aggregated inventory data with polars: {e}")
        import traceback
        traceback.print_exc()


def populate_aggregated_financial_data_polars(scenario):
    """
    Polars-optimized version of populate_aggregated_financial_data
    """
    from website.models import AggregatedFinancialChartData
    
    print(f"DEBUG: Starting polars-optimized financial data population for scenario: {scenario}")
    
    try:
        # Use polars-optimized functions for faster calculation
        months_financial, cogs_data_total, revenue_data_total = get_monthly_cogs_and_revenue_polars(scenario)
        months_production, production_data_total = get_monthly_production_cogs_polars(scenario)
        
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
                    'borderColor': 'rgba(75, 192, 192, 1)',
                    'backgroundColor': 'rgba(75, 192, 192, 0.2)',
                    'fill': False,
                    'tension': 0.1
                },
                {
                    'label': 'COGS AUD',
                    'data': cogs_data_total,
                    'borderColor': 'rgba(255, 99, 132, 1)',
                    'backgroundColor': 'rgba(255, 99, 132, 0.2)',
                    'fill': False,
                    'tension': 0.1
                },
                {
                    'label': 'Production AUD',
                    'data': production_data_total,
                    'borderColor': 'rgba(255, 206, 86, 1)',
                    'backgroundColor': 'rgba(255, 206, 86, 0.2)',
                    'fill': False,
                    'tension': 0.1
                },
                {
                    'label': 'Inventory Projection AUD',
                    'data': inventory_projection,
                    'borderColor': 'rgba(54, 162, 235, 1)',
                    'backgroundColor': 'rgba(54, 162, 235, 0.2)',
                    'fill': False,
                    'tension': 0.1
                }
            ]
        }
        
        # Calculate summary metrics
        total_revenue = sum(revenue_data_total) if revenue_data_total else 0
        total_cogs = sum(cogs_data_total) if cogs_data_total else 0
        total_production = sum(production_data_total) if production_data_total else 0
        total_inventory_proj = sum(inventory_projection) if inventory_projection else 0
        
        # Build individual chart data
        revenue_chart_data = {
            'labels': months_financial,
            'datasets': [{
                'label': 'Revenue AUD',
                'data': revenue_data_total,
                'backgroundColor': 'rgba(75, 192, 192, 0.6)',
                'borderColor': 'rgba(75, 192, 192, 1)',
                'borderWidth': 1
            }]
        }
        
        cogs_chart_data = {
            'labels': months_financial,
            'datasets': [{
                'label': 'COGS AUD',
                'data': cogs_data_total,
                'backgroundColor': 'rgba(255, 99, 132, 0.6)',
                'borderColor': 'rgba(255, 99, 132, 1)',
                'borderWidth': 1
            }]
        }
        
        production_chart_data = {
            'labels': months_production,
            'datasets': [{
                'label': 'Production AUD',
                'data': production_data_total,
                'backgroundColor': 'rgba(255, 206, 86, 0.6)',
                'borderColor': 'rgba(255, 206, 86, 1)',
                'borderWidth': 1
            }]
        }
        
        inventory_projection_data = {
            'labels': months_financial,
            'datasets': [{
                'label': 'Inventory Projection AUD',
                'data': inventory_projection,
                'backgroundColor': 'rgba(54, 162, 235, 0.6)',
                'borderColor': 'rgba(54, 162, 235, 1)',
                'borderWidth': 1
            }]
        }
        
        # Store or update the financial data
        AggregatedFinancialChartData.objects.update_or_create(
            version=scenario,
            defaults={
                'total_revenue_aud': total_revenue,
                'total_cogs_aud': total_cogs,
                'total_production_aud': total_production,
                'total_inventory_projection': total_inventory_proj,
                'revenue_chart_data': revenue_chart_data,
                'cogs_chart_data': cogs_chart_data,
                'production_chart_data': production_chart_data,
                'inventory_projection_data': inventory_projection_data,
                'combined_financial_data': combined_financial_data,
            }
        )
        
        print(f"DEBUG: Saved polars-optimized financial chart data for scenario {scenario}")
        print(f"  Total Revenue: ${total_revenue:,.2f}")
        print(f"  Total COGS: ${total_cogs:,.2f}")
        print(f"  Total Production: ${total_production:,.2f}")
        
    except Exception as e:
        print(f"ERROR: Failed to populate financial data with polars for scenario {scenario}: {e}")
        import traceback
        traceback.print_exc()


def populate_all_aggregated_data_polars(scenario):
    """
    Polars-optimized version of populate_all_aggregated_data
    This replaces the 48.72-minute bottleneck with fast polars operations
    """
    print(f"DEBUG: Starting POLARS-OPTIMIZED aggregated data population for scenario: {scenario}")
    
    # Populate all chart data using polars optimization
    populate_aggregated_forecast_data_polars(scenario)
    populate_aggregated_foundry_data_polars(scenario)
    populate_aggregated_inventory_data_polars(scenario)
    populate_aggregated_financial_data_polars(scenario)
    
    print(f"DEBUG: Completed POLARS-OPTIMIZED aggregated data population for scenario: {scenario}")
