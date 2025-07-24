# customized_function_optimized.py - Database optimizations

from django.core.cache import cache
from django.db import connection

def calculate_control_tower_data_optimized(scenario_version):
    """Optimized control tower calculation with caching and efficient queries"""
    cache_key = f"control_tower_data_{scenario_version.pk}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        print(f"DEBUG: Using cached control tower data for scenario {scenario_version}")
        return cached_data
    
    print(f"DEBUG: Calculating control tower data for scenario {scenario_version}")
    
    # Use raw SQL for better performance
    with connection.cursor() as cursor:
        # Single query to get all demand plan data
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN pouring_date >= '2025-04-01' AND pouring_date <= '2026-03-31' THEN 'FY25'
                    WHEN pouring_date >= '2026-04-01' AND pouring_date <= '2027-03-31' THEN 'FY26'
                    WHEN pouring_date >= '2027-04-01' AND pouring_date <= '2028-03-31' THEN 'FY27'
                END as fiscal_year,
                s.SiteName,
                SUM(cp.tonnes) as total_tonnes
            FROM website_calculatedproductionmodel cp
            INNER JOIN website_masterdataplantmodel s ON cp.site_id = s.id
            WHERE cp.version_id = %s
                AND pouring_date >= '2025-04-01' 
                AND pouring_date <= '2028-03-31'
            GROUP BY fiscal_year, s.SiteName
            ORDER BY fiscal_year, s.SiteName
        """, [scenario_version.pk])
        
        demand_results = cursor.fetchall()
        
        # Single query to get all pour plan data
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN Month >= '2025-04-01' AND Month <= '2026-03-31' THEN 'FY25'
                    WHEN Month >= '2026-04-01' AND Month <= '2027-03-31' THEN 'FY26'
                    WHEN Month >= '2027-04-01' AND Month <= '2028-03-31' THEN 'FY27'
                END as fiscal_year,
                f.SiteName,
                SUM(PlanDressMass) as total_plan
            FROM website_masterdataplan mdp
            INNER JOIN website_masterdataplantmodel f ON mdp.Foundry_id = f.id
            WHERE mdp.version_id = %s
                AND Month >= '2025-04-01' 
                AND Month <= '2028-03-31'
            GROUP BY fiscal_year, f.SiteName
            ORDER BY fiscal_year, f.SiteName
        """, [scenario_version.pk])
        
        pour_results = cursor.fetchall()
    
    # Process results into data structures
    sites = ["MTJ1", "COI2", "XUZ1", "MER1", "WUN1", "WOD1", "CHI1"]
    
    demand_plan = {}
    pour_plan = {}
    
    for fy in ["FY25", "FY26", "FY27"]:
        demand_plan[fy] = {site: 0 for site in sites}
        pour_plan[fy] = {site: 0 for site in sites}
    
    # Fill demand plan data
    for row in demand_results:
        fiscal_year, site_name, total_tonnes = row
        if fiscal_year and site_name in sites:
            demand_plan[fiscal_year][site_name] = round(total_tonnes or 0)
    
    # Fill pour plan data
    for row in pour_results:
        fiscal_year, site_name, total_plan = row
        if fiscal_year and site_name in sites:
            pour_plan[fiscal_year][site_name] = round(total_plan or 0)
    
    # Get poured data (cached separately since it's from external DB)
    poured_data = get_poured_data_cached(scenario_version)
    
    # Combine demand with poured data
    combined_demand_plan = {}
    for fy in ["FY25", "FY26", "FY27"]:
        combined_demand_plan[fy] = {}
        for site in sites:
            demand_tonnes = demand_plan.get(fy, {}).get(site, 0)
            poured_tonnes = poured_data.get(fy, {}).get(site, 0)
            combined_demand_plan[fy][site] = round(demand_tonnes + poured_tonnes)
    
    result = {
        'combined_demand_plan': combined_demand_plan,
        'poured_data': poured_data,
        'pour_plan': pour_plan,
    }
    
    # Cache for 30 minutes
    cache.set(cache_key, result, 1800)
    
    print(f"DEBUG: Control tower data calculated and cached")
    return result

def get_poured_data_cached(scenario_version):
    """Get poured data with caching"""
    cache_key = f"poured_data_{scenario_version.pk}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        return cached_data
    
    # Only calculate if not cached
    poured_data = get_poured_data_by_fy_and_site(scenario_version)
    
    # Cache for 1 hour since external DB data changes less frequently
    cache.set(cache_key, poured_data, 3600)
    
    return poured_data

def get_foundry_chart_data_optimized(scenario_version):
    """Optimized foundry chart data with batch processing"""
    cache_key = f"foundry_data_{scenario_version.pk}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        return cached_data
    
    foundries = ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1']
    foundry_data = {}
    
    # Batch query for all foundries
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 
                s.SiteName,
                DATE_TRUNC('month', cp.pouring_date) as month,
                COALESCE(p.ProductGroup, 'Unknown') as product_group,
                p.Product,
                SUM(cp.tonnes) as total_tonnes
            FROM website_calculatedproductionmodel cp
            INNER JOIN website_masterdataplantmodel s ON cp.site_id = s.id
            INNER JOIN website_masterdataproductmodel p ON cp.product_id = p.id
            WHERE cp.version_id = %s 
                AND s.SiteName = ANY(%s)
                AND cp.tonnes > 0
            GROUP BY s.SiteName, month, product_group, p.Product
            ORDER BY s.SiteName, month, product_group, total_tonnes DESC
        """, [scenario_version.pk, foundries])
        
        results = cursor.fetchall()
    
    # Process results by foundry
    for foundry in foundries:
        foundry_results = [r for r in results if r[0] == foundry]
        
        if foundry == 'WUN1':
            chart_data = process_wun1_product_data(foundry_results)
            top_products = [dataset['label'] for dataset in chart_data['datasets']]
        else:
            chart_data = process_foundry_group_data(foundry_results)
            top_products = get_top_products_from_results(foundry_results)
        
        monthly_pour_plan = get_monthly_pour_plan_for_site(foundry, scenario_version, chart_data['labels'])
        
        foundry_data[foundry] = {
            'chart_data': chart_data,
            'top_products': json.dumps(top_products),
            'monthly_pour_plan': monthly_pour_plan
        }
    
    # Cache for 30 minutes
    cache.set(cache_key, foundry_data, 1800)
    
    return foundry_data

def process_foundry_group_data(results):
    """Process foundry results into chart format"""
    data = {}
    labels_set = set()
    
    for site_name, month, product_group, product, total_tonnes in results:
        month_str = month.strftime('%Y-%m')
        labels_set.add(month_str)
        
        if product_group not in data:
            data[product_group] = {}
        if month_str not in data[product_group]:
            data[product_group][month_str] = 0
        data[product_group][month_str] += total_tonnes
    
    labels = sorted(labels_set)
    
    # Convert to chart.js format
    datasets = []
    colors = [
        'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
        'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)'
    ]
    
    for idx, (group, month_dict) in enumerate(data.items()):
        datasets.append({
            'label': group,
            'data': [month_dict.get(label, 0) for label in labels],
            'backgroundColor': colors[idx % len(colors)],
            'borderColor': colors[idx % len(colors)],
            'borderWidth': 1,
            'stack': 'tonnes'
        })
    
    return {'labels': labels, 'datasets': datasets}

# Inventory data optimization
def get_inventory_data_optimized(scenario_version):
    """Optimized inventory data loading"""
    cache_key = f"inventory_data_{scenario_version.pk}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        return cached_data
    
    # Use select_related and prefetch_related to reduce queries
    inventory_data = get_inventory_data_with_start_date_optimized(scenario_version)
    
    # Cache for 20 minutes
    cache.set(cache_key, inventory_data, 1200)
    
    return inventory_data
