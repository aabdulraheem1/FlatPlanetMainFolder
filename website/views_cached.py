from django.shortcuts import get_object_or_404, render
from django.contrib.auth.decorators import login_required
from django.utils.safestring import mark_safe
from website.models import (
    scenarios,
    MasterDataInventory,
    CachedControlTowerData,
    CachedFoundryData,
    CachedForecastData,
    CachedInventoryData,
    CachedSupplierData,
    CachedDetailedInventoryData
)
import json

@login_required
def review_scenario_cached(request, version):
    """
    Optimized review scenario view using cached data.
    Falls back to real-time calculation if cache doesn't exist.
    """
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)

    # Get snapshot date
    snapshot_date = None
    try:
        inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
        if inventory_snapshot:
            snapshot_date = inventory_snapshot.date_of_snapshot.strftime('%B %d, %Y')
    except:
        snapshot_date = "Date not available"

    # Load cached data or fall back to real-time calculation
    control_tower_data = get_cached_control_tower_data(scenario)
    foundry_data = get_cached_foundry_data(scenario)
    forecast_data = get_cached_forecast_data(scenario)
    inventory_data = get_cached_inventory_data(scenario)
    supplier_data = get_cached_supplier_data(scenario)
    detailed_inventory_data = get_cached_detailed_inventory_data(scenario)

    context = {
        'version': scenario.version,
        'user_name': user_name,
        
        # Control Tower data
        'demand_plan': control_tower_data.get('combined_demand_plan', {}),
        'poured_data': control_tower_data.get('poured_data', {}),
        'pour_plan': control_tower_data.get('pour_plan', {}),
        
        # Foundry data
        'mt_joli_chart_data': foundry_data.get('MTJ1', {}).get('chart_data', {}),
        'mt_joli_top_products_json': foundry_data.get('MTJ1', {}).get('top_products', []),
        'mt_joli_monthly_pour_plan': foundry_data.get('MTJ1', {}).get('monthly_pour_plan', {}),
        'coimbatore_chart_data': foundry_data.get('COI2', {}).get('chart_data', {}),
        'coimbatore_top_products_json': foundry_data.get('COI2', {}).get('top_products', []),
        'coimbatore_monthly_pour_plan': foundry_data.get('COI2', {}).get('monthly_pour_plan', {}),
        'xuzhou_chart_data': foundry_data.get('XUZ1', {}).get('chart_data', {}),
        'xuzhou_top_products_json': foundry_data.get('XUZ1', {}).get('top_products', []),
        'xuzhou_monthly_pour_plan': foundry_data.get('XUZ1', {}).get('monthly_pour_plan', {}),
        'merlimau_chart_data': foundry_data.get('MER1', {}).get('chart_data', {}),
        'merlimau_top_products_json': foundry_data.get('MER1', {}).get('top_products', []),
        'merlimau_monthly_pour_plan': foundry_data.get('MER1', {}).get('monthly_pour_plan', {}),
        'wod1_chart_data': foundry_data.get('WOD1', {}).get('chart_data', {}),
        'wod1_top_products_json': foundry_data.get('WOD1', {}).get('top_products', []),
        'wod1_monthly_pour_plan': foundry_data.get('WOD1', {}).get('monthly_pour_plan', {}),
        'wun1_chart_data': foundry_data.get('WUN1', {}).get('chart_data', {}),
        'wun1_top_products_json': foundry_data.get('WUN1', {}).get('top_products', []),
        'wun1_monthly_pour_plan': foundry_data.get('WUN1', {}).get('monthly_pour_plan', {}),
        
        # Forecast data
        'chart_data_parent_product_group': json.dumps(forecast_data.get('parent_product_group', {})),
        'chart_data_product_group': json.dumps(forecast_data.get('product_group', {})),
        'chart_data_region': json.dumps(forecast_data.get('region', {})),
        'chart_data_customer': json.dumps(forecast_data.get('customer', {})),
        'chart_data_data_source': json.dumps(forecast_data.get('data_source', {})),
        
        # Supplier data
        'supplier_a_chart_data': supplier_data.get('chart_data', {}),
        'supplier_a_top_products_json': json.dumps(supplier_data.get('top_products', [])),
        
        # Inventory data
        'inventory_months': inventory_data.get('inventory_months', []),
        'inventory_cogs': inventory_data.get('inventory_cogs', []),
        'inventory_revenue': inventory_data.get('inventory_revenue', []),
        'production_aud': inventory_data.get('production_aud', []),
        'production_cogs_group_chart': json.dumps(inventory_data.get('production_cogs_group_chart', {})),
        'top_products_by_group_month': json.dumps(inventory_data.get('top_products_by_group_month', {})),
        'parent_product_groups': inventory_data.get('parent_product_groups', []),
        'cogs_data_by_group': json.dumps(inventory_data.get('cogs_data_by_group', {})),
        'detailed_inventory_data': detailed_inventory_data.get('inventory_data', []),
        'detailed_production_data': detailed_inventory_data.get('production_data', []),
        'snapshot_date': snapshot_date,
    }
    
    return render(request, 'website/review_scenario.html', context)


def get_cached_control_tower_data(scenario):
    """Get cached control tower data or fall back to real-time calculation"""
    try:
        cached = CachedControlTowerData.objects.get(version=scenario)
        return {
            'combined_demand_plan': cached.combined_demand_plan,
            'poured_data': cached.poured_data,
            'pour_plan': cached.pour_plan,
        }
    except CachedControlTowerData.DoesNotExist:
        # Fall back to real-time calculation
        from website.customized_function import calculate_control_tower_data
        return calculate_control_tower_data(scenario)


def get_cached_foundry_data(scenario):
    """Get cached foundry data or fall back to real-time calculation"""
    try:
        cached_foundries = CachedFoundryData.objects.filter(version=scenario)
        if cached_foundries.exists():
            foundry_data = {}
            for cached in cached_foundries:
                foundry_data[cached.foundry_site] = {
                    'chart_data': cached.chart_data,
                    'top_products': cached.top_products,
                    'monthly_pour_plan': cached.monthly_pour_plan,
                }
            return foundry_data
        else:
            raise CachedFoundryData.DoesNotExist()
    except CachedFoundryData.DoesNotExist:
        # Fall back to real-time calculation
        from website.customized_function import get_foundry_chart_data
        foundry_data = get_foundry_chart_data(scenario)
        # Convert top_products from JSON string to object if needed
        for site, data in foundry_data.items():
            if isinstance(data['top_products'], str):
                data['top_products'] = json.loads(data['top_products'])
        return foundry_data


def get_cached_forecast_data(scenario):
    """Get cached forecast data or fall back to real-time calculation"""
    try:
        cached_forecasts = CachedForecastData.objects.filter(version=scenario)
        if cached_forecasts.exists():
            forecast_data = {}
            for cached in cached_forecasts:
                forecast_data[cached.data_type] = cached.chart_data
            return forecast_data
        else:
            raise CachedForecastData.DoesNotExist()
    except CachedForecastData.DoesNotExist:
        # Fall back to real-time calculation
        from website.customized_function import (
            get_forecast_data_by_parent_product_group,
            get_forecast_data_by_product_group,
            get_forecast_data_by_region,
            get_forecast_data_by_customer,
            get_forecast_data_by_data_source
        )
        return {
            'parent_product_group': get_forecast_data_by_parent_product_group(scenario),
            'product_group': get_forecast_data_by_product_group(scenario),
            'region': get_forecast_data_by_region(scenario),
            'customer': get_forecast_data_by_customer(scenario),
            'data_source': get_forecast_data_by_data_source(scenario),
        }


def get_cached_inventory_data(scenario):
    """Get cached inventory data or fall back to real-time calculation"""
    try:
        cached = CachedInventoryData.objects.get(version=scenario)
        return {
            'inventory_months': cached.inventory_months,
            'inventory_cogs': cached.inventory_cogs,
            'inventory_revenue': cached.inventory_revenue,
            'production_aud': cached.production_aud,
            'production_cogs_group_chart': cached.production_cogs_group_chart,
            'top_products_by_group_month': cached.top_products_by_group_month,
            'parent_product_groups': cached.parent_product_groups,
            'cogs_data_by_group': cached.cogs_data_by_group,
        }
    except CachedInventoryData.DoesNotExist:
        # Fall back to real-time calculation
        from website.customized_function import get_inventory_data_with_start_date
        return get_inventory_data_with_start_date(scenario)


def get_cached_supplier_data(scenario):
    """Get cached supplier data or fall back to real-time calculation"""
    try:
        # Currently only HBZJBF02 supplier is used
        cached = CachedSupplierData.objects.get(version=scenario, supplier_code='HBZJBF02')
        return {
            'chart_data': cached.chart_data,
            'top_products': cached.top_products,
        }
    except CachedSupplierData.DoesNotExist:
        # Fall back to real-time calculation
        from website.customized_function import get_production_data_by_group, get_top_products_per_month_by_group
        return {
            'chart_data': get_production_data_by_group('HBZJBF02', scenario),
            'top_products': get_top_products_per_month_by_group('HBZJBF02', scenario),
        }


def get_cached_detailed_inventory_data(scenario):
    """Get cached detailed inventory data or fall back to real-time calculation"""
    try:
        cached = CachedDetailedInventoryData.objects.get(version=scenario)
        return {
            'inventory_data': cached.inventory_data,
            'production_data': cached.production_data,
        }
    except CachedDetailedInventoryData.DoesNotExist:
        # Fall back to real-time calculation (returns empty by default)
        from website.customized_function import detailed_view_scenario_inventory
        return detailed_view_scenario_inventory(scenario)
