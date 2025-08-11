from django.core.management.base import BaseCommand
from django.db import transaction
from website.models import (
    scenarios, 
    CachedControlTowerData,
    CachedFoundryData, 
    CachedForecastData,
    CachedInventoryData,
    CachedSupplierData,
    CachedDetailedInventoryData,
    MasterDataInventory
)
from website.customized_function import (
    calculate_control_tower_data,
    get_foundry_chart_data,
    get_forecast_data_by_parent_product_group,
    get_forecast_data_by_product_group,
    get_forecast_data_by_region,
    get_forecast_data_by_customer,
    get_forecast_data_by_data_source,
    get_inventory_data_with_start_date,
    get_production_data_by_group,
    get_top_products_per_month_by_group,
    detailed_view_scenario_inventory
)
import json
import time


class Command(BaseCommand):
    help = 'Pre-compute and cache expensive review scenario calculations'

    def add_arguments(self, parser):
        parser.add_argument(
            '--scenario',
            type=str,
            help='Specific scenario version to cache (if not provided, caches all scenarios)',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force recalculation even if cache exists',
        )

    def handle(self, *args, **options):
        version_filter = options.get('scenario')
        force_recalc = options.get('force', False)
        
        if version_filter:
            try:
                scenario_list = [scenarios.objects.get(version=version_filter)]
                self.stdout.write(f"Caching data for scenario: {version_filter}")
            except scenarios.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Scenario {version_filter} not found"))
                return
        else:
            scenario_list = scenarios.objects.all()
            self.stdout.write(f"Caching data for {scenario_list.count()} scenarios")

        for scenario in scenario_list:
            self.cache_scenario_data(scenario, force_recalc)

    def cache_scenario_data(self, scenario, force_recalc):
        """Cache all review scenario data for a given scenario"""
        self.stdout.write(f"\n=== Caching data for scenario: {scenario.version} ===")
        
        try:
            with transaction.atomic():
                # 1. Cache Control Tower Data
                self.cache_control_tower_data(scenario, force_recalc)
                
                # 2. Cache Foundry Data
                self.cache_foundry_data(scenario, force_recalc)
                
                # 3. Cache Forecast Data
                self.cache_forecast_data(scenario, force_recalc)
                
                # 4. Cache Inventory Data
                self.cache_inventory_data(scenario, force_recalc)
                
                # 5. Cache Supplier Data
                self.cache_supplier_data(scenario, force_recalc)
                
                # 6. Cache Detailed Inventory Data (empty by default)
                self.cache_detailed_inventory_data(scenario, force_recalc)
                
            self.stdout.write(self.style.SUCCESS(f"[OK] Successfully cached all data for {scenario.version}"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"[ERROR] Failed to cache data for {scenario.version}: {e}"))

    def cache_control_tower_data(self, scenario, force_recalc):
        """Cache control tower calculations"""
        if not force_recalc and CachedControlTowerData.objects.filter(version=scenario).exists():
            self.stdout.write("[SKIP] Control Tower data already cached")
            return
        
        self.stdout.write("[INFO] Computing Control Tower data...")
        start_time = time.time()
        
        control_tower_data = calculate_control_tower_data(scenario)
        
        CachedControlTowerData.objects.update_or_create(
            version=scenario,
            defaults={
                'combined_demand_plan': control_tower_data['combined_demand_plan'],
                'poured_data': control_tower_data['poured_data'],
                'pour_plan': control_tower_data['pour_plan'],
            }
        )
        
        elapsed = time.time() - start_time
        self.stdout.write(f"[OK] Control Tower data cached ({elapsed:.2f}s)")

    def cache_foundry_data(self, scenario, force_recalc):
        """Cache foundry data for all sites"""
        if not force_recalc and CachedFoundryData.objects.filter(version=scenario).exists():
            self.stdout.write("[SKIP] Foundry data already cached")
            return
        
        self.stdout.write("[INFO] Computing Foundry data...")
        start_time = time.time()
        
        foundry_data = get_foundry_chart_data(scenario)
        
        # Clear existing data
        CachedFoundryData.objects.filter(version=scenario).delete()
        
        # Cache data for each foundry
        for foundry_site, data in foundry_data.items():
            CachedFoundryData.objects.create(
                version=scenario,
                foundry_site=foundry_site,
                chart_data=data['chart_data'],
                top_products=json.loads(data['top_products']) if isinstance(data['top_products'], str) else data['top_products'],
                monthly_pour_plan=data['monthly_pour_plan']
            )
        
        elapsed = time.time() - start_time
        self.stdout.write(f"[OK] Foundry data cached for {len(foundry_data)} sites ({elapsed:.2f}s)")

    def cache_forecast_data(self, scenario, force_recalc):
        """Cache forecast chart data by type"""
        if not force_recalc and CachedForecastData.objects.filter(version=scenario).exists():
            self.stdout.write("[SKIP] Forecast data already cached")
            return
        
        self.stdout.write("[INFO] Computing Forecast data...")
        start_time = time.time()
        
        forecast_functions = {
            'parent_product_group': get_forecast_data_by_parent_product_group,
            'product_group': get_forecast_data_by_product_group,
            'region': get_forecast_data_by_region,
            'customer': get_forecast_data_by_customer,
            'data_source': get_forecast_data_by_data_source,
        }
        
        # Clear existing data
        CachedForecastData.objects.filter(version=scenario).delete()
        
        # Cache data for each forecast type
        for data_type, func in forecast_functions.items():
            chart_data = func(scenario)
            CachedForecastData.objects.create(
                version=scenario,
                data_type=data_type,
                chart_data=chart_data
            )
        
        elapsed = time.time() - start_time
        self.stdout.write(f"[OK] Forecast data cached for {len(forecast_functions)} types ({elapsed:.2f}s)")

    def cache_inventory_data(self, scenario, force_recalc):
        """Cache complex inventory calculations"""
        if not force_recalc and CachedInventoryData.objects.filter(version=scenario).exists():
            self.stdout.write("[SKIP] Inventory data already cached")
            return
        
        self.stdout.write("[INFO] Computing Inventory data...")
        start_time = time.time()
        
        inventory_data = get_inventory_data_with_start_date(scenario)
        
        CachedInventoryData.objects.update_or_create(
            version=scenario,
            defaults={
                'inventory_months': inventory_data['inventory_months'],
                'inventory_cogs': inventory_data['inventory_cogs'],
                'inventory_revenue': inventory_data['inventory_revenue'],
                'production_aud': inventory_data['production_aud'],
                'production_cogs_group_chart': inventory_data['production_cogs_group_chart'],
                'top_products_by_group_month': inventory_data['top_products_by_group_month'],
                'parent_product_groups': inventory_data['parent_product_groups'],
                'cogs_data_by_group': inventory_data['cogs_data_by_group'],
            }
        )
        
        elapsed = time.time() - start_time
        self.stdout.write(f"[OK] Inventory data cached ({elapsed:.2f}s)")

    def cache_supplier_data(self, scenario, force_recalc):
        """Cache supplier production data"""
        if not force_recalc and CachedSupplierData.objects.filter(version=scenario).exists():
            self.stdout.write("[SKIP] Supplier data already cached")
            return
        
        self.stdout.write("[INFO] Computing Supplier data...")
        start_time = time.time()
        
        # Currently only HBZJBF02 supplier is being used
        supplier_codes = ['HBZJBF02']
        
        # Clear existing data
        CachedSupplierData.objects.filter(version=scenario).delete()
        
        for supplier_code in supplier_codes:
            chart_data = get_production_data_by_group(supplier_code, scenario)
            top_products = get_top_products_per_month_by_group(supplier_code, scenario)
            
            CachedSupplierData.objects.create(
                version=scenario,
                supplier_code=supplier_code,
                chart_data=chart_data,
                top_products=top_products
            )
        
        elapsed = time.time() - start_time
        self.stdout.write(f"[OK] Supplier data cached for {len(supplier_codes)} suppliers ({elapsed:.2f}s)")

    def cache_detailed_inventory_data(self, scenario, force_recalc):
        """Cache detailed inventory data (empty by default)"""
        if not force_recalc and CachedDetailedInventoryData.objects.filter(version=scenario).exists():
            self.stdout.write("[SKIP] Detailed inventory data already cached")
            return
        
        self.stdout.write("[INFO] Setting up empty Detailed Inventory data...")
        
        # This returns empty data by default - only populated when searched
        detailed_data = detailed_view_scenario_inventory(scenario)
        
        CachedDetailedInventoryData.objects.update_or_create(
            version=scenario,
            defaults={
                'inventory_data': detailed_data['inventory_data'],
                'production_data': detailed_data['production_data'],
            }
        )
        
        self.stdout.write("[OK] Detailed inventory data structure created")
