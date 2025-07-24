# views.py - Optimized version

@login_required
def review_scenario(request, version):
    """Optimized review scenario with minimal initial load"""
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)

    # Only load essential data initially
    context = {
        'version': scenario.version,
        'user_name': user_name,
        'scenario': scenario,
    }
    
    return render(request, 'website/review_scenario_optimized.html', context)

@login_required
def load_section_data(request, version, section):
    """AJAX endpoint to load specific section data"""
    scenario = get_object_or_404(scenarios, version=version)
    
    if section == 'control_tower':
        from website.views_cached import get_cached_control_tower_data
        control_tower_data = get_cached_control_tower_data(scenario)
        return JsonResponse({
            'html': render_to_string('website/review_scenario_control_tower.html', {
                'demand_plan': control_tower_data['combined_demand_plan'],
                'poured_data': control_tower_data['poured_data'],
                'pour_plan': control_tower_data['pour_plan'],
                'version': version
            })
        })
    
    elif section == 'foundry':
        foundry_data = get_foundry_chart_data(scenario)
        return JsonResponse({
            'html': render_to_string('website/review_scenario_foundry.html', {
                'mt_joli_chart_data': foundry_data['MTJ1']['chart_data'],
                'mt_joli_top_products_json': foundry_data['MTJ1']['top_products'],
                'mt_joli_monthly_pour_plan': foundry_data['MTJ1']['monthly_pour_plan'],
                # ... other foundry data
                'version': version
            })
        })
    
    elif section == 'forecast':
        chart_data_parent_product_group = get_forecast_data_by_parent_product_group(scenario)
        chart_data_product_group = get_forecast_data_by_product_group(scenario)
        chart_data_region = get_forecast_data_by_region(scenario)
        chart_data_customer = get_forecast_data_by_customer(scenario)
        chart_data_data_source = get_forecast_data_by_data_source(scenario)
        
        return JsonResponse({
            'html': render_to_string('website/review_scenario_forecast.html', {
                'chart_data_parent_product_group': json.dumps(chart_data_parent_product_group),
                'chart_data_product_group': json.dumps(chart_data_product_group),
                'chart_data_region': json.dumps(chart_data_region),
                'chart_data_customer': json.dumps(chart_data_customer),
                'chart_data_data_source': json.dumps(chart_data_data_source),
                'version': version
            })
        })
    
    elif section == 'inventory':
        inventory_data = get_inventory_data_with_start_date(scenario)
        detailed_inventory_data = detailed_view_scenario_inventory(scenario)
        
        snapshot_date = None
        try:
            inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
            if inventory_snapshot:
                snapshot_date = inventory_snapshot.date_of_snapshot.strftime('%B %d, %Y')
        except:
            snapshot_date = "Date not available"
        
        return JsonResponse({
            'html': render_to_string('website/review_scenario_inventory.html', {
                'inventory_months': inventory_data['inventory_months'],
                'inventory_cogs': inventory_data['inventory_cogs'],
                'inventory_revenue': inventory_data['inventory_revenue'],
                'production_aud': inventory_data['production_aud'],
                'production_cogs_group_chart': json.dumps(inventory_data['production_cogs_group_chart']),
                'top_products_by_group_month': json.dumps(inventory_data['top_products_by_group_month']),
                'parent_product_groups': inventory_data['parent_product_groups'],
                'cogs_data_by_group': json.dumps(inventory_data['cogs_data_by_group']),
                'detailed_inventory_data': detailed_inventory_data['inventory_data'],
                'detailed_production_data': detailed_inventory_data['production_data'],
                'snapshot_date': snapshot_date,
                'version': version
            })
        })
    
    elif section == 'supplier':
        supplier_a_chart_data = get_production_data_by_group('HBZJBF02', scenario)
        supplier_a_top_products = get_top_products_per_month_by_group('HBZJBF02', scenario)
        
        return JsonResponse({
            'html': render_to_string('website/review_scenario_supplier.html', {
                'supplier_a_chart_data': supplier_a_chart_data,
                'supplier_a_top_products_json': json.dumps(supplier_a_top_products),
                'version': version
            })
        })
    
    return JsonResponse({'error': 'Invalid section'}, status=400)
