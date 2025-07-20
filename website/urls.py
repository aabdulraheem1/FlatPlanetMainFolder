from django.urls import path
from . import views
from .views import fetch_data_from_mssql
from django.shortcuts import render, redirect

urlpatterns = [
   
   
    path('welcomepage/', views.welcomepage, name='welcomepage'),
    path('create-scenario/', views.create_scenario, name='create_scenario'),
    path('fetch-data/', fetch_data_from_mssql, name='fetch_data_from_mssql'),
    path('ProductsList/', views.product_list, name='ProductsList'),
    path('products/edit/<path:pk>/', views.edit_product, name='edit_product'),
    path('delete_product/<path:pk>/', views.delete_product, name='delete_product'),
    path('PlantsList/', views.plants_list, name='PlantsList'),
    path('plants-fetch-data/', views.plants_fetch_data_from_mssql, name='plants-fetch_data_from_mssql'),
    path('plants/edit/<path:pk>/', views.edit_plant, name='edit_plant'),    
    path('success/', lambda request: render(request, 'success.html'), name='success'),
    path('edit-scenario/<int:scenario_id>/', views.edit_scenario, name='edit_scenario'),    
    
    path('delete-forecast/<str:version>/<str:data_source>/', views.delete_forecast, name='delete_forecast'),
    path('copy-forecast/<str:version>/<str:data_source>/', views.copy_forecast, name='copy_forecast'),

    path('list-scenarios/', views.list_scenarios, name='list_scenarios'),
    path('edit-scenario/<str:version>/', views.edit_scenario, name='edit_scenario'),
    path('delete-scenario/<str:version>/', views.delete_scenario, name='delete_scenario'),

    path('upload_forecast/<str:forecast_type>/', views.upload_forecast, name='upload_forecast'),    
    path('edit_forecasts/<str:version>/<str:forecast_type>/', views.edit_forecasts, name='edit_forecasts'),
    
    path('scenario/review/<str:version>', views.review_scenario, name='review_scenario'),
    path('warningList/<str:version>', views.ScenarioWarningList, name='warningList'),
    path('create_product/', views.create_product, name='create_product'),
    path('update_product_allocation/<str:version>/', views.update_product_allocation, name='update_product_allocation'),
    path('delete_product_allocation/<str:version>/', views.delete_product_allocation, name='delete_product_allocation'),
    path('update_product_allocation/<str:version>/', views.update_product_allocation, name='update_product_allocation'),
    path('copy_product_allocation/<str:version>/', views.copy_product_allocation, name='copy_product_allocation'),    
    path('upload_product_allocation/<str:version>/', views.upload_product_allocation, name='upload_product_allocation'),
    path('upload_production_history/<str:version>/', views.upload_production_history, name='upload_production_history'),
    path('delete_production_history/<str:version>/', views.delete_production_history, name='delete_production_history'),
    path('update_production_history/<str:version>/', views.update_production_history, name='update_production_history'),
    path('copy_production_history/<str:version>/', views.copy_production_history, name='copy_production_history'),
    # On hand stock and in Transit from Epicor
    path('update_on_hand_stock/<str:version>/', views.update_on_hand_stock, name='update_on_hand_stock'),
    path('delete_on_hand_stock/<str:version>/', views.delete_on_hand_stock, name='delete_on_hand_stock'),
    path('upload_on_hand_stock/<str:version>/', views.upload_on_hand_stock, name='upload_on_hand_stock'),
    path('copy_on_hand_stock/<str:version>/', views.copy_on_hand_stock, name='copy_on_hand_stock'),

    path('customers/', views.customers_list, name='CustomersList'),
    path('forecast-region/', views.forecast_region_list, name='ForecastRegionList'),
    path('forecast-region/', views.forecast_region_list, name='ForecastRegionList'),
    path('forecast-region/add/', views.add_forecast_region, name='add_forecast_region'),
    path('forecast-region/update/<str:region_id>/', views.update_forecast_region, name='update_forecast_region'),
    path('forecast-region/delete/<str:region_id>/', views.delete_forecast_region, name='delete_forecast_region'),

    path('update_master_data_freight/<str:version>/', views.update_master_data_freight, name='update_master_data_freight'),
    path('delete_master_data_freight/<str:version>/', views.delete_master_data_freight, name='delete_master_data_freight'),
    path('copy_master_data_freight/<str:version>/', views.copy_master_data_freight, name='copy_master_data_freight'),
    path('upload_master_data_freight/<str:version>/', views.upload_master_data_freight, name='upload_master_data_freight'),

    path('update_master_data_casto_to_despatch_days/<str:version>/', views.update_master_data_casto_to_despatch_days, name='update_master_data_casto_to_despatch_days'),
    path('delete_master_data_casto_to_despatch_days/<str:version>/', views.delete_master_data_casto_to_despatch_days, name='delete_master_data_casto_to_despatch_days'),
    path('copy_master_data_casto_to_despatch_days/<str:version>/', views.copy_master_data_casto_to_despatch_days, name='copy_master_data_casto_to_despatch_days'),

    path('incoterms/', views.incoterm_list, name='incoterm_list'),
    path('incoterms/create/<str:version>/', views.incoterm_create, name='incoterm_create'),
    path('incoterms/<str:version>/update/', views.incoterm_update_formset, name='incoterm_update_formset'),
    path('incoterms/<str:version>/delete/', views.incoterm_delete_all, name='incoterm_delete_all'),
    path('incoterms/upload/', views.incoterm_upload, name='incoterm_upload'),

    # Master Data Inco Terms URLs
    path('master-data-inco-terms/<str:version>/update/', views.master_data_inco_terms_update_formset, name='master_data_inco_terms_update'),
    path('master-data-inco-terms/<str:version>/upload/', views.master_data_inco_terms_upload, name='master_data_inco_terms_upload'),
    path('master-data-inco-terms/<str:version>/delete/', views.master_data_inco_terms_delete_all, name='master_data_inco_terms_delete'),
    path('master-data-inco-terms/<str:version>/copy/', views.master_data_inco_terms_copy, name='master_data_inco_terms_copy'),


    path('edit_scenario/<str:version>/update_master_data_plan/', views.update_master_data_plan, name='update_master_data_plan'),
    path('edit_scenario/<str:version>/delete_master_data_plan/', views.delete_master_data_plan, name='delete_master_data_plan'),
    path('edit_scenario/<str:version>/upload_master_data_plan/', views.upload_master_data_plan, name='upload_master_data_plan'),
    path('edit_scenario/<str:version>/copy_master_data_plan/', views.copy_master_data_plan, name='copy_master_data_plan'),
    path('edit_scenario/<str:version>/update_pour_plan_data/', views.update_pour_plan_data, name='update_pour_plan_data'),

    path('edit_scenario/<str:version>/update_master_data_capacity/', views.update_master_data_capacity, name='update_master_data_capacity'),
    path('edit_scenario/<str:version>/delete_master_data_capacity/', views.delete_master_data_capacity, name='delete_master_data_capacity'),
    path('edit_scenario/<str:version>/upload_master_data_capacity/', views.upload_master_data_capacity, name='upload_master_data_capacity'),
    path('edit_scenario/<str:version>/copy_master_data_capacity/', views.copy_master_data_capacity, name='copy_master_data_capacity'),

    # URL for the suppliers list
    path('suppliers/', views.suppliers_list, name='suppliers_list'),

    # URL for the customers list
    path('customers/', views.customers_list, name='customers_list'),

    # URL for fetching suppliers data from the server
    path('suppliers/fetch/', views.suppliers_fetch_data_from_mssql, name='suppliers_fetch_data'),

    # URL for fetching customers data from the server
    path('customers/fetch/', views.customers_fetch_data_from_mssql, name='customers_fetch_data'),

    path('supplyoptions', views.SupplyOptions, name='supplyoptions'),




    # Updated URLs for Epicor Supplier Master Data
    path('update-epicor-supplier/<str:version>/', views.update_epicor_supplier_master_data, name='update_production_epicor_master_data'),
    path('delete-epicor-supplier/<str:version>/', views.delete_epicor_supplier_master_data, name='delete_production_epicor_master_data'),
    path('copy-epicor-supplier/<str:version>/', views.copy_epicor_supplier_master_data, name='copy_production_epicor_master_data'),
    path('upload-epicor-supplier/<str:version>/', views.upload_epicor_supplier_master_data, name='upload_production_epicor_master_data'),

    # ... existing URL patterns ...
    path('calculate-model/<str:version>/', views.calculate_model, name='calculate_model'),
    
    # Other URL patterns...
    path('create-plant/', views.create_plant, name='create_plant'),

    path('bom/', views.bom_list, name='bom_list'),
    path('bom/fetch/', views.BOM_fetch_data_from_mssql, name='BOM_fetch_data_from_mssql'),

    path('update_manually_assign_production_requirement/<str:version>/', views.update_manually_assign_production_requirement, name='update_manually_assign_production_requirement'),
    path('delete_manually_assign_production_requirement/<str:version>/', views.delete_manually_assign_production_requirement, name='delete_manually_assign_production_requirement'),
    path('upload_manually_assign_production_requirement/<str:version>/', views.upload_manually_assign_production_requirement, name='upload_manually_assign_production_requirement'),
    path('copy_manually_assign_production_requirement/<str:version>/', views.copy_manually_assign_production_requirement, name='copy_manually_assign_production_requirement'),

    path('add_manually_assign_production_requirement/<str:version>/', views.add_manually_assign_production_requirement, name='add_manually_assign_production_requirement'),

     # Add these three URLs for the scenario review action buttons
    path('manual_optimize_product/<str:version>/', views.manual_optimize_product, name='manual_optimize_product'),
    path('balance_hard_green_sand/<str:version>/', views.balance_hard_green_sand, name='balance_hard_green_sand'),
    path('create_balanced_pour_plan/<str:version>/', views.create_balanced_pour_plan, name='create_balanced_pour_plan'),

    # ... other url patterns ...
    path('update_products_cost/<str:version>/', views.update_products_cost, name='update_products_cost'),
    path('delete_products_cost/<str:version>/', views.delete_products_cost, name='delete_products_cost'),
    path('upload_products_cost/<str:version>/', views.upload_products_cost, name='upload_products_cost'),
    path('copy_products_cost/<str:version>/', views.copy_products_cost, name='copy_products_cost'),

    path('update_fixed_plant_conversion_modifiers/<str:version>/', views.update_fixed_plant_conversion_modifiers, name='update_fixed_plant_conversion_modifiers'),
    path('delete_fixed_plant_conversion_modifiers/<str:version>/', views.delete_fixed_plant_conversion_modifiers, name='delete_fixed_plant_conversion_modifiers'),
    path('upload_fixed_plant_conversion_modifiers/<str:version>/', views.upload_fixed_plant_conversion_modifiers, name='upload_fixed_plant_conversion_modifiers'),
    path('copy_fixed_plant_conversion_modifiers/<str:version>/', views.copy_fixed_plant_conversion_modifiers, name='copy_fixed_plant_conversion_modifiers'),
    
    # Revenue Conversion Modifiers URLs
    path('update_revenue_conversion_modifiers/<str:version>/', views.update_revenue_conversion_modifiers, name='update_revenue_conversion_modifiers'),
    path('delete_revenue_conversion_modifiers/<str:version>/', views.delete_revenue_conversion_modifiers, name='delete_revenue_conversion_modifiers'),
    path('upload_revenue_conversion_modifiers/<str:version>/', views.upload_revenue_conversion_modifiers, name='upload_revenue_conversion_modifiers'),
    path('copy_revenue_conversion_modifiers/<str:version>/', views.copy_revenue_conversion_modifiers, name='copy_revenue_conversion_modifiers'),

    # Revenue to COGS Conversion URLs
    path('scenario/<str:version>/update-revenue-to-cogs-conversion/', views.update_revenue_to_cogs_conversion, name='update_revenue_to_cogs_conversion'),
    path('scenario/<str:version>/upload-revenue-to-cogs-conversion/', views.upload_revenue_to_cogs_conversion, name='upload_revenue_to_cogs_conversion'),
    path('scenario/<str:version>/delete-revenue-to-cogs-conversion/', views.delete_revenue_to_cogs_conversion, name='delete_revenue_to_cogs_conversion'),
    path('scenario/<str:version>/copy-revenue-to-cogs-conversion/', views.copy_revenue_to_cogs_conversion, name='copy_revenue_to_cogs_conversion'),
    
    # Site Allocation URLs
    path('scenario/<str:version>/update-site-allocation/', views.update_site_allocation, name='update_site_allocation'),
    path('scenario/<str:version>/upload-site-allocation/', views.upload_site_allocation, name='upload_site_allocation'),
    path('scenario/<str:version>/delete-site-allocation/', views.delete_site_allocation, name='delete_site_allocation'),
    path('scenario/<str:version>/copy-site-allocation/', views.copy_site_allocation, name='copy_site_allocation'),

    # Method of Manufacturing URLs
    path('method-of-manufacturing/', views.method_of_manufacturing_list, name='method_of_manufacturing_list'),
    path('method-of-manufacturing/fetch/', views.method_of_manufacturing_fetch_data_from_mssql, name='method_of_manufacturing_fetch_data_from_mssql'),

     # ... existing patterns ...
    path('detailed-view-inventory/', views.detailed_view_scenario_inventory, name='detailed_view_scenario_inventory'),

    path('search-detailed-inventory/', views.search_detailed_inventory, name='search_detailed_inventory'),



    # Add other URL patterns as needed
]
