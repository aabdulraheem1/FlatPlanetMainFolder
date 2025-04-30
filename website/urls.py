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
    path('delete-forecast/<int:id>/', views.delete_forecast, name='delete_forecast'),
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
    path('incoterms/create/', views.incoterm_create, name='incoterm_create'),
    path('incoterms/<str:version>/update/', views.incoterm_update_formset, name='incoterm_update_formset'),
    path('incoterms/<str:version>/delete/', views.incoterm_delete_all, name='incoterm_delete_all'),
    path('incoterms/upload/', views.incoterm_upload, name='incoterm_upload'),

    # Master Data Inco Terms URLs
    path('master-data-inco-terms/<str:version>/update/', views.master_data_inco_terms_update_formset, name='master_data_inco_terms_update'),
    path('master-data-inco-terms/<str:version>/upload/', views.master_data_inco_terms_upload, name='master_data_inco_terms_upload'),
    path('master-data-inco-terms/<str:version>/delete/', views.master_data_inco_terms_delete_all, name='master_data_inco_terms_delete'),
    path('master-data-inco-terms/<str:version>/copy/', views.master_data_inco_terms_copy, name='master_data_inco_terms_copy'),




    # Add other URL patterns as needed
]
