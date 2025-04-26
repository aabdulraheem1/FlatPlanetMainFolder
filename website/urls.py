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
    
    
    




    # Add other URL patterns as needed
]
