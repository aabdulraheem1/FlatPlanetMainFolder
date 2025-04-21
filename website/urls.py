from django.urls import path
from . import views
from .views import fetch_data_from_mssql

urlpatterns = [
   
   
    path('welcomepage/', views.welcomepage, name='welcomepage'),
    path('create-scenario/', views.create_scenario, name='create_scenario'),
    path('fetch-data/', fetch_data_from_mssql, name='fetch_data_from_mssql'),
    path('ProductsList/', views.product_list, name='ProductsList'),
    path('products/edit/<path:pk>/', views.edit_product, name='edit_product'),
    
    # Add other URL patterns as needed
]
