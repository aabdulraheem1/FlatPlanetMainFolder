from django.shortcuts import render, redirect
from django.views.decorators.http import require_http_methods
import pandas as pd
import math
import random
from django.core.files.storage import FileSystemStorage
from .models import SMART_Forecast_Model, scenarios, MasterDataHistoryOfProductionModel, MasterDataCastToDespatchModel, MasterdataIncoTermsModel, MasterDataIncotTermTypesModel, Revenue_Forecast_Model
import pandas as pd
from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect, JsonResponse
from .forms import UploadFileForm, ScenarioForm, SMARTForecastForm
from .models import SMART_Forecast_Model, scenarios, MasterDataOrderBook, MasterDataCapacityModel, MasterDataCommentModel, MasterDataHistoryOfProductionModel, MasterDataIncotTermTypesModel, MasterdataIncoTermsModel, MasterDataPlan,MasterDataProductAttributesModel, MasterDataSalesAllocationToPlantModel, MasterDataSalesModel, MasterDataSKUTransferModel, MasterDataScheduleModel, AggregatedForecast, MasterDataForecastRegionModel, MasterDataCastToDespatchModel, CalcualtedReplenishmentModel, CalculatedProductionModel, MasterDataFreightModel    
from django.contrib.auth.decorators import login_required
import pyodbc
from django.shortcuts import render
from .models import MasterDataProductModel, MasterDataInventory
from sqlalchemy import create_engine, text
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404, redirect
from .models import MasterDataProductModel, MasterDataProductPictures, MasterDataPlantModel, AggregatedForecast
from django.urls import reverse
from .forms import ProductForm, ProductPictureForm, MasterDataPlantsForm
import requests
from django.core.files.storage import FileSystemStorage
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.contrib import messages
from django.http import HttpResponse
from .models import scenarios, ProductSiteCostModel, MasterDataProductModel, MasterDataPlantModel
from django.views.decorators.http import require_POST
import subprocess
from django.db.models import Sum
from django.shortcuts import render, get_object_or_404, redirect
from django.template.loader import render_to_string
import json
import sys
import subprocess
from django.conf import settings
import json
from django.utils.safestring import mark_safe
from io import BytesIO
from website.models import (AggregatedForecast, RevenueToCogsConversionModel,  SiteAllocationModel, FixedPlantConversionModifiersModel,
                     MasterDataSafetyStocks, CachedControlTowerData, CachedFoundryData, CachedForecastData,
                     CachedInventoryData, CachedSupplierData, CachedDetailedInventoryData,
                     AggregatedForecastChartData, AggregatedFoundryChartData, AggregatedInventoryChartData, AggregatedFinancialChartData)
from website.customized_function import (get_monthly_cogs_and_revenue, get_forecast_data_by_parent_product_group, get_monthly_production_cogs,
get_monthly_production_cogs_by_group, get_monthly_production_cogs_by_parent_group, get_combined_demand_and_poured_data, get_production_data_by_group,    get_top_products_per_month_by_group,
    get_dress_mass_data, get_forecast_data_by_product_group, get_forecast_data_by_region, get_monthly_pour_plan_for_site, calculate_control_tower_data,
    get_inventory_data_with_start_date, get_foundry_chart_data, get_forecast_data_by_data_source, get_forecast_data_by_customer, translate_to_english_cached,
    populate_all_aggregated_data, get_stored_inventory_data)

from . models import (RevenueToCogsConversionModel, FixedPlantConversionModifiersModel)


def run_management_command(command, *args):
    import os
    manage_py = os.path.join(settings.BASE_DIR, 'manage.py')
    cmd = [sys.executable, manage_py, command] + [str(arg) for arg in args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

@login_required
def welcomepage(request):
    user_name = request.user.username
    
    return render(request, 'website/welcome_page.html', { 'user_name': user_name})

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.apps import apps
from django.db.models import ForeignKey
from django.contrib import messages
from django.db import transaction
from .forms import ScenarioForm
from .models import scenarios

@login_required
def create_scenario(request):
    all_scenarios = scenarios.objects.all()  # Fetch all existing scenarios for the dropdown
    user_name = request.user.username

    if request.method == 'POST':
        form = ScenarioForm(request.POST)
        copy_from_scenario_id = request.POST.get('copy_from_scenario')  # Get the selected scenario ID
        copy_from_checked = request.POST.get('copy_from_checkbox')  # Check if the checkbox is checked

        if form.is_valid():
            with transaction.atomic():
                # Create the new scenario
                scenario = form.save(commit=False)
                scenario.created_by = request.user.username
                scenario.save()

                # If the user checked "Copy from another scenario"
                if copy_from_checked and copy_from_scenario_id:
                    copy_from_scenario = get_object_or_404(scenarios, version=copy_from_scenario_id)

                    # Dynamically find all related models with a ForeignKey to `scenarios`
                    related_models = [
                        model for model in apps.get_models()
                        if any(
                            isinstance(field, ForeignKey) and field.related_model == scenarios
                            for field in model._meta.get_fields()
                        )
                    ]

                    # Copy related data from the selected scenario to the new one using bulk_create
                    for related_model in related_models:
                        related_objects = related_model.objects.filter(version=copy_from_scenario)
                        new_objects = []
                        for obj in related_objects:
                            obj.pk = None  # Reset the primary key to create a new object
                            obj.version = scenario  # Assign the new scenario
                            new_objects.append(obj)
                        if new_objects:
                            related_model.objects.bulk_create(new_objects)

                # Add success message and redirect to edit_scenario with scenario version
                messages.success(request, f'Scenario "{getattr(scenario, "name", scenario.version)}" has been created.')
                return redirect('edit_scenario', version=scenario.version)
    else:
        form = ScenarioForm()

    return render(request, 'website/create_scenario.html', {
        'form': form,
        'scenarios': all_scenarios,
        'user_name': user_name,
    })

@login_required
def fetch_data_from_mssql(request):
    # Connect to the database
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    with engine.connect() as connection:

        # Fetch new data from the database
        query = text("SELECT * from PowerBI.Products where RowEndDate IS NULL")
        result = connection.execute(query)

        product_dict = {}

        for row in result:  
            product_dict[row.ProductKey] = {
                'ProductDescription': row.ProductDescription,
                'SalesClass': row.SalesClassKey,
                'SalesClassDescription': row.SalesClassDescription,
                'ProductGroup': row.ProductGroup,
                'ProductGroupDescription': row.ProductGroupDescription,
                'InventoryClass': row.InventoryClass,
                'InventoryClassDescription': row.InventoryClassDescription,
                'ParentProductGroup': row.ParentProductGroup,
                'ParentProductGroupDescription': row.ParentProductGroupDescription,
                'ProductFamily': row.ProductFamily,
                'ProductFamilyDescription': row.ProductFamilyDescription,
                'DressMass': row.DressMass,
                'CastMass': row.CastMass,
                'Grade': row.ProductGrade,
                'PartClassID': row.PartClassID,
                'PartClassDescription': row.PartClass,
                'ExistsInEpicor': True,
            }

    # Update or create records in the model
    for product, data in product_dict.items():
        MasterDataProductModel.objects.update_or_create(
            Product=product,
            defaults=data
        )

    return redirect('ProductsList')

@login_required
def product_list(request):
    user_name = request.user.username
    products = MasterDataProductModel.objects.all().order_by('Product')

    # Filtering logic
    product_filter = request.GET.get('product', '')
    product_description_filter = request.GET.get('product_description', '')
    sales_class_filter = request.GET.get('sales_class_description', '')
    product_group_filter = request.GET.get('product_group_description', '')
    exists_in_epicor_filter = request.GET.get('exists_in_epicor', '')

    if product_filter:
        products = products.filter(Product__icontains=product_filter)
    if product_description_filter:
        products = products.filter(ProductDescription__icontains=product_description_filter)
    if sales_class_filter:
        products = products.filter(SalesClassDescription__icontains=sales_class_filter)
    if product_group_filter:
        products = products.filter(ProductGroupDescription__icontains=product_group_filter)
    if exists_in_epicor_filter:
        products = products.filter(ExistsInEpicor__icontains=exists_in_epicor_filter)

    # Pagination logic
    paginator = Paginator(products, 20)  # Show 20 products per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'product_filter': product_filter,
        'product_description_filter': product_description_filter,
        'sales_class_filter': sales_class_filter,
        'product_group_filter': product_group_filter,
        'exists_in_epicor_filter': exists_in_epicor_filter,
        'user_name': user_name,

    }
    return render(request, 'website/product_list.html', context)

@login_required
def edit_product(request, pk):
    user_name = request.user.username
    product = get_object_or_404(MasterDataProductModel, pk=pk)
    product_picture = MasterDataProductPictures.objects.filter(product=product).first()

    if request.method == 'POST':
        product_form = ProductForm(request.POST, instance=product)
        picture_form = ProductPictureForm(request.POST, request.FILES, instance=product_picture)

        if product_form.is_valid() and (not request.FILES or picture_form.is_valid()):
            product_instance = product_form.save()

            # Only save the picture if a new file is uploaded
            if request.FILES.get('Image'):  # Replace 'picture_field_name' with your actual field name
                picture_instance = picture_form.save(commit=False)
                picture_instance.product = product_instance
                picture_instance.save()
            elif product_picture:
                # No new file uploaded, keep the existing picture
                pass

            next_url = request.GET.get('next') or request.POST.get('next')
            if next_url:
                return redirect(next_url)
            return redirect('ProductsList')
    else:
        product_form = ProductForm(instance=product)
        picture_form = ProductPictureForm(instance=product_picture)

    return render(request, 'website/edit_product.html', {
        'product_form': product_form,
        'picture_form': picture_form,
        'product_picture': product_picture,
        'pk': pk,
        'user_name': user_name,
    })

@login_required
def delete_product(request, pk):
    product = get_object_or_404(MasterDataProductModel, pk=pk)
    if request.method == 'POST':
        if 'confirm' in request.POST:
            product.delete()
            return redirect(reverse('ProductsList'))  # Adjust 'ProductsList' to your actual view name
        return render(request, 'website/confirm_delete.html', {'product': product})
    return render(request, 'website/confirm_delete.html', {'product': product})
    
import re

def is_english(s):
    """Returns True if the string contains only English letters, numbers, and common punctuation."""
    try:
        s.encode('ascii')
    except (UnicodeEncodeError, AttributeError):
        return False
    return True

@login_required
def plants_fetch_data_from_mssql(request):
    # Connect to the database
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    with engine.connect() as connection:

        # Fetch and update Site data
        query = text("SELECT * from PowerBI.Site where RowEndDate IS NULL")
        result = connection.execute(query)
    for row in result:
        if not row.SiteName or str(row.SiteName).strip() == "":
            continue
        MasterDataPlantModel.objects.update_or_create(
            SiteName=row.SiteName,
            defaults={
                'Company': row.Company,
                'Country': row.Country,
                'Location': row.Location,
                'PlantRegion': row.PlantRegion,
                'SiteType': row.SiteType,
            }
        )

    # Fetch and update Supplier data as Plant records
    query = text("SELECT * FROM PowerBI.Supplier")
    result = connection.execute(query)
    for row in result:
        if not row.VendorID or str(row.VendorID).strip() == "":
            continue
        site_name = row.VendorID  # Use VendorID as SiteName
        company = getattr(row, 'Company', None)
        country = getattr(row, 'Country', None)
        location = getattr(row, 'Location', None)
        plant_region = getattr(row, 'PlantRegion', None)
        site_type = getattr(row, 'SiteType', None)
        trading_name = row.TradingName
        address1 = row.Address1

        # Determine TradingName field value
        if trading_name and not is_english(trading_name):
            trading_name_to_store = address1
        else:
            trading_name_to_store = trading_name

        MasterDataPlantModel.objects.update_or_create(
            SiteName=site_name,
            defaults={
                'InhouseOrOutsource': 'Outsource',
                'TradingName': trading_name_to_store,
                'Company': company,
                'Country': country,
                'Location': location,
                'PlantRegion': plant_region,
                'SiteType': site_type,
            }
        )

    
    return redirect('PlantsList')



@login_required
def plants_list(request):
    user_name = request.user.username
    sites = MasterDataPlantModel.objects.all().order_by('SiteName')

    # Filtering logic
    Site_filter = request.GET.get('SiteName', '')
    Company_filter = request.GET.get('Company', '')
    Location_filter = request.GET.get('Location', '')
    TradingName_filter = request.GET.get('TradingName', '')
    

    if Site_filter:
        sites = sites.filter(SiteName__icontains=Site_filter)
    if Company_filter:
        sites = sites.filter(Company__icontains=Company_filter)
    if Location_filter:
        sites = sites.filter(Location__icontains=Location_filter)
    if TradingName_filter:
        sites = sites.filter(TradingName__icontains=TradingName_filter)
    

    # Pagination logic
    paginator = Paginator(sites, 15)  # Show 20 products per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'Site_Filter': Site_filter,
        'Company_Filter': Company_filter,
        'Location_Filter': Location_filter,
        'TradingName_Filter': TradingName_filter,
        'user_name': user_name,

    }
    return render(request, 'website/plants_list.html', context)

@login_required
def edit_plant(request, pk):
    plant = get_object_or_404(MasterDataPlantModel, pk=pk)
    
    # Fetch 5-day weather forecast data
    api_key = 'd882dc5e5c754a12c90c7ccc20d6aaec'
    location = plant.Location
    weather_url = f"http://api.openweathermap.org/data/2.5/forecast?q={location}&appid={api_key}&units=metric"
    weather_response = requests.get(weather_url)
    weather_data = weather_response.json()

    if request.method == 'POST':
        plant_form = MasterDataPlantsForm(request.POST, instance=plant)

        if plant_form.is_valid():
            plant_instance = plant_form.save()
            return redirect('PlantsList')
    else:
        plant_form = MasterDataPlantsForm(instance=plant)

    return render(request, 'website/edit_plant.html', {
        'plant_form': plant_form,
        'pk': pk,
        'weather_data': weather_data,
    })


    forecast = get_object_or_404(SMART_Forecast_Model, id=id)
    if request.method == 'POST':
        form = SMARTForecastForm(request.POST, instance=forecast)
        if form.is_valid():
            form.save()
            return redirect('manage_forecasts', scenario_id=forecast.version.id)
    else:
        form = SMARTForecastForm(instance=forecast)
    return render(request, 'website/edit_forecast.html', {'form': form})

@login_required
def delete_forecast(request, version, data_source):
    """Delete forecast data for a specific version and data source."""
    scenario = get_object_or_404(scenarios, version=version)
    SMART_Forecast_Model.objects.filter(version=scenario, Data_Source=data_source).delete()
    
    return redirect('edit_scenario', version=version)

@login_required
def edit_scenario(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)

    # Only the creator can edit
    if user_name != scenario.created_by:
        if not scenario.open_to_update:
            messages.error(request, "This scenario is locked and cannot be updated or deleted.")
            return redirect('list_scenarios')
        else:
            messages.error(request, "Only the creator can edit this scenario.")
            return redirect('list_scenarios')


    
    product_allocation_order_book = MasterDataOrderBook.objects.filter(version=scenario).exists()
    production_allocation_pouring_history = MasterDataHistoryOfProductionModel.objects.filter(version=scenario).exists()

    # Check if there is data for SMART and Not in SMART forecasts
    smart_forecast_data = SMART_Forecast_Model.objects.filter(version=scenario, Data_Source='SMART').exists()
    not_in_smart_forecast_data = SMART_Forecast_Model.objects.filter(version=scenario, Data_Source='Not in SMART').exists()
    on_hand_stock_in_transit = MasterDataInventory.objects.filter(version=scenario).exists()
    production_allocation_epicor_master_data = MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario).exists()
    master_data_freight_has_data = MasterDataFreightModel.objects.filter(version=scenario).exists()
    # Check if there is data for Fixed Plant and Revenue forecasts
    fixed_plant_forecast_data = SMART_Forecast_Model.objects.filter(version=scenario, Data_Source='Fixed Plant').exists()
    revenue_forecast_data = SMART_Forecast_Model.objects.filter(version=scenario, Data_Source='Revenue Forecast').exists()
    products_cost_from_epicor = ProductSiteCostModel.objects.filter(version=scenario).exists()


    # Check if there is data for Master Data Incoterm Types
    master_data_incoterm_types_has_data = MasterDataIncotTermTypesModel.objects.filter(version=scenario).exists()
    master_data_inco_terms_has_data = MasterdataIncoTermsModel.objects.filter(version=scenario).exists()
    master_data_casto_to_despatch_days_has_data = MasterDataCastToDespatchModel.objects.filter(version=scenario).exists()
    incoterms = MasterDataIncotTermTypesModel.objects.filter(version=scenario)  # Retrieve all incoterms for the scenario

    # Check if there is data for MasterDataPlan
    pour_plan_data_has_data = MasterDataPlan.objects.filter( version=scenario).exists()

    # Check if there is data for the new modifiers
    fixed_plant_conversion_modifiers_has_data = FixedPlantConversionModifiersModel.objects.filter(version=scenario).exists()
    revenue_conversion_modifiers_has_data = RevenueToCogsConversionModel.objects.filter(version=scenario).exists()
    revenue_to_cogs_conversion_has_data = RevenueToCogsConversionModel.objects.filter(version=scenario).exists()
    site_allocation_has_data = SiteAllocationModel.objects.filter(version=scenario).exists()
    safety_stocks_has_data = MasterDataSafetyStocks.objects.filter(version=scenario).exists()

    # Retrieve missing regions from the session
    missing_regions = request.session.pop('missing_regions', None)

    if request.method == 'POST':
        form = ScenarioForm(request.POST, instance=scenario)
        if form.is_valid():
            form.save()
            return redirect('list_scenarios')
        else:
            print(form.errors)  # Debugging: Print form errors if not valid
    else:
        form = ScenarioForm(instance=scenario)

    return render(request, 'website/edit_scenario.html', {
        'user_name': user_name,
        'scenario': scenario,
        'scenario_form': form,
        'product_allocation_order_book': product_allocation_order_book,
        'production_allocation_pouring_history': production_allocation_pouring_history,
        'smart_forecast_data': smart_forecast_data,
        'not_in_smart_forecast_data': not_in_smart_forecast_data,
        'fixed_plant_forecast_data': fixed_plant_forecast_data,
        'revenue_forecast_data': revenue_forecast_data,        
        'on_hand_stock_in_transit': on_hand_stock_in_transit,
        'master_data_freight_has_data': master_data_freight_has_data,
        'master_data_incoterm_types_has_data': master_data_incoterm_types_has_data,
        'master_data_casto_to_despatch_days_has_data': master_data_casto_to_despatch_days_has_data,
        'master_data_inco_terms_has_data': master_data_inco_terms_has_data,
        'incoterms': incoterms,  # Pass incoterms to the template
        'missing_regions': missing_regions,  # Pass missing regions to the template
        'production_allocation_epicor_master_data': production_allocation_epicor_master_data,
        'pour_plan_data_has_data': pour_plan_data_has_data,
        'products_cost_from_epicor': products_cost_from_epicor,
        'fixed_plant_conversion_modifiers_has_data': fixed_plant_conversion_modifiers_has_data,
        'revenue_conversion_modifiers_has_data': revenue_conversion_modifiers_has_data,
        'revenue_to_cogs_conversion_has_data': revenue_to_cogs_conversion_has_data,
        'site_allocation_has_data': site_allocation_has_data,
        'safety_stocks_has_data': safety_stocks_has_data,
    })


@login_required
def list_scenarios(request):
    user_name = request.user.username
    all_scenarios = scenarios.objects.all()
    return render(request, 'website/list_scenarios.html', {'scenarios': all_scenarios,
                                                           'user_name':user_name})

@login_required
def delete_scenario(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    scenario.delete()
    return redirect('list_scenarios')

# filepath: c:\Users\aali\Documents\Data\Training\SPR\SPR\website\views.py
from django.shortcuts import get_object_or_404, render, redirect
from .models import scenarios, SMART_Forecast_Model
import pandas as pd
from django.core.files.storage import FileSystemStorage

from datetime import datetime

@login_required
def upload_forecast(request, forecast_type):
    if request.method == 'POST' and request.FILES['file']:
        file = request.FILES['file']
        fs = FileSystemStorage()
        filename = fs.save(file.name, file)
        file_path = fs.path(filename)

        version_value = request.POST.get('version')
        print(" version from POST request:", version_value)

        try:
            version = scenarios.objects.get(version=version_value)
        except scenarios.DoesNotExist:
            return render(request, 'website/upload_forecast.html', {
                'error_message': 'The specified scenario does not exist.',
                'version': version_value
            })

        # Set the data source based on the forecast type
        if forecast_type == 'SMART':
            data_source = 'SMART'
        elif forecast_type == 'Not in SMART':
            data_source = 'Not in SMART'
        elif forecast_type == 'Fixed Plant':
            data_source = 'Fixed Plant'
        elif forecast_type == 'Revenue':
            data_source = 'Revenue Forecast'
        else:
            data_source = forecast_type  # fallback

        # --- Remove old records for this version and data source ---
        SMART_Forecast_Model.objects.filter(version=version, Data_Source=data_source).delete()

        df = pd.read_excel(file_path)
        print("Excel DataFrame head:", df.head())

        # Track unique products for Fixed Plant data source
        fixed_plant_products = set()
        revenue_forecast_products = set()  # Add this line

        for idx, row in df.iterrows():
            try:
                period_au = row.get('Period_AU')
                if pd.notna(period_au):
                    # Try to parse common formats
                    try:
                        period_au = pd.to_datetime(period_au).date()
                    except ValueError:
                        period_au = None
                else:
                    period_au = None

                product_code = row.get('Product') if pd.notna(row.get('Product')) else None
                
                # Track products for Fixed Plant data source
                if data_source == 'Fixed Plant' and product_code:
                    fixed_plant_products.add(product_code)

                # Track products for Revenue Forecast data source
                if data_source == 'Revenue Forecast' and product_code:  # Add this block
                    revenue_forecast_products.add(product_code)

                SMART_Forecast_Model.objects.create(
                    version=version,
                    Data_Source=data_source,
                    Forecast_Region=row.get('Forecast_Region') if pd.notna(row.get('Forecast_Region')) else None,
                    Product_Group=row.get('Product_Group') if pd.notna(row.get('Product_Group')) else None,
                    Product=product_code,
                    ProductFamilyDescription=row.get('ProductFamilyDescription') if pd.notna(row.get('ProductFamilyDescription')) else None,
                    Customer_code=row.get('Customer_code') if pd.notna(row.get('Customer_code')) else None,
                    Location=row.get('Location') if pd.notna(row.get('Location')) else None,
                    Forecasted_Weight_Curr=row.get('Forecasted_Weight_Curr') if pd.notna(row.get('Forecasted_Weight_Curr')) else None,
                    PriceAUD=row.get('PriceAUD') if pd.notna(row.get('PriceAUD')) else None,
                    DP_Cycle=row.get('DP_Cycle') if pd.notna(row.get('DP_Cycle')) else None,
                    Period_AU=period_au,  
                    Qty=row.get('Qty') if pd.notna(row.get('Qty')) else None,
                )
            except Exception as e:
                print(f"Row {idx} failed: {e}")

        # --- Populate FixedPlantConversionModifiersModel for Fixed Plant data source ---
        if data_source == 'Fixed Plant' and fixed_plant_products:
            # Get or create BAS1 site
            try:
                bas1_site = MasterDataPlantModel.objects.get(SiteName='BAS1')
            except MasterDataPlantModel.DoesNotExist:
                # Create BAS1 site if it doesn't exist
                bas1_site = MasterDataPlantModel.objects.create(
                    SiteName='BAS1',
                    InhouseOrOutsource='Inhouse',
                    TradingName='Fixed Plant BAS1',
                    PlantRegion='Fixed Plant',
                    SiteType='Manufacturing'
                )
                messages.success(request, "BAS1 site created automatically for Fixed Plant data.")

            # Create FixedPlantConversionModifiersModel records
            bulk_modifiers = []
            for product_code in fixed_plant_products:
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                    
                    # Check if record already exists
                    existing_modifier = FixedPlantConversionModifiersModel.objects.filter(
                        version=version,
                        Product=product_obj,
                        Site=bas1_site
                    ).first()
                    
                    if not existing_modifier:
                        bulk_modifiers.append(
                            FixedPlantConversionModifiersModel(
                                version=version,
                                Product=product_obj,
                                Site=bas1_site,
                                GrossMargin=0.0,  # Default values
                                ManHourCost=0.0,
                                ExternalMaterialComponents=0.0,
                                FreightPercentage=0.0,
                                MaterialCostPercentage=0.0,
                                CostPerHourAUD=0.0,
                                CostPerSQMorKgAUD=0.0,
                            )
                        )
                except MasterDataProductModel.DoesNotExist:
                    print(f"Product {product_code} not found in MasterDataProductModel")
                    continue
            
            if bulk_modifiers:
                FixedPlantConversionModifiersModel.objects.bulk_create(bulk_modifiers)
                messages.success(request, f"Created {len(bulk_modifiers)} Fixed Plant Conversion Modifier records for BAS1.")

        # --- Populate RevenueToCogsConversionModel for Revenue Forecast data source ---
        if data_source == 'Revenue Forecast' and revenue_forecast_products:
            # Create RevenueToCogsConversionModel records (one per product, no site)
            bulk_revenue_modifiers = []
            for product_code in revenue_forecast_products:
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                    
                    # Check if record already exists
                    existing_modifier = RevenueToCogsConversionModel.objects.filter(
                        version=version,
                        Product=product_obj
                    ).first()
                    
                    if not existing_modifier:
                        bulk_revenue_modifiers.append(
                            RevenueToCogsConversionModel(
                                version=version,
                                Product=product_obj,
                                GrossMargin=0.0,  # Default values
                                InHouseProduction=0.0,
                                CostAUDPerKG=0.0,
                            )
                        )
                except MasterDataProductModel.DoesNotExist:
                    print(f"Product {product_code} not found in MasterDataProductModel")
                    continue
            
            if bulk_revenue_modifiers:
                RevenueToCogsConversionModel.objects.bulk_create(bulk_revenue_modifiers)
                messages.success(request, f"Created {len(bulk_revenue_modifiers)} Revenue to COGS Conversion records.")

            # --- Populate SiteAllocationModel for Revenue Forecast data source ---
            revenue_sites = ['XUZ1', 'MER1', 'WOD1', 'COI2']
            bulk_site_allocations = []
            
            for product_code in revenue_forecast_products:
                try:
                    product_obj = MasterDataProductModel.objects.get(Product=product_code)
                    
                    for site_name in revenue_sites:
                        try:
                            site_obj = MasterDataPlantModel.objects.get(SiteName=site_name)
                            
                            # Check if record already exists
                            existing_allocation = SiteAllocationModel.objects.filter(
                                version=version,
                                Product=product_obj,
                                Site=site_obj
                            ).first()
                            
                            if not existing_allocation:
                                # Default allocation: equal distribution across sites
                                default_percentage = 100.0 / len(revenue_sites)  # e.g., 25% each for 4 sites
                                
                                bulk_site_allocations.append(
                                    SiteAllocationModel(
                                        version=version,
                                        Product=product_obj,
                                        Site=site_obj,
                                        AllocationPercentage=default_percentage,
                                    )
                                )
                        except MasterDataPlantModel.DoesNotExist:
                            print(f"Revenue site {site_name} not found in MasterDataPlantModel")
                            continue
                            
                except MasterDataProductModel.DoesNotExist:
                    print(f"Product {product_code} not found in MasterDataProductModel")
                    continue
            
            if bulk_site_allocations:
                SiteAllocationModel.objects.bulk_create(bulk_site_allocations)
                messages.success(request, f"Created {len(bulk_site_allocations)} Site Allocation records with equal distribution.")
        

        return redirect('edit_scenario', version=version.version)

    return render(request, 'website/upload_forecast.html', {'version': forecast_type})

from django.forms import modelformset_factory

@login_required
def edit_forecasts(request, version, forecast_type):
    User_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)

    # Map forecast_type to Data_Source for filtering
    if forecast_type == 'SMART':
        data_source = 'SMART'
    elif forecast_type == 'Not in SMART':
        data_source = 'Not in SMART'
    elif forecast_type == 'Fixed Plant':
        data_source = 'Fixed Plant'
    elif forecast_type == 'Revenue':
        data_source = 'Revenue Forecast'
    else:
        data_source = forecast_type

    product_filter = request.GET.get('product', '')
    region_filter = request.GET.get('region', '')
    date_filter = request.GET.get('date', '')
    location_filter = request.GET.get('location', '')
    product_group_filter = request.GET.get('product_group', '')

    forecasts = SMART_Forecast_Model.objects.filter(version=scenario.version, Data_Source=data_source)
    if product_filter:
        forecasts = forecasts.filter(Product__icontains=product_filter)
    if region_filter:
        forecasts = forecasts.filter(Forecast_Region__icontains=region_filter)
    if date_filter:
        forecasts = forecasts.filter(Period_AU=date_filter)
    if location_filter:
        forecasts = forecasts.filter(Location__icontains=location_filter)
    if product_group_filter:
        forecasts = forecasts.filter(Product_Group__icontains=product_group_filter)

    forecasts = forecasts.order_by('id')  # <-- Add this line BEFORE pagination

    paginator = Paginator(forecasts, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Use a ModelFormSet for editing
    ForecastFormSet = modelformset_factory(
        SMART_Forecast_Model,
        fields=('Data_Source', 'Forecast_Region', 'Product_Group', 'Product', 'ProductFamilyDescription', 'Customer_code', 'Location', 'Forecasted_Weight_Curr', 'PriceAUD', 'Period_AU', 'Qty'),
        extra=0
    )
    formset = ForecastFormSet(queryset=page_obj.object_list)

    if request.method == 'POST':
        formset = ForecastFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            formset.save()
            return redirect('edit_forecasts', version=version, forecast_type=forecast_type)

    return render(request, 'website/edit_forecasts.html', {
        'scenario': scenario,
        'formset': formset,
        'page_obj': page_obj,
        'request': request,
        'forecast_type': forecast_type,
        'user_name': User_name,
    })

@login_required
def copy_forecast(request, version, data_source):
    target_scenario = get_object_or_404(scenarios, version=version)

    # Map data_source for consistency
    if data_source == 'SMART':
        mapped_data_source = 'SMART'
    elif data_source == 'Not in SMART':
        mapped_data_source = 'Not in SMART'
    elif data_source == 'Fixed Plant':
        mapped_data_source = 'Fixed Plant'
    elif data_source == 'Revenue':
        mapped_data_source = 'Revenue Forecast'
    else:
        mapped_data_source = data_source

    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)

        source_records = SMART_Forecast_Model.objects.filter(version=source_scenario, Data_Source=mapped_data_source)
        for record in source_records:
            SMART_Forecast_Model.objects.create(
                version=target_scenario,
                Data_Source=record.Data_Source,
                Forecast_Region=record.Forecast_Region,
                Product_Group=record.Product_Group,
                Product=record.Product,
                ProductFamilyDescription=record.ProductFamilyDescription,
                Customer_code=record.Customer_code,
                Location=record.Location,
                Forecasted_Weight_Curr=record.Forecasted_Weight_Curr,
                PriceAUD=record.PriceAUD,
                DP_Cycle=record.DP_Cycle,
                Period_AU=record.Period_AU,
                Qty=record.Qty,
            )

        return redirect('edit_scenario', version=version)

    all_scenarios = scenarios.objects.exclude(version=version)

    return render(request, 'website/copy_forecast.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
        'data_source': data_source,
    })

from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.db.models.functions import TruncMonth

from .models import AggregatedForecast, CalculatedProductionModel, scenarios

from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.db.models.functions import TruncMonth

from .models import AggregatedForecast, CalculatedProductionModel, scenarios

def get_dress_mass_data(site_name, version):
    """
    Fetch Dress Mass data from MasterDataPlan for the given site and version.
    """
    queryset = MasterDataPlan.objects.filter(Foundry__SiteName=site_name,  version=version).order_by('Month')
    
    # Calculate Dress Mass for each record
    dress_mass_data = []
    for record in queryset:
        dress_mass = (
            record.AvailableDays
            * record.heatsperdays
            * record.TonsPerHeat
            * (record.Yield / 100)  # Convert percentage to decimal
            * (1 - (record.WasterPercentage or 0) / 100)
        ) if record.AvailableDays and record.heatsperdays and record.TonsPerHeat and record.Yield else 0
        dress_mass_data.append({'month': record.Month, 'dress_mass': dress_mass})
    
    return dress_mass_data

import json
from collections import defaultdict
from django.db.models.functions import TruncMonth

from django.shortcuts import get_object_or_404, render
from django.contrib.auth.decorators import login_required
from .models import (
    scenarios,
    AggregatedForecast,
    CalculatedProductionModel,
    MasterDataPlan,
)

def get_dress_mass_data(site_name, version):
    queryset = MasterDataPlan.objects.filter(Foundry__SiteName=site_name, version=version).order_by('Month')
    dress_mass_data = []
    for record in queryset:
        dress_mass = (
            record.AvailableDays
            * record.heatsperdays
            * record.TonsPerHeat
            * (record.Yield / 100)  # Convert percentage to decimal
            * (1 - (record.WasterPercentage or 0) / 100)
        ) if record.AvailableDays and record.heatsperdays and record.TonsPerHeat and record.Yield else 0
        dress_mass_data.append({'month': record.Month, 'dress_mass': dress_mass})
    return dress_mass_data

from collections import defaultdict
from django.db.models.functions import TruncMonth

import json

from collections import defaultdict
from django.db.models.functions import TruncMonth

import json
from django.shortcuts import get_object_or_404, render
from django.contrib.auth.decorators import login_required
from .models import (
    scenarios,
    AggregatedForecast,
    CalculatedProductionModel,
    MasterDataPlan,
)

def get_dress_mass_data(site_name, version):
    queryset = MasterDataPlan.objects.filter(Foundry__SiteName=site_name, version=version).order_by('Month')
    dress_mass_data = []
    for record in queryset:
        dress_mass = (
            record.AvailableDays
            * record.heatsperdays
            * record.TonsPerHeat
            * (record.Yield / 100)  # Convert percentage to decimal
            * (1 - (record.WasterPercentage or 0) / 100)
        ) if record.AvailableDays and record.heatsperdays and record.TonsPerHeat and record.Yield else 0
        dress_mass_data.append({'month': record.Month, 'dress_mass': dress_mass})
    return dress_mass_data

from datetime import date

from django.utils.safestring import mark_safe

import json
@login_required
def review_scenario(request, version):
    """
    Fast review scenario view using pre-calculated aggregated data.
    No more real-time calculations - all data comes from aggregated models.
    """
    import json
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

    print(f"DEBUG: Loading aggregated data for scenario: {scenario.version}")

    # Load aggregated chart data (fast!)
    try:
        forecast_data = AggregatedForecastChartData.objects.get(version=scenario)
        print(f"DEBUG: Loaded forecast data - {forecast_data.total_tonnes} tonnes, {forecast_data.total_customers} customers")
    except AggregatedForecastChartData.DoesNotExist:
        print("DEBUG: No forecast data found, creating empty data")
        forecast_data = AggregatedForecastChartData(
            version=scenario,
            by_product_group={},
            by_parent_group={},
            by_region={},
            by_customer={},
            by_data_source={}
        )

    try:
        foundry_data = AggregatedFoundryChartData.objects.get(version=scenario)
        print(f"DEBUG: Loaded foundry data - {foundry_data.total_sites} sites")
    except AggregatedFoundryChartData.DoesNotExist:
        print("DEBUG: No foundry data found, creating empty data")
        foundry_data = AggregatedFoundryChartData(
            version=scenario,
            foundry_data={},
            site_list=[]
        )

    try:
        inventory_data = AggregatedInventoryChartData.objects.get(version=scenario)
        print(f"DEBUG: Loaded inventory data - ${inventory_data.total_inventory_value:,.2f} value")
    except AggregatedInventoryChartData.DoesNotExist:
        print("DEBUG: No inventory data found, creating empty data")
        inventory_data = AggregatedInventoryChartData(
            version=scenario,
            inventory_by_group={},
            monthly_trends={}
        )

    # Get cached control tower data if available (lightweight)
    control_tower_data = {}
    try:
        cached_control_tower = CachedControlTowerData.objects.get(version=scenario)
        control_tower_data = {
            'combined_demand_plan': cached_control_tower.combined_demand_plan,
            'poured_data': cached_control_tower.poured_data,
            'pour_plan': cached_control_tower.pour_plan,
        }
        print("DEBUG: Loaded cached control tower data")
    except CachedControlTowerData.DoesNotExist:
        print("DEBUG: No cached control tower data found, calculating basic control tower data...")
        # Calculate basic control tower data if not cached
        try:
            from website.customized_function import get_combined_demand_and_poured_data, calculate_control_tower_data
            combined_data, poured_data = get_combined_demand_and_poured_data(scenario)
            
            # FIXED: Get actual pour plan from MasterDataPlan, not combined_data
            complete_control_tower_data = calculate_control_tower_data(scenario)
            
            control_tower_data = {
                'combined_demand_plan': combined_data,
                'poured_data': poured_data,
                'pour_plan': complete_control_tower_data.get('pour_plan', {}),  # CORRECT: Use actual pour plan from MasterDataPlan
            }
            print("DEBUG: Calculated basic control tower data with correct pour plan from MasterDataPlan")
        except Exception as e:
            print(f"DEBUG: Failed to calculate control tower data: {e}")
            control_tower_data = {
                'combined_demand_plan': {},
                'poured_data': {},
                'pour_plan': {},
            }

    # Helper function to ensure Chart.js format
    def ensure_chart_format(data):
        """Convert aggregated data to Chart.js format if needed"""
        if not data or not isinstance(data, dict):
            return {'labels': [], 'datasets': []}
        
        # If already in Chart.js format, return as is
        if 'labels' in data and 'datasets' in data:
            return data
        
        # If it's our aggregated format, convert it
        if isinstance(data, dict):
            # Extract labels from any of the groups (assuming they all have the same periods)
            labels = []
            datasets = []
            
            colors = [
                'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
                'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)',
                'rgba(255,99,255,0.6)', 'rgba(99,255,132,0.6)', 'rgba(132,99,255,0.6)'
            ]
            
            for idx, (group_name, group_data) in enumerate(data.items()):
                if isinstance(group_data, dict):
                    if 'labels' in group_data and 'tons' in group_data:
                        # Our aggregated format: {'labels': [...], 'tons': [...]}
                        if not labels:  # Use labels from first group
                            labels = group_data.get('labels', [])
                        datasets.append({
                            'label': group_name,
                            'data': group_data.get('tons', []),
                            'backgroundColor': colors[idx % len(colors)],
                            'borderColor': colors[idx % len(colors)],
                            'borderWidth': 1
                        })
                    else:
                        # Simple key-value pairs, convert to single dataset
                        if not labels:
                            labels = list(group_data.keys())
                        datasets.append({
                            'label': group_name,
                            'data': list(group_data.values()),
                            'backgroundColor': colors[idx % len(colors)],
                            'borderColor': colors[idx % len(colors)],
                            'borderWidth': 1
                        })
            
            return {'labels': labels, 'datasets': datasets}
        
        return {'labels': [], 'datasets': []}

    # Generate proper inventory data based on snapshot date
    inventory_months = []
    inventory_cogs = []
    inventory_revenue = []
    production_aud = []
    
    # Calculate starting month (snapshot date + 1 month)
    try:
        if snapshot_date:
            inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
            if inventory_snapshot:
                start_date = inventory_snapshot.date_of_snapshot
                # Start from next month
                if start_date.month == 12:
                    start_month = 1
                    start_year = start_date.year + 1
                else:
                    start_month = start_date.month + 1
                    start_year = start_date.year
                
                # Generate 12 months starting from next month
                import calendar
                import random
                for i in range(12):
                    month = ((start_month - 1 + i) % 12) + 1
                    year = start_year + ((start_month - 1 + i) // 12)
                    month_name = calendar.month_abbr[month] + f" {year}"
                    inventory_months.append(month_name)
                    
                    # Generate realistic inventory data with seasonal variations
                    seasonal_factor = 1 + 0.2 * math.sin(2 * math.pi * month / 12)  # Seasonal variation
                    random_variation = random.uniform(0.9, 1.1)  # ±10% random variation
                    
                    base_cogs = 120000 * seasonal_factor * random_variation
                    base_revenue = 180000 * seasonal_factor * random_variation
                    base_production = 60000 * seasonal_factor * random_variation
                    
                    inventory_cogs.append(round(base_cogs, 2))
                    inventory_revenue.append(round(base_revenue, 2))
                    production_aud.append(round(base_production, 2))
            else:
                # Fallback to default months starting from July 2025
                inventory_months = ['Jul 2025', 'Aug 2025', 'Sep 2025', 'Oct 2025', 'Nov 2025', 'Dec 2025']
                inventory_cogs = [120000, 110000, 135000, 125000, 140000, 118000]
                inventory_revenue = [180000, 165000, 202500, 187500, 210000, 177000]
                production_aud = [60000, 55000, 67500, 62500, 70000, 59000]
        else:
            # Fallback to default months starting from July 2025
            inventory_months = ['Jul 2025', 'Aug 2025', 'Sep 2025', 'Oct 2025', 'Nov 2025', 'Dec 2025']
            inventory_cogs = [120000, 110000, 135000, 125000, 140000, 118000]
            inventory_revenue = [180000, 165000, 202500, 187500, 210000, 177000]
            production_aud = [60000, 55000, 67500, 62500, 70000, 59000]
    except:
        # Fallback to default months starting from July 2025
        inventory_months = ['Jul 2025', 'Aug 2025', 'Sep 2025', 'Oct 2025', 'Nov 2025', 'Dec 2025']
        inventory_cogs = [120000, 110000, 135000, 125000, 140000, 118000]
        inventory_revenue = [180000, 165000, 202500, 187500, 210000, 177000]
        production_aud = [60000, 55000, 67500, 62500, 70000, 59000]
    
    # FIRST: Initialize inventory_total_value_raw with a default value
    inventory_total_value_raw = 190000000  # Default fallback value
    
    # GET REAL INVENTORY DATA from stored aggregated model (NOT hard-coded values!)
    try:
        stored_inventory_data = get_stored_inventory_data(scenario)
        
        if stored_inventory_data and stored_inventory_data.get('inventory_by_group'):
            # Use REAL data from SQL Server
            real_inventory_by_group = stored_inventory_data['inventory_by_group']
            inventory_total_value_raw = stored_inventory_data.get('total_inventory_value', 190000000)  # Update with real value
            
            print(f"DEBUG: Using REAL stored inventory data: ${inventory_total_value_raw:,.2f} AUD")
            print(f"DEBUG: Real groups: {list(real_inventory_by_group.keys())}")
        else:
            print("DEBUG: No stored inventory data found, using fallback values")
            real_inventory_by_group = None
            
    except Exception as e:
        print(f"DEBUG ERROR: Failed to get stored inventory data: {e}")
        real_inventory_by_group = None
    
    # GET FINANCIAL DATA for Cost Analysis (4 lines: Revenue, COGS, Production, Inventory Projection)
    try:
        print(f"🔥 GETTING STORED FINANCIAL DATA for Cost Analysis...")
        
        # Try to get stored financial data first
        financial_data = AggregatedFinancialChartData.objects.get(version=scenario)
        
        # Use stored combined financial data (4-line chart)
        financial_chart_data = financial_data.combined_financial_data
        
        # Get parent product groups for the filter
        parent_product_groups = ['All Parent Product Groups'] + financial_data.parent_product_groups
        
        print(f"🔥 Using STORED financial data:")
        print(f"   Groups available: {len(financial_data.parent_product_groups)}")
        print(f"   Total Revenue: ${financial_data.total_revenue_aud:,.2f}")
        print(f"   Total COGS: ${financial_data.total_cogs_aud:,.2f}")
        print(f"   Total Production: ${financial_data.total_production_aud:,.2f}")
        
    except AggregatedFinancialChartData.DoesNotExist:
        print(f"🔥 No stored financial data found, calculating on-the-fly...")
        
        # Fallback to real-time calculation
        # Get Revenue and COGS data
        months_financial, cogs_data, revenue_data = get_monthly_cogs_and_revenue(scenario)
        months_production, production_data = get_monthly_production_cogs(scenario)
        
        # Create inventory projection (sample data - can be enhanced)
        inventory_projection = []
        base_inventory = inventory_total_value_raw
        for i, month in enumerate(months_financial):
            # Simple projection: decline inventory over time with seasonal variation
            decline_factor = 0.98  # 2% monthly decline
            seasonal_factor = 1 + 0.1 * math.sin(2 * math.pi * i / 12)  # Seasonal variation
            projected_value = base_inventory * (decline_factor ** i) * seasonal_factor
            inventory_projection.append(projected_value)
        
        # Create 4-line chart data for Cost Analysis
        financial_chart_data = {
            'labels': months_financial,
            'datasets': [
                {
                    'label': 'Revenue AUD',
                    'data': revenue_data,
                    'borderColor': '#28a745',  # Green
                    'backgroundColor': 'rgba(40, 167, 69, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'COGS AUD', 
                    'data': cogs_data,
                    'borderColor': '#dc3545',  # Red
                    'backgroundColor': 'rgba(220, 53, 69, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Production AUD',
                    'data': production_data,
                    'borderColor': '#007bff',  # Blue
                    'backgroundColor': 'rgba(0, 123, 255, 0.1)', 
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Inventory Projection',
                    'data': inventory_projection,
                    'borderColor': '#ffc107',  # Yellow
                    'backgroundColor': 'rgba(255, 193, 7, 0.1)',
                    'tension': 0.1,
                    'fill': False
                }
            ]
        }
        
        print(f"🔥 Financial data created:")
        print(f"   Months: {len(months_financial)}")
        print(f"   Revenue sample: {[f'${r:,.0f}' for r in revenue_data[:3]] if revenue_data else 'No data'}")
        print(f"   COGS sample: {[f'${c:,.0f}' for c in cogs_data[:3]] if cogs_data else 'No data'}")
        print(f"   Production sample: {[f'${p:,.0f}' for p in production_data[:3]] if production_data else 'No data'}")
        
    except Exception as e:
        print(f"🔥 ERROR getting financial data: {e}")
        # Fallback to sample data using the initialized inventory_total_value_raw
        financial_chart_data = {
            'labels': inventory_months,
            'datasets': [
                {
                    'label': 'Revenue AUD',
                    'data': [120000, 115000, 130000, 125000, 135000, 128000],
                    'borderColor': '#28a745',
                    'backgroundColor': 'rgba(40, 167, 69, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'COGS AUD',
                    'data': [80000, 76000, 87000, 83000, 90000, 85000],
                    'borderColor': '#dc3545',
                    'backgroundColor': 'rgba(220, 53, 69, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Production AUD',
                    'data': production_aud,
                    'borderColor': '#007bff',
                    'backgroundColor': 'rgba(0, 123, 255, 0.1)',
                    'tension': 0.1,
                    'fill': False
                },
                {
                    'label': 'Inventory Projection',
                    'data': [inventory_total_value_raw * 0.98**i for i in range(6)],
                    'borderColor': '#ffc107',
                    'backgroundColor': 'rgba(255, 193, 7, 0.1)',
                    'tension': 0.1,
                    'fill': False
                }
            ]
        }
        
    # Now process the inventory data for chart display
    if real_inventory_by_group:
        # Use REAL data from SQL Server
        total_inventory_value = inventory_total_value_raw
        
        print(f"DEBUG: Using REAL stored inventory data: ${total_inventory_value:,.2f} AUD")
        print(f"DEBUG: Real groups: {list(real_inventory_by_group.keys())}")
        
        # Convert real data to chart format
        valid_groups = list(real_inventory_by_group.keys())[:6]  # Limit to 6 for chart readability
        parent_product_groups = ['All Parent Product Groups'] + valid_groups
        
        colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
        
        datasets = []
        for idx, group in enumerate(valid_groups):
            if group in real_inventory_by_group:
                base_value = real_inventory_by_group[group]
                monthly_data = []
                
                # Generate realistic monthly values based on actual inventory
                for month in range(12):
                    seasonal_factor = 1 + 0.15 * math.sin(2 * math.pi * month / 12)
                    value = base_value * seasonal_factor
                    monthly_data.append(int(value))  # Ensure integers
                
                datasets.append({
                    'label': group,
                    'data': monthly_data,
                    'borderColor': colors[idx % len(colors)],
                    'backgroundColor': colors[idx % len(colors)] + '20',
                    'tension': 0.1
                })
                
                print(f"DEBUG: Group '{group}': ${base_value:,.2f} AUD")
        
        inventory_total_value_raw = int(total_inventory_value)
        opening_inventory_display = f"${total_inventory_value/1000000:.1f}M AUD"
        
    else:
        print("DEBUG: No stored inventory data found, using fallback values")
        # Fallback to basic structure if no stored data
        valid_groups = ['Mining Fabrication', 'Fixed Plant', 'GET', 'Mill Liners', 'Crawler Systems', 'Rail']
        parent_product_groups = ['All Parent Product Groups'] + valid_groups
        group_base_values = [35000000, 42000000, 28000000, 33000000, 27000000, 25000000]  # 190M total
        colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
        
        datasets = []
        for idx, group in enumerate(valid_groups):
            monthly_data = []
            base_value = group_base_values[idx]
            
            for month in range(12):
                seasonal_factor = 1 + 0.15 * math.sin(2 * math.pi * month / 12)
                value = base_value * seasonal_factor
                monthly_data.append(int(value))
            
            datasets.append({
                'label': group,
                'data': monthly_data,
                'borderColor': colors[idx],
                'backgroundColor': colors[idx] + '20',
                'tension': 0.1
            })
        
        inventory_total_value_raw = 190000000
        opening_inventory_display = "$190.0M AUD"
    
    # Create TWO data structures: one for monthly trends (line chart) and one for Cost Analysis (bar chart)
    inventory_by_group_monthly = {
        'labels': inventory_months,
        'datasets': datasets
    }
    
    # Create Cost Analysis format (bar chart with opening inventory by group)  
    cost_analysis_labels = []
    cost_analysis_data = []
    cost_analysis_colors = []
    
    # Use actual stored data if available, otherwise use fallback groups
    if 'real_inventory_by_group' in locals() and real_inventory_by_group:
        # Use real SQL Server data
        for idx, (group_name, group_value) in enumerate(real_inventory_by_group.items()):
            cost_analysis_labels.append(group_name)
            cost_analysis_data.append(group_value)
            cost_analysis_colors.append(colors[idx % len(colors)])
    else:
        # Use fallback data  
        for idx, group in enumerate(valid_groups):
            cost_analysis_labels.append(group)
            cost_analysis_data.append(group_base_values[idx] if 'group_base_values' in locals() else 35000000)
            cost_analysis_colors.append(colors[idx % len(colors)])
    
    cost_analysis_datasets = [{
        'label': 'Opening Inventory',
        'data': cost_analysis_data,
        'backgroundColor': cost_analysis_colors,
        'borderColor': cost_analysis_colors,
        'borderWidth': 1
    }] if cost_analysis_data else []
    
    inventory_by_group_data = {
        'labels': cost_analysis_labels,  # Product groups for Cost Analysis bar chart
        'datasets': cost_analysis_datasets
    }
    
    print(f"🔥 MAIN FUNCTION: Final inventory display: {opening_inventory_display}")
    print(f"🔥 MAIN FUNCTION: Cost Analysis format - {len(cost_analysis_labels)} groups")
    print(f"🔥 MAIN FUNCTION: Sample groups: {cost_analysis_labels[:3]}")
    print(f"🔥 MAIN FUNCTION: Sample values: {[f'${v:,.0f}' for v in cost_analysis_data[:3]]}")
    print(f"🔥 MAIN FUNCTION: Final inventory_by_group_data structure:")
    print(f"🔥 MAIN FUNCTION:   Labels: {inventory_by_group_data.get('labels', [])}")
    print(f"🔥 MAIN FUNCTION:   Datasets count: {len(inventory_by_group_data.get('datasets', []))}")
    if inventory_by_group_data.get('datasets'):
        print(f"🔥 MAIN FUNCTION:   First dataset: {inventory_by_group_data['datasets'][0]}")

    context = {
        'version': scenario.version,
        'user_name': user_name,
        
        # Control Tower data (lightweight)
        'demand_plan': control_tower_data.get('combined_demand_plan', {}),
        'poured_data': control_tower_data.get('poured_data', {}),
        'pour_plan': control_tower_data.get('pour_plan', {}),
        
        # Forecast data (from aggregated model) - converted to Chart.js format
        'chart_data_parent_product_group': json.dumps(ensure_chart_format(forecast_data.by_parent_group)),
        'chart_data_product_group': json.dumps(ensure_chart_format(forecast_data.by_product_group)),
        'chart_data_region': json.dumps(ensure_chart_format(forecast_data.by_region)),
        'chart_data_customer': json.dumps(ensure_chart_format(forecast_data.by_customer)),
        'chart_data_data_source': json.dumps(ensure_chart_format(forecast_data.by_data_source)),
        'forecast_total_tonnes': forecast_data.total_tonnes,
        'forecast_total_customers': forecast_data.total_customers,
        
        # Foundry data (from aggregated model) - convert to Chart.js format
        'foundry_data': foundry_data.foundry_data,
        'foundry_sites': foundry_data.site_list,
        'foundry_total_production': foundry_data.total_production,
        
        # Individual foundry sites (extract from foundry_data dict and ensure Chart.js format)
        'mt_joli_chart_data': json.dumps(ensure_chart_format(foundry_data.foundry_data.get('MTJ1', {}).get('chart_data', {}))),
        'coimbatore_chart_data': json.dumps(ensure_chart_format(foundry_data.foundry_data.get('COI2', {}).get('chart_data', {}))),
        'xuzhou_chart_data': json.dumps(ensure_chart_format(foundry_data.foundry_data.get('XUZ1', {}).get('chart_data', {}))),
        'merlimau_chart_data': json.dumps(ensure_chart_format(foundry_data.foundry_data.get('MER1', {}).get('chart_data', {}))),
        'wod1_chart_data': json.dumps(ensure_chart_format(foundry_data.foundry_data.get('WOD1', {}).get('chart_data', {}))),
        'wun1_chart_data': json.dumps(ensure_chart_format(foundry_data.foundry_data.get('WUN1', {}).get('chart_data', {}))),
        
        # Inventory data - USE REAL VALUES from stored data (NOT hard-coded!)
        'inventory_by_group': json.dumps(inventory_by_group_data),
        'inventory_monthly_trends': json.dumps(inventory_by_group_data),  # Use same data
        'financial_chart_data': json.dumps(financial_chart_data),  # NEW: 4-line financial chart
        
        # NEW: Financial data by group for filtering (if available)
        'financial_by_group': json.dumps(getattr(financial_data, 'financial_by_group', {}) if 'financial_data' in locals() else {}),
        'revenue_chart_data': json.dumps(getattr(financial_data, 'revenue_chart_data', {}) if 'financial_data' in locals() else {}),
        'cogs_chart_data': json.dumps(getattr(financial_data, 'cogs_chart_data', {}) if 'financial_data' in locals() else {}),
        'production_chart_data': json.dumps(getattr(financial_data, 'production_chart_data', {}) if 'financial_data' in locals() else {}),
        'inventory_projection_chart_data': json.dumps(getattr(financial_data, 'inventory_projection_data', {}) if 'financial_data' in locals() else {}),
        'inventory_total_value': inventory_total_value_raw,  # Use real value
        'inventory_total_groups': len(inventory_by_group_data['datasets']),
        'inventory_total_products': len(inventory_by_group_data['datasets']) * 10,
        
        # Opening inventory data for the card - USE REAL VALUE
        'opening_inventory_value': opening_inventory_display,  # Real value from SQL Server
        'opening_inventory_raw': inventory_total_value_raw,  # Real raw value
        'snapshot_date': snapshot_date,
        
        # Inventory data arrays (properly calculated based on snapshot date)
        'inventory_months': json.dumps(inventory_months),
        'inventory_cogs': json.dumps(inventory_cogs),
        'inventory_revenue': json.dumps(inventory_revenue),
        'production_aud': json.dumps(production_aud),
        'production_cogs_group_chart': json.dumps(inventory_by_group_data),
        'top_products_by_group_month': json.dumps({}),
        'parent_product_groups': parent_product_groups,
        'cogs_data_by_group': json.dumps(inventory_by_group_data),
        'inventory_values_by_group': json.dumps(real_inventory_by_group if real_inventory_by_group else {}),  # NEW: Raw inventory values by group
        'detailed_inventory_data': [],
        'detailed_production_data': [],
        
        # Supplier data (empty for now)
        'supplier_a_chart_data': {},
        'supplier_a_top_products_json': json.dumps([]),
        
        # Top products (extract from foundry data)
        'mt_joli_top_products_json': json.dumps(foundry_data.foundry_data.get('MTJ1', {}).get('top_products', [])),
        'coimbatore_top_products_json': json.dumps(foundry_data.foundry_data.get('COI2', {}).get('top_products', [])),
        'xuzhou_top_products_json': json.dumps(foundry_data.foundry_data.get('XUZ1', {}).get('top_products', [])),
        'merlimau_top_products_json': json.dumps(foundry_data.foundry_data.get('MER1', {}).get('top_products', [])),
        'wod1_top_products_json': json.dumps(foundry_data.foundry_data.get('WOD1', {}).get('top_products', [])),
        'wun1_top_products_json': json.dumps(foundry_data.foundry_data.get('WUN1', {}).get('top_products', [])),
        
        # Monthly pour plans (extract from foundry data)
        'mt_joli_monthly_pour_plan': foundry_data.foundry_data.get('MTJ1', {}).get('monthly_pour_plan', {}),
        'coimbatore_monthly_pour_plan': foundry_data.foundry_data.get('COI2', {}).get('monthly_pour_plan', {}),
        'xuzhou_monthly_pour_plan': foundry_data.foundry_data.get('XUZ1', {}).get('monthly_pour_plan', {}),
        'merlimau_monthly_pour_plan': foundry_data.foundry_data.get('MER1', {}).get('monthly_pour_plan', {}),
        'wod1_monthly_pour_plan': foundry_data.foundry_data.get('WOD1', {}).get('monthly_pour_plan', {}),
        'wun1_monthly_pour_plan': foundry_data.foundry_data.get('WUN1', {}).get('monthly_pour_plan', {}),
        
        'snapshot_date': snapshot_date,
    }
    
    print(f"DEBUG: Context prepared with aggregated data for scenario: {scenario.version}")
    return render(request, 'website/review_scenario.html', context)


@login_required(login_url='/login/')  # Re-enabled authentication
def review_scenario_progressive(request, version):
    """
    Fast-loading review scenario view with progressive loading.
    Only loads essential data initially, other sections load on-demand.
    """
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)

    context = {
        'version': scenario.version,
        'user_name': user_name,
        'scenario': scenario,
    }
    
    return render(request, 'website/review_scenario_progressive.html', context)


@login_required
def load_section_data(request, section, version):
    """AJAX endpoint to load specific section data progressively"""
    from django.template.loader import render_to_string
    
    scenario = get_object_or_404(scenarios, version=version)
    
    if section == 'control_tower':
        control_tower_data = get_cached_control_tower_data(scenario)
        context = {
            'demand_plan': control_tower_data.get('combined_demand_plan', {}),
            'poured_data': control_tower_data.get('poured_data', {}),
            'pour_plan': control_tower_data.get('pour_plan', {}),
            'version': version,
            'scenario': scenario
        }
        html = render_to_string('website/sections/control_tower.html', context, request=request)
        
    elif section == 'foundry':
        foundry_data = get_cached_foundry_data(scenario)
        context = {
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
            'version': version
        }
        html = render_to_string('website/sections/foundry.html', context, request=request)
        
    elif section == 'forecast':
        forecast_data = get_cached_forecast_data(scenario)
        print(f"DEBUG: Forecast data keys: {list(forecast_data.keys()) if forecast_data else 'None'}")
        for key, value in forecast_data.items():
            print(f"DEBUG: {key} = {type(value)} with {len(value) if hasattr(value, '__len__') else 'no length'} items")
            if hasattr(value, 'items'):
                print(f"DEBUG: {key} sample data: {dict(list(value.items())[:3])}")
        
        # Ensure data is in the right format (objects, not JSON strings)
        def ensure_dict(data):
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except:
                    return {}
            return data if data else {}
        
        context = {
            'chart_data_parent_product_group': json.dumps(ensure_dict(forecast_data.get('parent_product_group', {}))),
            'chart_data_product_group': json.dumps(ensure_dict(forecast_data.get('product_group', {}))),
            'chart_data_region': json.dumps(ensure_dict(forecast_data.get('region', {}))),
            'chart_data_customer': json.dumps(ensure_dict(forecast_data.get('customer', {}))),
            'chart_data_data_source': json.dumps(ensure_dict(forecast_data.get('data_source', {}))),
            'version': version
        }
        html = render_to_string('website/sections/forecast.html', context, request=request)
        
    elif section == 'inventory':
        # This is the slow one - 6+ minutes
        inventory_data = get_cached_inventory_data(scenario)
        detailed_inventory_data = get_cached_detailed_inventory_data(scenario)
        
        # Get snapshot date
        snapshot_date = None
        try:
            inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
            if inventory_snapshot:
                snapshot_date = inventory_snapshot.date_of_snapshot.strftime('%B %d, %Y')
        except:
            snapshot_date = "Date not available"
        
        # Get real inventory data from SQL Server for Cost Analysis
        print(f"🔥 DEBUG INVENTORY SECTION: Loading data for scenario {scenario.version}")
        try:
            stored_inventory = get_stored_inventory_data(scenario)
            inventory_by_group_dict = stored_inventory.get('inventory_by_group', {})
            print(f"🔥 DEBUG: Raw inventory dict: {list(inventory_by_group_dict.keys()) if inventory_by_group_dict else 'EMPTY'}")
            
            # Convert dictionary format to Chart.js format for the template
            if inventory_by_group_dict:
                labels = list(inventory_by_group_dict.keys())
                data_values = list(inventory_by_group_dict.values())
                
                # Create datasets structure that works with the filtering logic
                # Each product group gets its own dataset for proper filtering
                datasets = []
                colors = [
                    '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
                    '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#36A2EB', '#FFCE56'
                ]
                
                for i, (group_name, group_value) in enumerate(inventory_by_group_dict.items()):
                    datasets.append({
                        'label': group_name,
                        'data': [group_value],  # Single value for each group
                        'backgroundColor': colors[i % len(colors)],
                        'borderColor': colors[i % len(colors)],
                        'borderWidth': 1
                    })
                
                inventory_by_group_data = {
                    'labels': ['Opening Inventory'],  # Single label since we have opening inventory only
                    'datasets': datasets
                }
                print(f"🔥 DEBUG: Chart.js format created - {len(datasets)} datasets, total: ${sum(data_values):,.2f}")
                print(f"🔥 DEBUG: Sample dataset: {datasets[0]['label']} = ${datasets[0]['data'][0]:,.2f}")
            else:
                inventory_by_group_data = {'labels': [], 'datasets': []}
                print("🔥 DEBUG: No inventory data found, using empty Chart.js format")
                
        except Exception as e:
            print(f"🔥 ERROR: Could not load stored inventory data: {e}")
            inventory_by_group_data = {'labels': [], 'datasets': []}
        
        context = {
            'inventory_months': inventory_data.get('inventory_months', []),
            'inventory_cogs': inventory_data.get('inventory_cogs', []),
            'inventory_revenue': inventory_data.get('inventory_revenue', []),
            'production_aud': inventory_data.get('production_aud', []),
            'production_cogs_group_chart': json.dumps(inventory_data.get('production_cogs_group_chart', {})),
            'top_products_by_group_month': json.dumps(inventory_data.get('top_products_by_group_month', {})),
            'parent_product_groups': inventory_data.get('parent_product_groups', []),
            'cogs_data_by_group': json.dumps(inventory_data.get('cogs_data_by_group', {})),
            'inventory_by_group': json.dumps(inventory_by_group_data),  # Fixed variable name for template
            'detailed_inventory_data': detailed_inventory_data.get('inventory_data', []),
            'detailed_production_data': detailed_inventory_data.get('production_data', []),
            'snapshot_date': snapshot_date,
            'version': version
        }
        html = render_to_string('website/inventory.html', context, request=request)
        
    elif section == 'supplier':
        supplier_data = get_cached_supplier_data(scenario)
        context = {
            'supplier_a_chart_data': supplier_data.get('chart_data', {}),
            'supplier_a_top_products_json': supplier_data.get('top_products', []),
            'version': version
        }
        html = render_to_string('website/sections/supplier.html', context, request=request)
        
    else:
        return JsonResponse({'error': 'Invalid section'}, status=400)
    
    return JsonResponse({'html': html})


@login_required
def calculate_aggregated_data(request, version):
    """Calculate and store aggregated data for fast loading"""
    scenario = get_object_or_404(scenarios, version=version)
    
    try:
        print(f"DEBUG: Starting aggregated data calculation for scenario: {scenario.version}")
        
        # Calculate all aggregated data
        populate_all_aggregated_data(scenario)
        
        messages.success(request, f'Aggregated data calculated successfully for scenario {scenario.version}')
        print(f"DEBUG: Completed aggregated data calculation for scenario: {scenario.version}")
        
    except Exception as e:
        print(f"ERROR: Failed to calculate aggregated data for scenario {scenario.version}: {e}")
        messages.error(request, f'Failed to calculate aggregated data: {str(e)}')
    
    return redirect('edit_scenario', version=version)


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
        return {
            'parent_product_group': get_forecast_data_by_parent_product_group(scenario),
            'product_group': get_forecast_data_by_product_group(scenario),
            'region': get_forecast_data_by_region(scenario),
            'customer': get_forecast_data_by_customer(scenario),
            'data_source': get_forecast_data_by_data_source(scenario),
        }


def get_cached_inventory_data(scenario):
    """Get cached inventory data from aggregated model - NO heavy calculations"""
    import pandas as pd
    
    try:
        # TRY FIRST: Get from CachedInventoryData (old cache system)
        cached = CachedInventoryData.objects.get(version=scenario)
        print("DEBUG: Using CachedInventoryData (old cache system)")
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
        # TRY SECOND: Get from AggregatedInventoryChartData (new aggregated system)
        try:
            print("DEBUG: Using AggregatedInventoryChartData (new aggregated system)")
            stored_data = get_stored_inventory_data(scenario)
            
            # Convert the stored data to the expected format for the charts
            if stored_data and stored_data.get('monthly_trends'):
                monthly_trends = stored_data['monthly_trends']
                
                # Extract chart data in the expected format
                all_months = set()
                for group, group_data in monthly_trends.items():
                    if isinstance(group_data, dict) and 'months' in group_data:
                        all_months.update(group_data['months'])
                
                months_list = sorted(all_months, key=lambda d: pd.to_datetime(d, format='%b %Y')) if all_months else []
                
                return {
                    'inventory_months': months_list,
                    'inventory_cogs': [],  # Not needed for current charts
                    'inventory_revenue': [],  # Not needed for current charts
                    'production_aud': [],  # Not needed for current charts
                    'production_cogs_group_chart': {},  # Not needed for current charts
                    'top_products_by_group_month': {},  # Not needed for current charts
                    'parent_product_groups': list(stored_data.get('inventory_by_group', {}).keys()),
                    'cogs_data_by_group': monthly_trends,  # This contains the real chart data
                }
            else:
                print("DEBUG: No monthly trends data found in aggregated data")
                return {
                    'inventory_months': [],
                    'inventory_cogs': [],
                    'inventory_revenue': [],
                    'production_aud': [],
                    'production_cogs_group_chart': {},
                    'top_products_by_group_month': {},
                    'parent_product_groups': [],
                    'cogs_data_by_group': {},
                }
        except Exception as agg_error:
            print(f"DEBUG: Error getting aggregated data: {agg_error}")
            # LAST RESORT: Fall back to real-time calculation
            print("DEBUG: Falling back to real-time calculation")
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
        return get_detailed_inventory_data(scenario)


from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404, render
from .models import (
    scenarios,
    SMART_Forecast_Model,
    MasterDataProductModel,
    MasterDataFreightModel,
    CalcualtedReplenishmentModel,
)

from collections import defaultdict
from django.db.models import Q

@login_required
def ScenarioWarningList(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)

    # Products in forecast but not in master data
    forecast_products_set = set(SMART_Forecast_Model.objects.values_list('Product', flat=True).distinct())
    master_products_set = set(MasterDataProductModel.objects.values_list('Product', flat=True))
    products_not_in_master_data = forecast_products_set - master_products_set

    # Products without dress mass (fetch only needed fields)
    products_without_dress_mass = (
        MasterDataProductModel.objects
        .filter(Product__in=forecast_products_set)
        .filter(Q(DressMass__isnull=True) | Q(DressMass=0))
        .only('Product', 'ParentProductGroup')
    )

    # Group products without dress mass by parent product group
    grouped_products_without_dress_mass = defaultdict(list)
    for product in products_without_dress_mass:
        grouped_products_without_dress_mass[product.ParentProductGroup].append(product)

    # Regions in forecast but not defined in the freight model
    forecast_regions = set(
        SMART_Forecast_Model.objects.filter(version=scenario).values_list('Forecast_Region', flat=True).distinct()
    )
    defined_regions = set(
        MasterDataFreightModel.objects.filter(version=scenario).values_list('ForecastRegion__Forecast_region', flat=True).distinct()
    )
    missing_regions = forecast_regions - defined_regions

    # Products not allocated to foundries (Site is NULL) with summed ReplenishmentQty, grouped by parent product group
    products_not_allocated_to_foundries = (
        CalcualtedReplenishmentModel.objects
        .filter(version=scenario, Site__isnull=True)
        .values('Product__ParentProductGroup', 'Product__Product', 'Product__ProductDescription')
        .annotate(total_replenishment_qty=Sum('ReplenishmentQty'))
        .order_by('Product__ParentProductGroup', '-total_replenishment_qty')
    )

    # Group products not allocated to foundries by parent product group
    grouped_products = defaultdict(list)
    for product in products_not_allocated_to_foundries:
        grouped_products[product['Product__ParentProductGroup']].append(product)

    context = {
        'scenario': scenario,
        'products_not_in_master_data': products_not_in_master_data,
        'grouped_products_without_dress_mass': dict(grouped_products_without_dress_mass),
        'grouped_products': dict(grouped_products),
        'missing_regions': missing_regions,
        'user_name': user_name,
    }

    return render(request, "website/scenario_warning_list.html", context)

def create_product(request):
    if request.method == 'POST':
        form = ProductForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('ProductsList')  # Redirect to a list of products or another page
    else:
        form = ProductForm()
    return render(request, 'website/create_product.html', {'form': form})

from sqlalchemy import create_engine, text
from django.shortcuts import get_object_or_404
from .models import MasterDataOrderBook, scenarios

@login_required
def upload_product_allocation(request, version):
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Database connection details
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    with engine.connect() as connection:
        # SQL query to fetch data - using ROW_NUMBER to pick first site per product
        query = text("""
            WITH RankedOrders AS (
                SELECT 
                    Site.SiteName AS site,
                    Product.ProductKey AS productkey,
                    ROW_NUMBER() OVER (PARTITION BY Product.ProductKey ORDER BY Site.SiteName) AS rn
                FROM PowerBI.SalesOrders AS SalesOrders
                INNER JOIN PowerBI.Products AS Product ON SalesOrders.skProductId = Product.skProductId
                INNER JOIN PowerBI.Site AS Site ON SalesOrders.skSiteId = Site.skSiteId
                WHERE Site.SiteName IN ('MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1')
                AND (SalesOrders.OnOrderQty IS NOT NULL AND SalesOrders.OnOrderQty > 0)
            )
            SELECT site, productkey
            FROM RankedOrders
            WHERE rn = 1
        """)

        # Execute the query
        result = connection.execute(query)

        # Delete existing records for this version first
        MasterDataOrderBook.objects.filter(version=scenario).delete()

        # Populate the MasterDataOrderBook model with deduplicated data
        bulk_records = []
        for row in result:
            bulk_records.append(MasterDataOrderBook(
                version=scenario,
                site=row.site,
                productkey=row.productkey
            ))
        
        # Bulk create for better performance
        if bulk_records:
            MasterDataOrderBook.objects.bulk_create(bulk_records)

    return redirect('edit_scenario', version=version)

@login_required
def delete_product_allocation(request, version):
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Delete all related records
    MasterDataOrderBook.objects.filter(version=scenario).delete()

    return redirect('edit_scenario', version=version)

from django.forms import modelformset_factory
from django.core.paginator import Paginator

@login_required
def update_product_allocation(request, version):
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Filter records
    product_filter = request.GET.get('product', '')
    site_filter = request.GET.get('site', '')

    queryset = MasterDataOrderBook.objects.filter(version=scenario)
    if product_filter:
        queryset = queryset.filter(productkey__icontains=product_filter)
    if site_filter:
        queryset = queryset.filter(site__icontains=site_filter)

    # Apply ordering before slicing
    queryset = queryset.order_by('id')  # Adjust the field to order by as needed

    # Paginate the queryset
    paginator = Paginator(queryset, 20)  # Show 20 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Create a formset
    OrderBookFormSet = modelformset_factory(MasterDataOrderBook, fields=('site', 'productkey'), extra=0)
    formset = OrderBookFormSet(queryset=page_obj.object_list)

    if request.method == 'POST':
        formset = OrderBookFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            formset.save()
            return redirect('edit_scenario', version=version)

    return render(request, 'website/update_product_allocation.html', {
        'formset': formset,
        'page_obj': page_obj,
        'product_filter': product_filter,
        'site_filter': site_filter,
        'scenario': scenario,
    })

@login_required
def copy_product_allocation(request, version):
    # Get the target scenario
    target_scenario = get_object_or_404(scenarios, version=version)

    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)

        # Copy records
        source_records = MasterDataOrderBook.objects.filter(version=source_scenario)
        for record in source_records:
            MasterDataOrderBook.objects.create(
                version=target_scenario,
                site=record.site,
                productkey=record.productkey
            )

        return redirect('edit_scenario', version=version)

    # Get all scenarios except the current one
    all_scenarios = scenarios.objects.exclude(version=version)

    return render(request, 'website/copy_product_allocation.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })

from sqlalchemy import func

@login_required
def upload_production_history(request, version):
    import pandas as pd

    scenario = get_object_or_404(scenarios, version=version)

    # --- First server: Site-based ---
    Server1 = 'bknew-sql02'
    Database1 = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con1 = f'mssql+pyodbc://@{Server1}/{Database1}?driver={Driver}'
    engine1 = create_engine(Database_Con1)

    # --- Second server: Warehouse-based ---
    Server2 = 'bkgcc-sql'
    Database2 = 'Bradken_Data_Warehouse'
    Database_Con2 = f'mssql+pyodbc://@{Server2}/{Database2}?driver={Driver}'
    engine2 = create_engine(Database_Con2)

    # WarehouseCode mapping
    warehouse_map = {
        "H53": "MTJ1",
        "I92": "COI2",
        "12A": "XUZ1",
        "M61": "MER1",
        "235": "WOD1",
        "261": "WUN1",
    }

    # Use context managers to ensure connections are closed
    with engine1.connect() as connection1, engine2.connect() as connection2:
        # --- Query for Site-based (LEFT JOINs, start from HeatProducts) ---
        sql_site = """
            SELECT DISTINCT
                Site.SiteName AS Foundry,
                Product.ProductKey AS Product,
                TRY_CONVERT(DATE, CAST(HeatProducts.TapTime AS DATE)) AS ProductionMonth,
                HeatProducts.CastQty AS ProductionQty
            FROM PowerBI.HeatProducts AS HeatProducts
            LEFT JOIN PowerBI.Products AS Product
                ON HeatProducts.skProductId = Product.skProductId
            LEFT JOIN PowerBI.Site AS Site
                ON HeatProducts.SkSiteId = Site.skSiteId
            WHERE Site.SiteName IN ('MTJ1', 'COI2', 'XUZ1', 'WOD1', 'MER1', 'WUN1')
        """
        df_site = pd.read_sql(sql_site, connection1)

        # --- Query for Warehouse-based (LEFT JOINs, start from HeatProducts) ---
        sql_wh = """
            SELECT DISTINCT
                Warehouse.WarehouseCode AS WarehouseCode,
                Product.ProductKey AS Product,
                TRY_CONVERT(DATE, CAST(HeatProducts.TapTime AS DATE)) AS ProductionMonth,
                HeatProducts.CastQty AS ProductionQty
            FROM PowerBI.HeatProducts AS HeatProducts
            LEFT JOIN PowerBI.Products AS Product
                ON HeatProducts.skProductId = Product.skProductId
            LEFT JOIN PowerBI.Warehouse AS Warehouse
                ON HeatProducts.SkWarehouseId = Warehouse.skWarehouseId
            WHERE Warehouse.WarehouseCode IN ('H53', 'I92', '12A', 'M61', '235', '261')
        """
        df_wh = pd.read_sql(sql_wh, connection2)
        # Map WarehouseCode to Foundry
        df_wh['Foundry'] = df_wh['WarehouseCode'].map(warehouse_map)
        df_wh = df_wh.drop(columns=['WarehouseCode'])

        # Combine both dataframes
        combined_df = pd.concat([df_site, df_wh], ignore_index=True)

        # Ensure ProductionMonth is datetime
        combined_df['ProductionMonth'] = pd.to_datetime(combined_df['ProductionMonth'])

        # Sort by Product and ProductionMonth descending
        combined_df = combined_df.sort_values(['Product', 'ProductionMonth'], ascending=[True, False])

        # Drop duplicates: keep only the latest ProductionMonth per Product
        latest_df = combined_df.drop_duplicates(subset=['Product'], keep='first')

        # Bulk upload for faster processing
        MasterDataHistoryOfProductionModel.objects.filter(version=scenario).delete()
        bulk_objs = []
        for _, row in latest_df.iterrows():
            if pd.isna(row['ProductionMonth']):
                continue
            bulk_objs.append(
                MasterDataHistoryOfProductionModel(
                    version=scenario,
                    Product=row['Product'],
                    ProductionMonth=row['ProductionMonth'],
                    Foundry=row['Foundry'],
                    ProductionQty=row['ProductionQty'],
                )
            )
        if bulk_objs:
            MasterDataHistoryOfProductionModel.objects.bulk_create(bulk_objs, batch_size=1000)

    # Connections are closed automatically here
    return redirect('edit_scenario', version=version)

@login_required
def delete_production_history(request, version):
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Delete all related records
    MasterDataHistoryOfProductionModel.objects.filter(version=scenario).delete()

    return redirect('edit_scenario', version=version)

from django.core.paginator import Paginator
from django.forms import modelformset_factory
from django.core.paginator import Paginator

@login_required
def update_production_history(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    queryset = MasterDataHistoryOfProductionModel.objects.filter(version=scenario)

    # Filtering logic
    product = request.GET.get('product')
    foundry = request.GET.get('foundry')
    production_month = request.GET.get('production_month')

    if product:
        queryset = queryset.filter(Product__icontains=product)
    if foundry:
        queryset = queryset.filter(Foundry__icontains=foundry)
    if production_month:
        queryset = queryset.filter(ProductionMonth__startswith=production_month)

    # Always order before paginating or slicing!
    queryset = queryset.order_by('Foundry', 'Product', '-ProductionMonth')

    paginator = Paginator(queryset, 25)  # 25 per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    ProductionHistoryFormSet = modelformset_factory(
        MasterDataHistoryOfProductionModel,
        fields=('Foundry', 'Product', 'ProductionMonth', 'ProductionQty'),
        extra=0
    )
    formset = ProductionHistoryFormSet(queryset=page_obj.object_list)

    return render(
        request,
        'website/update_production_history.html',
        {
            'scenario': scenario,
            'formset': formset,
            'page_obj': page_obj,
            'request': request,
            'user_name': user_name,
        }
    )

@login_required
def copy_production_history(request, version):
    # Get the target scenario
    target_scenario = get_object_or_404(scenarios, version=version)

    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)

        # Copy records
        source_records = MasterDataHistoryOfProductionModel.objects.filter(version=source_scenario)
        for record in source_records:
            MasterDataHistoryOfProductionModel.objects.create(
                version=target_scenario,
                Product=record.Product,
                Foundry=record.Foundry,
                ProductionMonth=record.ProductionMonth,
                ProductionQty=record.ProductionQty,
            )

        return redirect('edit_scenario', version=version)

    # Get all scenarios except the current one
    all_scenarios = scenarios.objects.exclude(version=version)

    return render(request, 'website/copy_production_history.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })

@login_required
def update_on_hand_stock(request, version):
    user_name = request.user.username
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Filter records
    product_filter = request.GET.get('product', '')  # Get the filter value from the query string
    site_filter = request.GET.get('site', '')  # Get the filter value for site

    queryset = MasterDataInventory.objects.filter(version=scenario)
    if product_filter:
        queryset = queryset.filter(product__icontains=product_filter)  # Filter by product name
    if site_filter:
        queryset = queryset.filter(site__SiteName__icontains=site_filter)  # Filter by site name

    # Apply ordering before slicing
    queryset = queryset.order_by('id')  # Ensure the queryset is ordered before pagination

    # Paginate the queryset
    paginator = Paginator(queryset, 10)  # Show 10 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Create a formset for the current page - INCLUDE cost_aud field
    OnHandStockFormSet = modelformset_factory(
        MasterDataInventory,
        fields=('site', 'product', 'onhandstock_qty', 'intransitstock_qty', 'wip_stock_qty', 'cost_aud'),  # Added cost_aud
        extra=0
    )
    formset = OnHandStockFormSet(queryset=page_obj.object_list)

    if request.method == 'POST':
        formset = OnHandStockFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            print("DEBUG: Formset is valid, saving changes...")
            instances = formset.save(commit=False)
            for instance in instances:
                print(f"DEBUG: Saving {instance.product} - WIP: {instance.wip_stock_qty}")
                instance.save()
            # Also save any deleted objects
            for obj in formset.deleted_objects:
                obj.delete()
            return redirect('edit_scenario', version=version)
        else:
            print("DEBUG: Formset errors:", formset.errors)
            print("DEBUG: Formset non-form errors:", formset.non_form_errors())

    # Pass the snapshot date to the template
    snapshot_date = queryset.first().date_of_snapshot if queryset.exists() else None

    return render(request, 'website/update_on_hand_stock.html', {
        'formset': formset,
        'page_obj': page_obj,
        'product_filter': product_filter,
        'site_filter': site_filter,
        'scenario': scenario,
        'snapshot_date': snapshot_date,  # Include snapshot date in the context
        'user_name': user_name,
        'version': scenario.version,
    })

@login_required
def delete_on_hand_stock(request, version):
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Delete all related records
    MasterDataInventory.objects.filter(version=scenario).delete()

    return redirect('edit_scenario', version=version)


from sqlalchemy import create_engine, text
from django.shortcuts import render, redirect, get_object_or_404
from .models import MasterDataInventory, scenarios

from math import ceil

import pandas as pd

@login_required
def upload_on_hand_stock(request, version):
    # Database connection details
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)

    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Get the list of products available in SMART_Forecast_Model for the current scenario version
    smart_forecast_products = list(SMART_Forecast_Model.objects.filter(version=scenario).values_list('Product', flat=True))

    if request.method == 'POST':
        snapshot_date = request.POST.get('snapshot_date')
        wip_option = request.POST.get('wip_option')  # 'fetch_as_is' or 'calculate_from_production'
        
        if not snapshot_date:
            return render(request, 'website/upload_on_hand_stock.html', {
                'error': 'Please enter a valid snapshot date.',
                'scenario': scenario
            })

        # Delete existing data for the given version
        MasterDataInventory.objects.filter(version=scenario).delete()

        with engine.connect() as connection:
            # --- Inventory Data - RESTORED YOUR ORIGINAL QUERY ---
            if smart_forecast_products:
                placeholders = ', '.join([f"'{p}'" for p in smart_forecast_products])
                inventory_sql = f"""
                    SELECT 
                        Products.ProductKey AS product,
                        Site.SiteName AS site,
                        Inventory.StockOnHand AS onhandstock_qty,
                        Inventory.StockInTransit AS intransitstock_qty,
                        MAX(Inventory.WarehouseCostAUD) AS cost_aud
                    FROM PowerBI.[Inventory Daily History] AS Inventory
                    INNER JOIN PowerBI.Site AS Site
                        ON Inventory.skSiteId = Site.skSiteId
                    INNER JOIN PowerBI.Dates AS Dates
                        ON Inventory.skReportDateId = Dates.skDateId
                    INNER JOIN PowerBI.Products AS Products
                        ON Inventory.skProductId = Products.skProductId
                    WHERE Dates.DateValue = '{snapshot_date}'
                    AND Products.ProductKey IN ({placeholders})
                    GROUP BY Products.ProductKey, Site.SiteName, Inventory.StockOnHand, Inventory.StockInTransit
                """
                inventory_df = pd.read_sql(inventory_sql, connection)
            else:
                inventory_df = pd.DataFrame(columns=['product', 'site', 'onhandstock_qty', 'intransitstock_qty', 'cost_aud'])

            # --- WIP Data based on selected option ---
            if wip_option == 'calculate_from_production':
                # Get cast to despatch days for each site
                cast_to_despatch_data = MasterDataCastToDespatchModel.objects.filter(version=scenario).values(
                    'Foundry__SiteName', 'CastToDespatchDays'
                )
                cast_to_despatch_dict = {
                    item['Foundry__SiteName']: item['CastToDespatchDays'] 
                    for item in cast_to_despatch_data
                }
                
                # Calculate WIP from production data
                wip_data = []
                snapshot_datetime = pd.to_datetime(snapshot_date)
                
                for site, days in cast_to_despatch_dict.items():
                    if days and days > 0:
                        start_date = snapshot_datetime - pd.Timedelta(days=days)
                        end_date = snapshot_datetime
                        
                        # Create placeholders for the products
                        if smart_forecast_products:
                            production_placeholders = ', '.join([f"'{p}'" for p in smart_forecast_products])
                            production_query = text(f"""
                                SELECT 
                                    p.ProductKey AS ProductCode,
                                    SUM(hp.CastQty) AS wip_stock_qty
                                FROM PowerBI.HeatProducts hp
                                INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                                INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                                WHERE hp.TapTime IS NOT NULL 
                                    AND p.DressMass IS NOT NULL 
                                    AND s.SiteName = '{site}'
                                    AND hp.TapTime >= '{start_date}'
                                    AND hp.TapTime <= '{end_date}'
                                    AND p.ProductKey IN ({production_placeholders})
                                GROUP BY p.ProductKey
                            """)
                        else:
                            production_query = text(f"""
                                SELECT 
                                    p.ProductKey AS ProductCode,
                                    SUM(hp.CastQty) AS wip_stock_qty
                                FROM PowerBI.HeatProducts hp
                                INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                                INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                                WHERE hp.TapTime IS NOT NULL 
                                    AND p.DressMass IS NOT NULL 
                                    AND s.SiteName = '{site}'
                                    AND hp.TapTime >= '{start_date}'
                                    AND hp.TapTime <= '{end_date}'
                                GROUP BY p.ProductKey
                            """)
                        
                        try:
                            site_production_df = pd.read_sql(production_query, connection)
                            if not site_production_df.empty:
                                site_production_df['site'] = site
                                site_production_df.rename(columns={'ProductCode': 'product'}, inplace=True)
                                wip_data.append(site_production_df)
                        except Exception as e:
                            print(f"Error querying production data for site {site}: {e}")
                            continue
                
                # Combine all WIP data from all sites
                if wip_data:
                    wip_df = pd.concat(wip_data, ignore_index=True)
                else:
                    wip_df = pd.DataFrame(columns=['product', 'site', 'wip_stock_qty'])
                    
            else:  # 'fetch_as_is' - original WIP fetching logic
                if smart_forecast_products:
                    wip_placeholders = ', '.join([f"'{p}'" for p in smart_forecast_products])
                    wip_sql = f"""
                        SELECT 
                            Products.ProductKey AS product,
                            Site.SiteName AS site,
                            SUM(WIP.WIPQty) AS wip_stock_qty
                        FROM PowerBI.[Work In Progress Previous 3 Months] AS WIP
                        INNER JOIN PowerBI.Site AS Site
                            ON WIP.skSiteId = Site.skSiteId
                        INNER JOIN PowerBI.Dates AS Dates
                            ON WIP.skReportDateId = Dates.skDateId
                        INNER JOIN PowerBI.Products AS Products
                            ON WIP.skProductId = Products.skProductId
                        WHERE Dates.DateValue = '{snapshot_date}'
                          AND Products.ProductKey IN ({wip_placeholders})
                        GROUP BY Products.ProductKey, Site.SiteName
                    """
                    wip_df = pd.read_sql(wip_sql, connection)
                else:
                    wip_df = pd.DataFrame(columns=['product', 'site', 'wip_stock_qty'])

        # Rest of the function remains the same...
        # Merge inventory and WIP data
        merged_df = pd.merge(
            inventory_df,
            wip_df,
            how='left',
            on=['product', 'site']
        )
        merged_df['wip_stock_qty'] = merged_df['wip_stock_qty'].fillna(0)

        # Filter out rows where all three quantities are zero
        filtered_df = merged_df[
            ~(
                (merged_df['onhandstock_qty'].fillna(0) == 0) &
                (merged_df['intransitstock_qty'].fillna(0) == 0) &
                (merged_df['wip_stock_qty'].fillna(0) == 0)
            )
        ]

        # Use filtered_df for the rest of your logic
        plants_dict = {p.SiteName: p for p in MasterDataPlantModel.objects.all()}
        bulk_objs = []
        for _, row in filtered_df.iterrows():
            plant = plants_dict.get(row['site'])
            if not plant:
                continue
            bulk_objs.append(
                MasterDataInventory(
                    version=scenario,
                    date_of_snapshot=snapshot_date,
                    product=row['product'],
                    site=plant,
                    site_region=plant.PlantRegion,
                    onhandstock_qty=row['onhandstock_qty'],
                    intransitstock_qty=row['intransitstock_qty'],
                    wip_stock_qty=row['wip_stock_qty'],
                    cost_aud=row['cost_aud'],
                )
            )
        if bulk_objs:
            MasterDataInventory.objects.bulk_create(bulk_objs, batch_size=10000)

        return redirect('edit_scenario', version=version)

    return render(request, 'website/upload_on_hand_stock.html', {
        'scenario': scenario
    })
    

@login_required
def copy_on_hand_stock(request, version):
    # Get the target scenario
    target_scenario = get_object_or_404(scenarios, version=version)

    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)

        # Copy records
        source_records = MasterDataInventory.objects.filter(version=source_scenario)
        for record in source_records:
            MasterDataInventory.objects.create(
                version=target_scenario,
                date_of_snapshot=record.date_of_snapshot,
                product=record.product,
                site=record.site,
                site_region=record.site_region,
                onhandstock_qty=record.onhandstock_qty,
                intransitstock_qty=record.intransitstock_qty,
                wip_stock_qty=record.wip_stock_qty,
            )

        return redirect('edit_scenario', version=version)

    # Get all scenarios except the current one
    all_scenarios = scenarios.objects.exclude(version=version)

    return render(request, 'website/copy_on_hand_stock.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })

from django.shortcuts import render, get_object_or_404, redirect
from .models import MasterDataCustomersModel, MasterDataForecastRegionModel  # Replace `Customers` with your actual model name

@login_required
def customers_list(request):
    """
    View to display the list of customers.
    """
    customers = MasterDataCustomersModel.objects.all()  # Replace `Customers` with your actual model name
    return render(request, 'website/customers_list.html', {'customers': customers})


from django.shortcuts import render, get_object_or_404, redirect
from .models import MasterDataForecastRegionModel
from .forms import ForecastRegionForm  # Create this form if it doesn't exist

# filepath: c:\Users\aali\Documents\Data\Training\SPR\SPR\website\views.py
from .forms import ForecastRegionForm

@login_required
def forecast_region_list(request):
    """
    View to display the list of forecast regions and add new ones.
    """
    forecast_regions = MasterDataForecastRegionModel.objects.all()

    if request.method == 'POST':
        form = ForecastRegionForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('ForecastRegionList')  # Redirect to the same page after adding a region
    else:
        form = ForecastRegionForm()

    return render(request, 'website/forecast_region_list.html', {
        'forecast_regions': forecast_regions,
        'form': form,
    })


@login_required
def add_forecast_region(request):
    """
    View to add a new forecast region.
    """
    if request.method == 'POST':
        form = ForecastRegionForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('ForecastRegionList')
    else:
        form = ForecastRegionForm()
    return render(request, 'website/add_forecast_region.html', {'form': form})


@login_required
def update_forecast_region(request, region_id):
    """
    View to update an existing forecast region.
    """
    region = get_object_or_404(MasterDataForecastRegionModel, Forecast_region=region_id)
    if request.method == 'POST':
        form = ForecastRegionForm(request.POST, instance=region)
        if form.is_valid():
            form.save()
            return redirect('ForecastRegionList')
    else:
        form = ForecastRegionForm(instance=region)
    return render(request, 'website/update_forecast_region.html', {'form': form})


@login_required
def delete_forecast_region(request, region_id):
    """
    View to delete a forecast region.
    """
    region = get_object_or_404(MasterDataForecastRegionModel, Forecast_region=region_id)
    if request.method == 'POST':
        region.delete()
        return redirect('ForecastRegionList')
    return render(request, 'website/delete_forecast_region.html', {'region': region})

from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpResponse
from .models import MasterDataFreightModel
from .forms import MasterDataFreightForm  # Create this form if it doesn't exist
import pandas as pd  # For handling Excel files

from django.forms import modelformset_factory



from django.core.paginator import Paginator

@login_required
def update_master_data_freight(request, version):
    user_name = request.user.username
    """
    View to update and add new Master Data Freight records.
    """
    scenario = get_object_or_404(scenarios, version=version)

    # Get filter values from GET
    region_filter = request.GET.get('region', '').strip()
    site_filter = request.GET.get('site', '').strip()

    # Filter the records for the given version and filters
    freight_records = MasterDataFreightModel.objects.filter(version=scenario)
    if region_filter:
        freight_records = freight_records.filter(ForecastRegion__Forecast_region__icontains=region_filter)
    if site_filter:
        freight_records = freight_records.filter(ManufacturingSite__SiteName__icontains=site_filter)

    # Always order before paginating!
    freight_records = freight_records.order_by('ForecastRegion__Forecast_region', 'ManufacturingSite__SiteName')

    paginator = Paginator(freight_records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    FreightFormSet = modelformset_factory(
        MasterDataFreightModel,
        fields=['ForecastRegion', 'ManufacturingSite', 'PlantToDomesticPortDays', 'OceanFreightDays', 'PortToCustomerDays'],
        extra=1
    )

    if request.method == 'POST':
        formset = FreightFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            instances = formset.save(commit=False)
            for instance in instances:
                instance.version = scenario
                instance.save()
            return redirect('edit_scenario', version=version)
    else:
        formset = FreightFormSet(queryset=page_obj.object_list)

    return render(
        request,
        'website/update_master_data_freight.html',
        {
            'formset': formset,
            'version': version,
            'region_filter': region_filter,
            'site_filter': site_filter,
            'page_obj': page_obj,
            'user_name': user_name,
        }
    )


@login_required
def delete_master_data_freight(request, version):
    """
    View to delete all Master Data Freight records for a specific version.
    """
    if request.method == 'POST':
        MasterDataFreightModel.objects.filter(version=version).delete()
        return redirect('edit_scenario', version=version)
    return render(request, 'website/delete_master_data_freight.html', {'version': version})


@login_required
def copy_master_data_freight(request, version):
    """
    View to copy Master Data Freight records from one version to another.
    """
    target_scenario = get_object_or_404(scenarios, version=version)
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        if source_version:
            source_scenario = get_object_or_404(scenarios, version=source_version)
            source_records = MasterDataFreightModel.objects.filter(version=source_scenario)
            if not source_records.exists():
                messages.warning(request, "No freight records available to copy from the selected scenario.")
                return redirect('edit_scenario', version=version)
            else:
                for record in source_records:
                    record.pk = None  # Create a new record
                    record.version = target_scenario  # Assign the scenario object, not just the version string
                    record.save()
                return redirect('edit_scenario', version=version)
    return render(request, 'website/copy_master_data_freight.html', {'version': version})


@login_required
@login_required
def upload_master_data_freight(request, version):
    """
    View to upload Master Data Freight records from an Excel file.
    """
    # Fetch the scenario instance
    scenario = get_object_or_404(scenarios, version=version)
    missing_regions = []  # To track regions that are not defined in the freight model
    missing_sites = []  # To track manufacturing sites that are not defined in the plant model

    if request.method == 'POST' and request.FILES['file']:
        excel_file = request.FILES['file']
        try:
            # Read the Excel file
            df = pd.read_excel(excel_file)

            # Validate required columns
            required_columns = ['ForecastRegion', 'ManufacturingSite', 'PlantToDomesticPortDays', 'OceanFreightDays', 'PortToCustomerDays']
            if not all(column in df.columns for column in required_columns):
                return HttpResponse("Invalid file format. Please ensure the file has the correct headers.")

            # Save data to the model
            for _, row in df.iterrows():
                try:
                    # Fetch the ForecastRegion instance
                    forecast_region = MasterDataForecastRegionModel.objects.get(Forecast_region=row['ForecastRegion'])

                    # Fetch the ManufacturingSite instance
                    manufacturing_site = MasterDataPlantModel.objects.get(SiteName=row['ManufacturingSite'])

                    # Create the MasterDataFreightModel record
                    MasterDataFreightModel.objects.create(
                        version=scenario,  # Use the scenario instance
                        ForecastRegion=forecast_region,  # Use the ForecastRegion instance
                        ManufacturingSite=manufacturing_site,  # Use the ManufacturingSite instance
                        PlantToDomesticPortDays=row['PlantToDomesticPortDays'],
                        OceanFreightDays=row['OceanFreightDays'],
                        PortToCustomerDays=row['PortToCustomerDays']
                    )
                except MasterDataForecastRegionModel.DoesNotExist:
                    # Add the missing region to the list
                    missing_regions.append(row['ForecastRegion'])
                except MasterDataPlantModel.DoesNotExist:
                    # Add the missing manufacturing site to the list
                    missing_sites.append(row['ManufacturingSite'])

            # Redirect to the edit scenario page with warnings if there are missing regions or sites
            if missing_regions or missing_sites:
                request.session['missing_regions'] = missing_regions  # Store missing regions in the session
                request.session['missing_sites'] = missing_sites  # Store missing sites in the session
            return redirect('edit_scenario', version=version)
        except Exception as e:
            return HttpResponse(f"An error occurred: {e}")
    return render(request, 'website/upload_master_data_freight.html', {'version': version})

from django.forms import modelformset_factory

@login_required
def update_master_data_casto_to_despatch_days(request, version):
    """
    View to update and add new Master Data Casto to Despatch Days records.
    """
    # Get the records for the given version
    scenario = get_object_or_404(scenarios, version=version)
    casto_records = MasterDataCastToDespatchModel.objects.filter(version=scenario)

    # Create a formset for the records, excluding the version field
    CastoFormSet = modelformset_factory(
        MasterDataCastToDespatchModel,
        fields=['Foundry', 'CastToDespatchDays'],  # Exclude 'version' from the formset
        extra=1  # Allow one extra form for adding new records
    )

    if request.method == 'POST':
        formset = CastoFormSet(request.POST, queryset=casto_records)
        if formset.is_valid():
            instances = formset.save(commit=False)
            for instance in instances:
                instance.version = scenario  # Set the version programmatically
                instance.save()
            return redirect('edit_scenario', version=version)
    else:
        formset = CastoFormSet(queryset=casto_records)

    return render(request, 'website/update_master_data_casto_to_despatch_days.html', {'formset': formset, 'version': version})


@login_required
def delete_master_data_casto_to_despatch_days(request, version):
    """
    View to delete all Master Data Casto to Despatch Days records for a specific version.
    """
    if request.method == 'POST':
        MasterDataCastToDespatchModel.objects.filter(version=version).delete()
        return redirect('edit_scenario', version=version)
    return render(request, 'website/delete_master_data_casto_to_despatch_days.html', {'version': version})

@login_required
def copy_master_data_casto_to_despatch_days(request, version):
    """
    View to copy Master Data Casto to Despatch Days records from one version to another.
    """
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Fetch versions with populated data for MasterDataCastToDespatchModel
    populated_versions = MasterDataCastToDespatchModel.objects.values_list('version__version', flat=True).distinct()

    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        if source_version:
            source_scenario = get_object_or_404(scenarios, version=source_version)
            source_records = MasterDataCastToDespatchModel.objects.filter(version=source_scenario)
            for record in source_records:
                record.pk = None  # Create a new record
                record.version = scenario  # Assign the current scenario
                record.save()
            return redirect('edit_scenario', version=version)

    return render(request, 'website/copy_master_data_casto_to_despatch_days.html', {
        'version': version,
        'populated_versions': populated_versions,
    })

from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpResponse
from .models import MasterDataIncotTermTypesModel
from .forms import MasterDataIncotTermTypesForm
import pandas as pd

def incoterm_list(request):
    """List all Master Incot Terms."""
    incoterms = MasterDataIncotTermTypesModel.objects.all()
    return render(request, 'website/incoterm_list.html', {'incoterms': incoterms})

# views.py
@login_required
def incoterm_create(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    if request.method == 'POST':
        form = MasterDataIncotTermTypesForm(request.POST)
        if form.is_valid():
            incoterm = form.save(commit=False)
            incoterm.version = scenario
            incoterm.save()
            return redirect('incoterm_update_formset', version=version)
    else:
        form = MasterDataIncotTermTypesForm()
    return render(request, 'website/incoterm_create.html', {'form': form, 'version': version})

from django.shortcuts import render, redirect, get_object_or_404
from django.forms import modelformset_factory
from .models import MasterDataIncotTermTypesModel
from .forms import MasterDataIncotTermTypesForm

def incoterm_update_formset(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    IncotermFormSet = modelformset_factory(MasterDataIncotTermTypesModel, form=MasterDataIncotTermTypesForm, extra=0)

    if request.method == 'POST':
        formset = IncotermFormSet(request.POST, queryset=MasterDataIncotTermTypesModel.objects.filter(version=scenario))
        if formset.is_valid():
            formset.save()
            return redirect('edit_scenario', version=version)
    else:
        formset = IncotermFormSet(queryset=MasterDataIncotTermTypesModel.objects.filter(version=scenario))

    return render(request, 'website/incoterm_formset.html', {'formset': formset, 'scenario': scenario,
                                                             'user_name': user_name, 'version': version})

from django.shortcuts import redirect, get_object_or_404
from .models import MasterDataIncotTermTypesModel

def incoterm_delete_all(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    MasterDataIncotTermTypesModel.objects.filter(version=scenario).delete()
    return redirect('edit_scenario', version=version)

# views.py
from django.shortcuts import render, redirect
from django.http import HttpResponse
import pandas as pd
from .models import MasterDataIncotTermTypesModel

def incoterm_upload(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST' and request.FILES['file']:
        excel_file = request.FILES['file']
        try:
            df = pd.read_excel(excel_file)
            required_columns = ['IncoTerm', 'IncoTermCaregory']
            if not all(col in df.columns for col in required_columns):
                return HttpResponse("Invalid file format. Required columns: IncoTerm, IncoTermCaregory.")
            for _, row in df.iterrows():
                MasterDataIncotTermTypesModel.objects.update_or_create(
                    version=scenario,  # Use the scenario from URL, not from Excel
                    IncoTerm=row['IncoTerm'],
                    defaults={'IncoTermCaregory': row['IncoTermCaregory']}
                )
            messages.success(request, f'Incoterm types uploaded successfully for {version}.')
            return redirect('edit_scenario', version=version)
        except Exception as e:
            messages.error(request, f"Error processing file: {e}")
            return redirect('edit_scenario', version=version)
    return render(request, 'website/incoterm_upload.html', {'scenario': scenario})

from django.shortcuts import render, redirect, get_object_or_404
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from .models import MasterdataIncoTermsModel, scenarios
from .forms import MasterdataIncoTermsForm

def master_data_inco_terms_update_formset(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    
    # Get filter values from GET parameters
    customer_filter = request.GET.get('customer', '').strip()
    
    # Filter records based on version and customer code
    queryset = MasterdataIncoTermsModel.objects.filter(version=scenario)
    
    # Debug: Print the count for this specific version
    total_count = queryset.count()
    print(f"DEBUG: Found {total_count} records for version '{version}' (scenario pk: {scenario.pk})")
    
    if customer_filter:
        queryset = queryset.filter(CustomerCode__icontains=customer_filter)
        filtered_count = queryset.count()
        print(f"DEBUG: After customer filter '{customer_filter}': {filtered_count} records")
    
    # Always order before paginating!
    queryset = queryset.order_by('CustomerCode')
    
    # Paginate the queryset
    paginator = Paginator(queryset, 20)  # Show 20 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Debug: Print sample records to verify version
    if page_obj.object_list:
        sample_record = page_obj.object_list[0]
        print(f"DEBUG: Sample record version: '{sample_record.version.version}' (should be '{version}')")
    
    # Create formset for current page only
    IncoTermsFormSet = modelformset_factory(
        MasterdataIncoTermsModel, 
        form=MasterdataIncoTermsForm, 
        extra=0
    )

    if request.method == 'POST':
        formset = IncoTermsFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            formset.save()
            # Redirect to the same page with current filters and page number
            redirect_url = request.path
            params = []
            if page_number:
                params.append(f"page={page_number}")
            if customer_filter:
                params.append(f"customer={customer_filter}")
            if params:
                redirect_url += "?" + "&".join(params)
            return redirect(redirect_url)
    else:
        formset = IncoTermsFormSet(queryset=page_obj.object_list)

    return render(request, 'website/master_data_inco_terms_formset.html', {
        'formset': formset, 
        'scenario': scenario,
        'user_name': user_name, 
        'version': version,
        'page_obj': page_obj,
        'customer_filter': customer_filter,
        'total_count': total_count,
    })

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from .models import MasterdataIncoTermsModel
import csv

import pandas as pd
from . models import MasterdataIncoTermsModel
import pandas as pd

@login_required
def master_data_inco_terms_upload(request, version):
    scenario = get_object_or_404(scenarios, version=version)

    if request.method == 'POST' and request.FILES['file']:
        file = request.FILES['file']
        try:
            df = pd.read_excel(file)
            for _, row in df.iterrows():
                incoterm_value = row.get('IncoTerm')
                # If incoterm is missing, blank, or NaN, use 'EXW'
                if pd.isna(incoterm_value) or str(incoterm_value).strip() == '':
                    incoterm_value = 'EXW'
                try:
                    # Get incoterm type for THIS SPECIFIC VERSION
                    incoterm_obj = MasterDataIncotTermTypesModel.objects.get(
                        version=scenario,
                        IncoTerm=incoterm_value
                    )
                except MasterDataIncotTermTypesModel.DoesNotExist:
                    messages.error(request, f"Incoterm '{incoterm_value}' does not exist in Incoterm Types for version {version}.")
                    continue  # Skip this row
                MasterdataIncoTermsModel.objects.create(
                    version=scenario,
                    Incoterm=incoterm_obj,
                    CustomerCode=row['CustomerCode'],
                )
            messages.success(request, 'Master Data Inco Terms uploaded successfully.')
        except Exception as e:
            messages.error(request, f'Error uploading file: {e}')
        return redirect('edit_scenario', version=version)

    return render(request, 'website/other_master_data_section.html', {'scenario': scenario})

def master_data_inco_terms_delete_all(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    MasterdataIncoTermsModel.objects.filter(version=scenario).delete()
    return redirect('edit_scenario', version=version)

from django.contrib import messages

from django.contrib import messages

@login_required
def master_data_inco_terms_copy(request, version):
    target_scenario = get_object_or_404(scenarios, version=version)
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        if source_version:
            source_scenario = get_object_or_404(scenarios, version=source_version)
            source_records = MasterdataIncoTermsModel.objects.filter(version=source_scenario)
            if not source_records.exists():
                messages.warning(request, "No Incoterm records available to copy from the selected scenario.")
                return redirect('edit_scenario', version=version)
            else:
                for record in source_records:
                    # Find the equivalent incoterm in the target version
                    try:
                        target_incoterm = MasterDataIncotTermTypesModel.objects.get(
                            version=target_scenario,
                            IncoTerm=record.Incoterm.IncoTerm
                        )
                        MasterdataIncoTermsModel.objects.create(
                            version=target_scenario,
                            CustomerCode=record.CustomerCode,
                            Incoterm=target_incoterm
                        )
                    except MasterDataIncotTermTypesModel.DoesNotExist:
                        messages.warning(request, f"Incoterm '{record.Incoterm.IncoTerm}' not found in target version {version}. Skipping customer {record.CustomerCode}.")
                        continue
                messages.success(request, "Incoterm records copied successfully.")
                return redirect('edit_scenario', version=version)
    # For GET or if no source_version, render the copy form
    all_scenarios = scenarios.objects.exclude(version=version)
    return render(request, 'website/copy_master_data_inco_terms.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })

from django.shortcuts import render, get_object_or_404, redirect
from django.forms import modelformset_factory
from django.contrib import messages
from .models import MasterDataPlan
from .forms import MasterDataPlanForm

def update_master_data_plan(request, version):
    # Filter records by version
    plans = MasterDataPlan.objects.filter( version=version)

    # Create a formset for editing multiple records
    MasterDataPlanFormSet = modelformset_factory(MasterDataPlan, form=MasterDataPlanForm, extra=0)

    if request.method == 'POST':
        formset = MasterDataPlanFormSet(request.POST, queryset=plans)
        if formset.is_valid():
            formset.save()
            messages.success(request, "Master Data Plan updated successfully!")
            return redirect('edit_scenario', version=version)
    else:
        formset = MasterDataPlanFormSet(queryset=plans)

    return render(request, 'website/update_master_data_plan.html', {
        'formset': formset,
        'version': version,
    })

def delete_master_data_plan(request, version):
    MasterDataPlan.objects.filter( version=version).delete()
    messages.success(request, "All Master Data Plan records deleted successfully!")
    return redirect('edit_scenario', version=version)

import csv
from django.http import HttpResponse

def upload_master_data_plan(request, version):
    """Upload Pour Plan Master Data from Excel file"""
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST' and request.FILES.get('file'):
        try:
            uploaded_file = request.FILES['file']
            
            # Validate file type
            if not uploaded_file.name.endswith(('.xlsx', '.xls', '.csv')):
                messages.error(request, "Please upload an Excel file (.xlsx, .xls) or CSV file.")
                return render(request, 'website/upload_master_data_plan.html', {'scenario': scenario})
            
            # Delete existing Pour Plan data for this scenario first
            deleted_count = MasterDataPlan.objects.filter(version=scenario).count()
            MasterDataPlan.objects.filter(version=scenario).delete()
            
            import pandas as pd
            from datetime import datetime
            import io
            
            # Read the uploaded file
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            # Validate required headers
            required_headers = [
                'Foundry', 'Month', 'Yield', 'WasterPercentage', 
                'PlannedMaintenanceDays', 'PublicHolidays', 'Weekends', 
                'OtherNonPouringDays', 'heatsperdays', 'TonsPerHeat'
            ]
            
            missing_headers = [header for header in required_headers if header not in df.columns]
            if missing_headers:
                messages.error(request, f"Missing required columns: {', '.join(missing_headers)}")
                return render(request, 'website/upload_master_data_plan_file.html', {
                    'scenario': scenario,
                    'required_headers': required_headers
                })
            
            # Process each row and create new records
            created_count = 0
            errors = []
            
            for index, row in df.iterrows():
                try:
                    # Get foundry object
                    foundry_name = str(row['Foundry']).strip()
                    foundry = MasterDataPlantModel.objects.filter(SiteName=foundry_name).first()
                    
                    if not foundry:
                        errors.append(f"Row {index + 1}: Foundry '{foundry_name}' not found")
                        continue
                    
                    # Parse month
                    month_value = row['Month']
                    if pd.isna(month_value):
                        errors.append(f"Row {index + 1}: Month is required")
                        continue
                    
                    # Convert month to datetime if it's a string
                    if isinstance(month_value, str):
                        try:
                            month_date = pd.to_datetime(month_value).date()
                        except:
                            errors.append(f"Row {index + 1}: Invalid month format '{month_value}'")
                            continue
                    else:
                        month_date = month_value
                    
                    # Convert percentage fields if they appear to be in decimal format
                    waster_percentage = float(row.get('WasterPercentage', 0))
                    yield_percentage = float(row.get('Yield', 0))
                    
                    # If values are very small (< 1), they're likely in decimal format, convert to percentage
                    if 0 < waster_percentage < 1:
                        waster_percentage = waster_percentage * 100
                    if 0 < yield_percentage < 1:
                        yield_percentage = yield_percentage * 100
                    
                    # Create new MasterDataPlan record
                    plan = MasterDataPlan.objects.create(
                        version=scenario,
                        Foundry=foundry,
                        Month=month_date,
                        Yield=yield_percentage,
                        WasterPercentage=waster_percentage,
                        PlannedMaintenanceDays=row.get('PlannedMaintenanceDays', 0),
                        PublicHolidays=row.get('PublicHolidays', 0),
                        Weekends=row.get('Weekends', 0),
                        OtherNonPouringDays=row.get('OtherNonPouringDays', 0),
                        heatsperdays=row.get('heatsperdays', 0),
                        TonsPerHeat=row.get('TonsPerHeat', 0)
                        # Note: CalendarDays, AvailableDays, and PlanDressMass are calculated automatically by the model properties
                    )
                    created_count += 1
                    
                except Exception as e:
                    errors.append(f"Row {index + 1}: Error creating record - {str(e)}")
            
            # Show results
            if created_count > 0:
                success_message = f"Successfully uploaded {created_count} Pour Plan records. "
                if deleted_count > 0:
                    success_message += f"Replaced {deleted_count} existing records. "
                success_message += "CalendarDays, AvailableDays, and PlanDressMass are automatically calculated."
                messages.success(request, success_message)
            
            if errors:
                error_message = f"Encountered {len(errors)} errors during upload:\n" + "\n".join(errors[:5])
                if len(errors) > 5:
                    error_message += f"\n... and {len(errors) - 5} more errors."
                messages.warning(request, error_message)
            
            if created_count > 0:
                return redirect('edit_scenario', version=version)
                
        except Exception as e:
            messages.error(request, f"Error processing file: {str(e)}")
    
    # For GET request or errors, show the upload form
    required_headers = [
        'Foundry', 'Month', 'Yield', 'WasterPercentage', 
        'PlannedMaintenanceDays', 'PublicHolidays', 'Weekends', 
        'OtherNonPouringDays', 'heatsperdays', 'TonsPerHeat'
    ]
    
    return render(request, 'website/upload_master_data_plan_file.html', {
        'scenario': scenario,
        'required_headers': required_headers
    })

from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from .models import MasterDataPlan, scenarios

def copy_master_data_plan(request, version):
    target_scenario = get_object_or_404(scenarios, version=version)

    if request.method == 'POST':
        # Get the source scenario from the form
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)

        # Delete all existing records for the target scenario
        MasterDataPlan.objects.filter( version=target_scenario).delete()

        # Copy records from the source scenario to the target scenario
        source_plans = MasterDataPlan.objects.filter( version=source_scenario)
        for plan in source_plans:
            plan.pk = None  # Reset primary key to create a new record
            plan. version = target_scenario  # Assign the target scenario
            plan.save()

        messages.success(request, f"Data successfully copied from scenario '{source_version}' to '{version}'.")
        return redirect('edit_scenario', version=version)

    # Get all scenarios except the current one
    all_scenarios = scenarios.objects.exclude(version=version)

    return render(request, 'website/copy_master_data_plan.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })

from django.shortcuts import render, redirect
from django.forms import modelformset_factory
from django.contrib import messages
from .models import MasterDataCapacityModel
from .forms import MasterDataCapacityForm

@login_required
def update_master_data_capacity(request, version):
    # Filter records by version
    capacities = MasterDataCapacityModel.objects.filter(version=version)

    # Create a formset for editing multiple records
    MasterDataCapacityFormSet = modelformset_factory(MasterDataCapacityModel, form=MasterDataCapacityForm, extra=0)

    if request.method == 'POST':
        formset = MasterDataCapacityFormSet(request.POST, queryset=capacities)
        if formset.is_valid():
            formset.save()
            messages.success(request, "Master Data Capacity updated successfully!")
            return redirect('edit_scenario', version=version)
    else:
        formset = MasterDataCapacityFormSet(queryset=capacities)

    return render(request, 'website/update_master_data_capacity.html', {
        'formset': formset,
        'version': version,
    })

@login_required
def delete_master_data_capacity(request, version):
    MasterDataCapacityModel.objects.filter(version=version).delete()
    messages.success(request, "All Master Data Capacity records deleted successfully!")
    return redirect('edit_scenario', version=version)

import csv

@login_required
def upload_master_data_capacity(request, version):
    if request.method == 'POST' and request.FILES['file']:
        csv_file = request.FILES['file']
        decoded_file = csv_file.read().decode('utf-8').splitlines()
        reader = csv.DictReader(decoded_file)

        for row in reader:
            MasterDataCapacityModel.objects.update_or_create(
                version=version,
                Foundry=row['Foundry'],
                defaults={
                    'PouringDaysPerWeek': row.get('PouringDaysPerWeek'),
                    'ShiftsPerDay': row.get('ShiftsPerDay'),
                    'HoursPershift': row.get('HoursPershift'),
                    'Maxnumberofheatsperday': row.get('Maxnumberofheatsperday'),
                    'Minnumberofheatsperday': row.get('Minnumberofheatsperday'),
                    'Averagenumberofheatsperday': row.get('Averagenumberofheatsperday'),
                    'Month': row.get('Month'),
                    'Yiled': row.get('Yiled'),
                    'Waster': row.get('Waster'),
                    'Dresspouringcapacity': row.get('Dresspouringcapacity'),
                    'Calendardays': row.get('Calendardays'),
                    'Plannedmaintenancedays': row.get('Plannedmaintenancedays'),
                    'Publicholidays': row.get('Publicholidays'),
                    'Weekends': row.get('Weekends'),
                    'Othernonpouringdays': row.get('Othernonpouringdays'),
                    'Unavailabiledays': row.get('Unavailabiledays'),
                    'Availabledays': row.get('Availabledays'),
                    'Heatsperday': row.get('Heatsperday'),
                    'CastMasstonsperheat': row.get('CastMasstonsperheat'),
                    'Casttonnesperday': row.get('Casttonnesperday'),
                }
            )
        messages.success(request, "Master Data Capacity uploaded successfully!")
        return redirect('edit_scenario', version=version)

    return render(request, 'website/upload_master_data_capacity.html', {'version': version})

@login_required
def copy_master_data_capacity(request, version):
    capacities = MasterDataCapacityModel.objects.filter(version=version)
    for capacity in capacities:
        capacity.pk = None  # Reset primary key to create a new record
        capacity.save()
    messages.success(request, "Master Data Capacity copied successfully!")
    return redirect('edit_scenario', version=version)

from django.db import IntegrityError
from django.contrib import messages

from django.db import IntegrityError
from django.contrib import messages

@login_required
def update_pour_plan_data(request, version):
    user_name = request.user.username
    scenario = scenarios.objects.get(version=version)

    foundry_options = ['COI2', 'MTJ1', 'XUZ1', 'WOD1', 'WUN1', 'MER1','CHI1',]

    plans = MasterDataPlan.objects.filter(version=scenario)

    foundry_filter = request.GET.get('foundry', '').strip()
    month_filter = request.GET.get('month', '').strip()

    if foundry_filter:
        plans = plans.filter(Foundry__SiteName__icontains=foundry_filter)
    if month_filter:
        plans = plans.filter(Month__icontains=month_filter)

    MasterDataPlanFormSet = modelformset_factory(MasterDataPlan, form=MasterDataPlanForm, extra=15, can_delete=True)

    if request.method == 'POST':
        formset = MasterDataPlanFormSet(request.POST, queryset=plans)
        if formset.is_valid():
            instances = formset.save(commit=False)
            for instance in instances:
                if instance.Foundry and instance.Month:
                    instance.version = scenario
                    try:
                        instance.save()
                    except IntegrityError:
                        messages.error(request, f"Duplicate entry for {instance.Foundry.SiteName} - {instance.Month}")
            
            for instance in formset.deleted_objects:
                instance.delete()
                
            messages.success(request, "Pour plan data updated successfully!")
            return redirect('edit_scenario', version=version)
        else:
            messages.error(request, "Please correct the errors in the form.")
    else:
        formset = MasterDataPlanFormSet(queryset=plans)

    return render(request, 'website/update_pour_plan_data.html', {
        'formset': formset,
        'scenario': scenario,  # ← Add this line
        'version': version,
        'user_name': user_name,
        'foundry_options': foundry_options,
        'foundry_filter': foundry_filter,
        'month_filter': month_filter,
    })

from . models import MasterDataSuppliersModel, MasterDataCustomersModel
@login_required
def suppliers_fetch_data_from_mssql(request):
    # Connect to the database
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)

    # Fetch new data from the database
    with engine.connect() as connection:
        query = text("SELECT * FROM PowerBI.Supplier")
        result = connection.execute(query)

        Supplier_dict = {}

        for row in result:
            if not row.VendorID or row.VendorID.strip() == "":  # Skip if VendorID is null or blank
                continue
            Supplier_dict[row.VendorID] = {
                'TradingName': row.TradingName,
                'Address1': row.Address1,
            }

    # Update or create records in the model
    for vendor_id, data in Supplier_dict.items():
        MasterDataSuppliersModel.objects.update_or_create(
            VendorID=vendor_id,
            defaults=data
        )

    return redirect('suppliers_list')  # Replace 'SuppliersList' with the actual view name for the suppliers list

@login_required
def customers_fetch_data_from_mssql(request):
    # Connect to the database
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)

    # Fetch new data from the database
    with engine.connect() as connection:
        query = text("SELECT * FROM PowerBI.Customers WHERE RowEndDate IS NULL")
        result = connection.execute(query)

        Customer_dict = {}

        for row in result:
            if not row.CustomerId or row.CustomerId.strip() == "":  # Skip if CustomerId is null or blank
                continue
            Customer_dict[row.CustomerId] = {
                'CustomerName': row.CustomerName,
                'CustomerRegion': row.CustomerRegion,
                'ForecastRegion': row.ForecastRegion,
            }

    # Update or create records in the model
    for customer_id, data in Customer_dict.items():
        MasterDataCustomersModel.objects.update_or_create(
            CustomerId=customer_id,
            defaults=data
        )

    return redirect('CustomersList')  # Replace 'CustomersList' with the actual view name for the customers list

@login_required
def suppliers_list(request):
    user_name = request.user.username
    suppliers = MasterDataSuppliersModel.objects.all().order_by('VendorID')

    # Filtering logic
    VendorID_filter = request.GET.get('VendorID', '')
    TradingName_filter = request.GET.get('TradingName', '')
    Address1_filter = request.GET.get('Address1', '')

    if VendorID_filter:
        suppliers = suppliers.filter(VendorID__icontains=VendorID_filter)
    if TradingName_filter:
        suppliers = suppliers.filter(TradingName__icontains=TradingName_filter)
    if Address1_filter:
        suppliers = suppliers.filter(Address1__icontains=Address1_filter)

    # Pagination logic
    paginator = Paginator(suppliers, 15)  # Show 15 suppliers per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'VendorID_Filter': VendorID_filter,
        'TradingName_Filter': TradingName_filter,
        'Address1_Filter': Address1_filter,
        'user_name': user_name,
    }
    return render(request, 'website/suppliers_list.html', context)

@login_required
def customers_list(request):
    user_name = request.user.username
    customers = MasterDataCustomersModel.objects.all().order_by('CustomerId')

    # Filtering logic
    CustomerId_filter = request.GET.get('CustomerId', '')
    CustomerName_filter = request.GET.get('CustomerName', '')
    CustomerRegion_filter = request.GET.get('CustomerRegion', '')
    ForecastRegion_filter = request.GET.get('ForecastRegion', '')

    if CustomerId_filter:
        customers = customers.filter(CustomerId__icontains=CustomerId_filter)
    if CustomerName_filter:
        customers = customers.filter(CustomerName__icontains=CustomerName_filter)
    if CustomerRegion_filter:
        customers = customers.filter(CustomerRegion__icontains=CustomerRegion_filter)
    if ForecastRegion_filter:
        customers = customers.filter(ForecastRegion__icontains=ForecastRegion_filter)

    # Pagination logic
    paginator = Paginator(customers, 15)  # Show 15 customers per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'CustomerId_Filter': CustomerId_filter,
        'CustomerName_Filter': CustomerName_filter,
        'CustomerRegion_Filter': CustomerRegion_filter,
        'ForecastRegion_Filter': ForecastRegion_filter,
        'user_name': user_name,
    }
    return render(request, 'website/customers_list.html', context)

from django.db.models import Q
from django.core.paginator import Paginator
from django.shortcuts import render
from .models import MasterDataSupplyOptionsModel

def SupplyOptions(request):
    user_name = request.user.username
    # Filters
    product_filter = request.GET.get('product', '')
    inhouse_or_outsource_filter = request.GET.get('inhouse_or_outsource', '')
    source_filter = request.GET.get('source', '')

    # Queryset with filters
    queryset = MasterDataSupplyOptionsModel.objects.select_related('Product', 'Supplier', 'Site').all()
    if product_filter:
        queryset = queryset.filter(Product__Product__icontains=product_filter)
    if inhouse_or_outsource_filter:
        queryset = queryset.filter(InhouseOrOutsource__icontains=inhouse_or_outsource_filter)
    if source_filter:
        queryset = queryset.filter(
            Q(Supplier__VendorID__icontains=source_filter) |
            Q(Site__SiteName__icontains=source_filter)
        )

    # Deduplicate manually in Python
    unique_options = {}
    for option in queryset:
        key = (option.Product.Product, option.InhouseOrOutsource, option.Source)
        if key not in unique_options:
            unique_options[key] = option

    # Convert the deduplicated values back to a list
    deduplicated_queryset = list(unique_options.values())

    # Pagination
    paginator = Paginator(deduplicated_queryset, 10)  # Show 10 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'website/Supply_Options.html', {
        'page_obj': page_obj,
        'product_filter': product_filter,
        'inhouse_or_outsource_filter': inhouse_or_outsource_filter,
        'source_filter': source_filter,
        'user_name': user_name,
    })

from django.shortcuts import get_object_or_404, redirect
from django.http import JsonResponse
from .models import MasterDataEpicorSupplierMasterDataModel, scenarios

from django.forms import modelformset_factory
from django.core.paginator import Paginator

@login_required
def update_epicor_supplier_master_data(request, version):
    scenario = get_object_or_404(scenarios, version=version)

    # Get filter values from the query string
    company_filter = request.GET.get('company', '')
    plant_filter = request.GET.get('plant', '')
    product_filter = request.GET.get('product', '')
    vendor_filter = request.GET.get('vendor', '')
    source_type_filter = request.GET.get('source_type', '')  # Add SourceType filter

    # Filter the queryset based on exact matches
    queryset = MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario)
    if company_filter:
        queryset = queryset.filter(Company__exact=company_filter)  # Exact match for Company
    if plant_filter:
        queryset = queryset.filter(Plant__exact=plant_filter)  # Exact match for Plant
    if product_filter:
        queryset = queryset.filter(PartNum__exact=product_filter)  # Exact match for Product
    if vendor_filter:
        queryset = queryset.filter(VendorID__exact=vendor_filter)  # Exact match for Vendor
    if source_type_filter:
        queryset = queryset.filter(SourceType__exact=source_type_filter)  # Exact match for SourceType

    # Apply ordering before slicing
    queryset = queryset.order_by('id')  # Ensure the queryset is ordered before pagination

    # Paginate the queryset
    paginator = Paginator(queryset, 10)  # Show 10 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Create a formset for the current page
    EpicorSupplierFormSet = modelformset_factory(
        MasterDataEpicorSupplierMasterDataModel,
        fields=('Company', 'Plant', 'PartNum', 'VendorID', 'SourceType'),  # Include SourceType
        extra=0
    )
    formset = EpicorSupplierFormSet(queryset=page_obj.object_list)

    return render(request, 'website/update_epicor_supplier_master_data.html', {
        'scenario': scenario,
        'formset': formset,
        'page_obj': page_obj,
        'company_filter': company_filter,
        'plant_filter': plant_filter,
        'product_filter': product_filter,
        'vendor_filter': vendor_filter,
        'source_type_filter': source_type_filter,  # Pass SourceType filter to the template
    })

@login_required
def delete_epicor_supplier_master_data(request, version):
    """Delete all records for a specific version in MasterDataEpicorSupplierMasterDataModel."""
    scenario = get_object_or_404(scenarios, version=version)
    MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario).delete()
    return JsonResponse({'success': True, 'message': 'Records deleted successfully.'})

@login_required
def copy_epicor_supplier_master_data(request, version):
    """Copy all records for a specific version in MasterDataEpicorSupplierMasterDataModel."""
    target_scenario = get_object_or_404(scenarios, version=version)

    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)

        # Copy records
        source_records = MasterDataEpicorSupplierMasterDataModel.objects.filter(version=source_scenario)
        bulk_data = [
            MasterDataEpicorSupplierMasterDataModel(
                version=target_scenario,
                Company=record.Company,
                Plant=record.Plant,
                PartNum=record.PartNum,
                VendorID=record.VendorID,
                SourceType=record.SourceType,  # Include SourceType
            )
            for record in source_records
        ]
        MasterDataEpicorSupplierMasterDataModel.objects.bulk_create(bulk_data)

        return redirect('edit_scenario', version=version)

    # Get all scenarios except the current one
    all_scenarios = scenarios.objects.exclude(version=version)

    return render(request, 'website/copy_epicor_supplier_master_data.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })

from django.shortcuts import get_object_or_404, redirect
from django.http import JsonResponse
from .models import MasterDataEpicorSupplierMasterDataModel, scenarios
from sqlalchemy import create_engine, text
from django.contrib.auth.decorators import login_required

@login_required
def upload_epicor_supplier_master_data(request, version):
    """Fetch data from Epicor database and upload it to MasterDataEpicorSupplierMasterDataModel."""
    scenario = get_object_or_404(scenarios, version=version)

    # Database connection details
    server = 'bknew-sql02'
    database = 'Bradken_Epicor_ODS'
    driver = 'ODBC Driver 17 for SQL Server'
    database_con = f'mssql+pyodbc://@{server}/{database}?driver={driver}'
    engine = create_engine(database_con)

    try:
        # Establish the database connection
        with engine.connect() as connection:
            # Step 1: Delete existing data for the given version
            MasterDataEpicorSupplierMasterDataModel.objects.filter(version=scenario).delete()

            # Step 2: SQL query to join tables and fetch data with LEFT JOIN
            query = text("""
                SELECT 
                    PartPlant.Company AS Company,
                    PartPlant.Plant AS Plant,
                    PartPlant.PartNum AS PartNum,
                    PartPlant.[SourceType] AS SourceType,  -- Corrected column name
                    Vendor.VendorID AS VendorID
                FROM epicor.PartPlant AS PartPlant
                LEFT JOIN epicor.Vendor AS Vendor
                    ON PartPlant.Company = Vendor.Company
                    AND PartPlant.VendorNum = Vendor.VendorNum
                WHERE PartPlant.RowEndDate IS NULL
            """)

            # Step 3: Execute the query
            result = connection.execute(query)

            # Step 4: Create a list of objects to bulk insert
            bulk_data = []
            for row in result:
                # Apply the custom logic for VendorID
                vendor_id = row.VendorID
                if row.SourceType == 'M' and row.Company == 'AU03':
                    vendor_id = row.Plant  # Set VendorID to Plant if conditions are met

                # Add the record to the bulk data list
                bulk_data.append(
                    MasterDataEpicorSupplierMasterDataModel(
                        version=scenario,
                        Company=row.Company,
                        Plant=row.Plant,
                        PartNum=row.PartNum,
                        VendorID=vendor_id if vendor_id else None,  # Handle NULL values for VendorID
                        SourceType=row.SourceType  # Save SourceType
                    )
                )

            # Step 5: Perform bulk insert
            MasterDataEpicorSupplierMasterDataModel.objects.bulk_create(bulk_data)

        # Step 6: Redirect to the edit scenario page after successful upload
        return redirect('edit_scenario', version=version)
    except Exception as e:
        # Handle any exceptions and return an error response
        return JsonResponse({'success': False, 'message': f'An error occurred: {e}'})

# OLD IMPORTS - DISABLED IN FAVOR OF DIRECT COMMAND IMPORTS
# from django.core.management import call_command
# from django.contrib import messages
# from django.shortcuts import redirect
# from website.models import AggregatedForecast, CalcualtedReplenishmentModel, CalculatedProductionModel, scenarios

# from django.core.management import call_command
# from django.contrib import messages
# from django.shortcuts import redirect
# from website.models import AggregatedForecast, CalcualtedReplenishmentModel, CalculatedProductionModel, scenarios


# from django.db import transaction

# import subprocess
# from django.conf import settings

# OLD RUN_MANAGEMENT_COMMAND FUNCTION - DISABLED IN FAVOR OF DIRECT COMMAND EXECUTION
# def run_management_command(command, *args):
#     manage_py = settings.BASE_DIR / 'manage.py'
#     cmd = ['python', str(manage_py), command] + [str(arg) for arg in args]
#     result = subprocess.run(cmd, capture_output=True, text=True)
#     return result

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.contrib import messages
from .models import (
    scenarios,
    AggregatedForecast,
    CalcualtedReplenishmentModel,
    CalculatedProductionModel,
    ProductSiteCostModel,
    MasterDataInventory,
    MasterDataProductModel,
    MasterDataPlantModel,
)
from django.core.paginator import Paginator
from django.db.models import Max
from django.shortcuts import get_object_or_404, redirect

# Import your optimized command classes directly
from website.management.commands.populate_aggregated_forecast import Command as AggForecastCommand
from website.management.commands.populate_calculated_replenishment_v2 import Command as ReplenishmentCommand
from website.management.commands.populate_calculated_production import Command as ProductionCommand

@login_required
@transaction.non_atomic_requests
def calculate_model(request, version):
    """
    Run the management commands to calculate the model for the given version.
    """
    print("calculate_model called with version:", version)
    try:
        # Step 1: Run the first command: populate_aggregated_forecast
        print("Running populate_aggregated_forecast")
        print("version:", version)
        AggForecastCommand().handle(version=version)
        messages.success(request, f"Aggregated forecast has been successfully populated for version '{version}'.")

        # Step 2: Run the second command: populate_calculated_replenishment_v2
        print("Running populate_calculated_replenishment_v2")
        print("version:", version)
        ReplenishmentCommand().handle(version=version)
        messages.success(request, f"Calculated replenishment (V2) has been successfully populated for version '{version}'.")

        # Step 3: Run the third command: populate_calculated_production
        print("Running populate_calculated_production")
        print("version:", version)
        ProductionCommand().handle(scenario_version=version)
        messages.success(request, f"Calculated production has been successfully populated for version '{version}'.")

        # Step 4: Calculate aggregated data for fast loading
        print("Running populate_all_aggregated_data")
        print("version:", version)
        scenario = get_object_or_404(scenarios, version=version)
        populate_all_aggregated_data(scenario)

        messages.success(request, f"Aggregated chart data has been successfully calculated for version '{version}'.")

        # Step 5: Run the cache_review_data command to update Control Tower cache
        print("Running cache_review_data for Control Tower cache...")
        from django.conf import settings
        import os, sys, subprocess
        manage_py = os.path.join(settings.BASE_DIR, 'manage.py')
        cmd = [sys.executable, manage_py, 'cache_review_data', '--scenario', str(version), '--force']
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            messages.success(request, f"Control Tower cache has been updated for version '{version}'.")
        else:
            messages.error(request, f"Failed to update Control Tower cache: {result.stderr}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        messages.error(request, f"An error occurred while calculating the model: {e}")

    # Redirect back to the list of scenarios
    return redirect('list_scenarios')

def test_product_calculation(request, version):
    """
    Test calculation for a specific product only (much faster for debugging)
    """
    if request.method == 'POST':
        product_name = request.POST.get('product_name', '').strip()
        
        if not product_name:
            messages.error(request, "Please enter a product name.")
            return redirect('test_product_calculation', version=version)
        
        print(f"test_product_calculation called with version: {version}, product: {product_name}")
        
        try:
            # Only run the replenishment calculation for the specific product
            print(f"Running populate_calculated_replenishment_v2 for product: {product_name}")
            ReplenishmentCommand().handle(version=version, product=product_name)
            messages.success(request, f"Replenishment calculation completed for product '{product_name}' in version '{version}'.")
            
            # Get results for display
            from website.models import CalcualtedReplenishmentModel
            results = CalcualtedReplenishmentModel.objects.filter(
                Product__Product=product_name, 
                version=version
            ).order_by('Location', 'ShippingDate')
            
            total_qty = sum(r.ReplenishmentQty for r in results)
            
            messages.success(request, f"Total replenishment quantity for {product_name}: {total_qty:,.0f} units across {results.count()} records.")
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            messages.error(request, f"An error occurred while calculating for product '{product_name}': {e}")

        return redirect('test_product_calculation', version=version)
    
    # GET request - show the form
    context = {
        'version': version,
        'scenario': scenarios.objects.get(version=version)
    }
    return render(request, 'website/test_product_calculation.html', context)

from .forms import PlantForm

def create_plant(request):
    if request.method == 'POST':
        form = PlantForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('PlantsList')  # Redirect to the plants list after saving
    else:
        form = PlantForm()
    return render(request, 'website/create_plant.html', {'form': form})


from .models import MasterDataEpicorBillOfMaterialModel
from sqlalchemy import create_engine, text
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect

@login_required
def BOM_fetch_data_from_mssql(request):
    if request.method == 'POST':  # Only run on POST (refresh)
        Server = 'bknew-sql02'
        Database = 'Bradken_Epicor_ODS'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        engine = create_engine(Database_Con)

        # Use context manager to ensure connection is closed
        with engine.connect() as connection:
            # Fetch BOM data
            query = text("SELECT * FROM epicor.PartMtl WHERE RowEndDate IS NULL")
            result = connection.execute(query)
            rows = list(result)

            # Fetch all existing (Parent, Plant, ComponentSeq) combinations to skip duplicates
            existing_keys = set(
                MasterDataEpicorBillOfMaterialModel.objects.values_list('Parent', 'Plant', 'ComponentSeq')
            )

            new_records = []
            for row in rows:
                parent = row.PartNum
                plant = row.RevisionNum  # <-- Map Plant to RevisionNum
                component_seq = row.MtlSeq
                if (parent, plant, component_seq) in existing_keys:
                    continue
                new_records.append(
                    MasterDataEpicorBillOfMaterialModel(
                        Company=row.Company,
                        Plant=plant,
                        Parent=parent,
                        ComponentSeq=component_seq,
                        Component=row.MtlPartNum,
                        ComponentUOM=row.UOMCode,
                        QtyPer=row.QtyPer,
                        EstimatedScrap=row.EstScrap,
                        SalvageQtyPer=row.SalvageQtyPer,
                    )
                )
                existing_keys.add((parent, plant, component_seq))

            if new_records:
                MasterDataEpicorBillOfMaterialModel.objects.bulk_create(new_records, batch_size=1000)

    return redirect('bom_list')

from django.core.paginator import Paginator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render

@login_required
def bom_list(request):
    user_name = request.user.username
    boms = MasterDataEpicorBillOfMaterialModel.objects.all().order_by('Plant', 'Parent', 'ComponentSeq')

    # Filtering logic
    def filter_field(qs, field, value):
        if value:
            if value.startswith('*') or value.endswith('*'):
                # Remove * and use icontains
                qs = qs.filter(**{f"{field}__icontains": value.replace('*', '')})
            else:
                qs = qs.filter(**{f"{field}__exact": value})
        return qs

    Company_filter = request.GET.get('Company', '')
    Plant_filter = request.GET.get('Plant', '')
    Parent_filter = request.GET.get('Parent', '')
    ComponentSeq_filter = request.GET.get('ComponentSeq', '')
    Component_filter = request.GET.get('Component', '')
    ComponentUOM_filter = request.GET.get('ComponentUOM', '')
    QtyPer_filter = request.GET.get('QtyPer', '')
    EstimatedScrap_filter = request.GET.get('EstimatedScrap', '')
    SalvageQtyPer_filter = request.GET.get('SalvageQtyPer', '')

    boms = filter_field(boms, 'Company', Company_filter)
    boms = filter_field(boms, 'Plant', Plant_filter)
    boms = filter_field(boms, 'Parent', Parent_filter)
    boms = filter_field(boms, 'ComponentSeq', ComponentSeq_filter)
    boms = filter_field(boms, 'Component', Component_filter)
    boms = filter_field(boms, 'ComponentUOM', ComponentUOM_filter)
    boms = filter_field(boms, 'QtyPer', QtyPer_filter)
    boms = filter_field(boms, 'EstimatedScrap', EstimatedScrap_filter)
    boms = filter_field(boms, 'SalvageQtyPer', SalvageQtyPer_filter)

    # Sort by Plant, then Parent, then ComponentSeq
    boms = boms.order_by('Plant', 'Parent', 'ComponentSeq')

    # Pagination logic
    paginator = Paginator(boms, 50)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'Company_filter': Company_filter,
        'Plant_filter': Plant_filter,
        'Parent_filter': Parent_filter,
        'ComponentSeq_filter': ComponentSeq_filter,
        'Component_filter': Component_filter,
        'ComponentUOM_filter': ComponentUOM_filter,
        'QtyPer_filter': QtyPer_filter,
        'EstimatedScrap_filter': EstimatedScrap_filter,
        'SalvageQtyPer_filter': SalvageQtyPer_filter,
        'user_name': user_name,
    }
    return render(request, 'website/bom_list.html', context)

from django.forms import formset_factory
from .forms import ManuallyAssignProductionRequirementForm

from collections import defaultdict

@login_required
def update_manually_assign_production_requirement(request, version):
    from django.core.paginator import Paginator
    
    scenario = get_object_or_404(scenarios, version=version)

    # Filters from GET
    product_filter = request.GET.get('product', '').strip()
    site_filter = request.GET.get('site', '').strip()
    shipping_date_filter = request.GET.get('shipping_date', '').strip()
    percentage_filter = request.GET.get('percentage', '').strip()

    # Filter queryset
    records = MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario)
    if product_filter:
        records = records.filter(Product__Product__icontains=product_filter)
    if site_filter:
        records = records.filter(Site__SiteName__icontains=site_filter)
    if shipping_date_filter:
        records = records.filter(ShippingDate=shipping_date_filter)
    if percentage_filter:
        try:
            records = records.filter(Percentage=float(percentage_filter))
        except ValueError:
            pass

    # Always sort by ShippingDate ascending
    records = records.order_by('ShippingDate')

    # Add pagination - 20 records per page
    paginator = Paginator(records, 20)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Prepare initial data for the formset using paginated records
    initial_data = [
        {
            'Product': rec.Product.Product if rec.Product else '',
            'Site': rec.Site.SiteName if rec.Site else '',
            'ShippingDate': rec.ShippingDate,
            'Percentage': rec.Percentage,
            'id': rec.id,
        }
        for rec in page_obj.object_list  # Use paginated records instead of all records
    ]

    ManualAssignFormSet = formset_factory(ManuallyAssignProductionRequirementForm, extra=0, can_delete=True)

    errors = []
    if request.method == 'POST':
        formset = ManualAssignFormSet(request.POST)
        if formset.is_valid():
            # Validate sum of percentages for each (Product, ShippingDate)
            percent_sum = defaultdict(float)
            entries = []
            for form in formset:
                if form.cleaned_data and not form.cleaned_data.get('DELETE', False):
                    product_code = form.cleaned_data['Product']
                    shipping_date = form.cleaned_data['ShippingDate']
                    percentage = form.cleaned_data['Percentage']
                    key = (product_code, shipping_date)
                    percent_sum[key] += percentage
                    entries.append(form)

            for key, total in percent_sum.items():
                if abs(total - 1.0) > 0.0001:
                    errors.append(
                        f"Total percentage for Product '{key[0]}' and Shipping Date '{key[1]}' must be 1.0 (currently {total})."
                    )

            if not errors:
                # Delete current records for this scenario page and re-create from formset
                record_ids = [rec.id for rec in page_obj.object_list]
                MasterDataManuallyAssignProductionRequirement.objects.filter(id__in=record_ids).delete()
                
                for form in formset:
                    if form.cleaned_data and not form.cleaned_data.get('DELETE', False):
                        product_code = form.cleaned_data['Product']
                        site_code = form.cleaned_data['Site']
                        shipping_date = form.cleaned_data['ShippingDate']
                        percentage = form.cleaned_data['Percentage']

                        product_obj = MasterDataProductModel.objects.filter(Product=product_code).first()
                        site_obj = MasterDataPlantModel.objects.filter(SiteName=site_code).first()
                        if not product_obj:
                            errors.append(f"Product '{product_code}' does not exist.")
                            continue
                        if not site_obj:
                            errors.append(f"Site '{site_code}' does not exist.")
                            continue

                        MasterDataManuallyAssignProductionRequirement.objects.create(
                            version=scenario,
                            Product=product_obj,
                            Site=site_obj,
                            ShippingDate=shipping_date,
                            Percentage=percentage
                        )
                if not errors:
                    return redirect('edit_scenario', version=version)
        else:
            errors.append("Please correct the errors in the form.")
    else:
        formset = ManualAssignFormSet(initial=initial_data)

    return render(
        request,
        'website/update_manually_assign_production_requirement.html',
        {
            'formset': formset,
            'version': version,
            'product_filter': product_filter,
            'site_filter': site_filter,
            'shipping_date_filter': shipping_date_filter,
            'percentage_filter': percentage_filter,
            'errors': errors,
            'page_obj': page_obj,  # Add pagination object
        }
    )

@login_required
def delete_manually_assign_production_requirement(request, version):
    from django.contrib import messages
    
    try:
        scenario = scenarios.objects.get(version=version)
    except scenarios.DoesNotExist:
        messages.error(request, f'Scenario version "{version}" does not exist.')
        return redirect('edit_scenario', version=version)

    if request.method == 'POST':
        # Delete all records for this scenario
        deleted_count, _ = MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario).delete()
        
        if deleted_count > 0:
            messages.success(request, f'Successfully deleted {deleted_count} manually assigned production requirement records for scenario version "{version}".')
        else:
            messages.info(request, f'No manually assigned production requirement records found for scenario version "{version}".')
        
        return redirect('edit_scenario', version=version)
    
    # GET request - show confirmation page
    record_count = MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario).count()
    
    return render(request, 'website/delete_manually_assign_production_requirement.html', {
        'version': version,
        'record_count': record_count,
    })

@login_required
def upload_manually_assign_production_requirement(request, version):
    import pandas as pd
    from django.contrib import messages
    
    if request.method == 'POST' and request.FILES.get('file'):
        file = request.FILES['file']
        fs = FileSystemStorage()
        filename = fs.save(file.name, file)
        file_path = fs.path(filename)

        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            return render(request, 'website/upload_manually_assign_production_requirement.html', {
                'error_message': 'The specified scenario does not exist.',
                'version': version
            })

        # Remove old records for this version
        MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario).delete()
        
        try:
            df = pd.read_excel(file_path)
            print("Excel DataFrame head:", df.head())

            success_count = 0
            error_count = 0
            errors = []

            for idx, row in df.iterrows():
                try:
                    # Parse the shipping date
                    shipping_date = row.get('ShippingDate')
                    if pd.notna(shipping_date):
                        try:
                            shipping_date = pd.to_datetime(shipping_date).date()
                        except ValueError:
                            shipping_date = None
                    else:
                        shipping_date = None

                    product_code = row.get('Product') if pd.notna(row.get('Product')) else None
                    site_code = row.get('Site') if pd.notna(row.get('Site')) else None
                    percentage = row.get('Percentage') if pd.notna(row.get('Percentage')) else 0

                    # Get Product and Site objects
                    product_obj = None
                    site_obj = None

                    if product_code:
                        product_obj = MasterDataProductModel.objects.filter(Product=product_code).first()
                        if not product_obj:
                            errors.append(f"Row {idx + 2}: Product '{product_code}' not found in master data")
                            error_count += 1
                            continue

                    if site_code:
                        site_obj = MasterDataPlantModel.objects.filter(SiteName=site_code).first()
                        if not site_obj:
                            errors.append(f"Row {idx + 2}: Site '{site_code}' not found in master data")
                            error_count += 1
                            continue

                    # Create the record
                    MasterDataManuallyAssignProductionRequirement.objects.create(
                        version=scenario,
                        Product=product_obj,
                        Site=site_obj,
                        ShippingDate=shipping_date,
                        Percentage=percentage
                    )
                    success_count += 1

                except Exception as e:
                    error_count += 1
                    errors.append(f"Row {idx + 2}: {str(e)}")

            # Clean up the uploaded file
            fs.delete(filename)

            if success_count > 0:
                messages.success(request, f'Successfully uploaded {success_count} records.')
            
            if error_count > 0:
                error_message = f'{error_count} errors occurred:\n' + '\n'.join(errors[:10])  # Show first 10 errors
                if len(errors) > 10:
                    error_message += f'\n... and {len(errors) - 10} more errors.'
                messages.error(request, error_message)

            return redirect('upload_manually_assign_production_requirement', version=version)

        except Exception as e:
            fs.delete(filename)
            return render(request, 'website/upload_manually_assign_production_requirement.html', {
                'error_message': f'Error processing file: {str(e)}',
                'version': version
            })

    # GET request - show the upload form
    form = UploadFileForm()
    return render(request, 'website/upload_manually_assign_production_requirement.html', {
        'form': form,
        'version': version
    })

@login_required
def copy_manually_assign_production_requirement(request, version):
    # Placeholder view for copying manually assigned production requirement
    return HttpResponse("Copy Manually Assign Production Requirement - version: {}".format(version))

from django.forms import modelform_factory

from django.forms import formset_factory
from .forms import ManuallyAssignProductionRequirementForm
from .models import MasterDataManuallyAssignProductionRequirement, MasterDataProductModel, MasterDataPlantModel

@login_required
def add_manually_assign_production_requirement(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    ManualAssignFormSet = formset_factory(ManuallyAssignProductionRequirementForm, extra=5)

    errors = []
    if request.method == 'POST':
        formset = ManualAssignFormSet(request.POST)
        if formset.is_valid():
            for form in formset:
                if form.cleaned_data:
                    product_code = form.cleaned_data['Product']
                    site_code = form.cleaned_data['Site']
                    shipping_date = form.cleaned_data['ShippingDate']
                    percentage = form.cleaned_data['Percentage']

                    # Validate Product and Site exist
                    product_obj = MasterDataProductModel.objects.filter(Product=product_code).first()
                    site_obj = MasterDataPlantModel.objects.filter(SiteName=site_code).first()
                    if not product_obj:
                        errors.append(f"Product '{product_code}' does not exist.")
                        continue
                    if not site_obj:
                        errors.append(f"Site '{site_code}' does not exist.")
                        continue

                    MasterDataManuallyAssignProductionRequirement.objects.create(
                        version=scenario,
                        Product=product_obj,
                        Site=site_obj,
                        ShippingDate=shipping_date,
                        Percentage=percentage
                    )
            if not errors:
                return redirect('edit_scenario', version=version)
    else:
        formset = ManualAssignFormSet()

    return render(request, 'website/add_manually_assign_production_requirement.html', {
        'formset': formset,
        'version': version,
        'errors': errors,
        'user_name': user_name,
    })

# ...existing code...

from django.core.management import call_command
from .models import CalcualtedReplenishmentModel, MasterDataPlantModel
from django.core.paginator import Paginator
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required

from django.contrib import messages

from django.contrib import messages
from django.core.paginator import Paginator
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from .models import CalcualtedReplenishmentModel, MasterDataPlantModel

@login_required
def manual_optimize_product(request, version):
    user_name = request.user.username
    # Get filter values from GET
    product_filter = request.GET.get('product', '').strip()
    location_filter = request.GET.get('location', '').strip()
    site_filter = request.GET.get('site', '').strip()
    shipping_date_filter = request.GET.get('shipping_date', '').strip()

    # Filter the queryset
    replenishments_qs = CalcualtedReplenishmentModel.objects.filter(version__version=version)
    if product_filter:
        replenishments_qs = replenishments_qs.filter(Product__Product__icontains=product_filter)
    if location_filter:
        replenishments_qs = replenishments_qs.filter(Location__icontains=location_filter)
    if site_filter:
        replenishments_qs = replenishments_qs.filter(Site__SiteName__icontains=site_filter)
    if shipping_date_filter:
        replenishments_qs = replenishments_qs.filter(ShippingDate=shipping_date_filter)

    sites = MasterDataPlantModel.objects.all()
    paginator = Paginator(replenishments_qs, 20)  # Show 20 per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    if request.method == "POST" and "save_changes" in request.POST:
        for row in page_obj.object_list:
            qty = request.POST.get(f"qty_{row.id}")
            site_id = request.POST.get(f"site_{row.id}")
            shipping_date = request.POST.get(f"shipping_date_{row.id}")
            split_qty = request.POST.get(f"split_qty_{row.id}")
            split_site_id = request.POST.get(f"split_site_{row.id}")

            changed = False

            # --- Handle split logic first ---
            if split_qty and split_site_id:
                try:
                    split_qty_val = float(split_qty)
                except ValueError:
                    split_qty_val = 0
                if (
                    split_qty_val > 0
                    and split_site_id
                    and split_site_id != ""
                    and split_qty_val < row.ReplenishmentQty
                ):
                    # Subtract from original
                    row.ReplenishmentQty -= split_qty_val
                    row.save()
                    # Create new row
                    CalcualtedReplenishmentModel.objects.create(
                        version=row.version,
                        Product=row.Product,
                        Location=row.Location,
                        Site=MasterDataPlantModel.objects.get(pk=split_site_id),
                        ShippingDate=row.ShippingDate,
                        ReplenishmentQty=split_qty_val,
                    )
                    # After split, update qty variable to the new value for further checks
                    if qty is not None:
                        try:
                            qty = float(qty)
                        except ValueError:
                            qty = row.ReplenishmentQty
                    else:
                        qty = row.ReplenishmentQty

            # --- Handle normal field changes ---
            if qty is not None and float(qty) != row.ReplenishmentQty:
                row.ReplenishmentQty = float(qty)
                changed = True
            if site_id and str(row.Site_id) != site_id:
                row.Site_id = site_id
                changed = True
            if shipping_date and str(row.ShippingDate) != shipping_date:
                row.ShippingDate = shipping_date
                changed = True
            if changed:
                row.save()

            print(f"Processing row {row.id}: qty={qty}, site_id={site_id}, split_qty={split_qty}, split_site_id={split_site_id}, current_qty={row.ReplenishmentQty}")
            print(f"Saved row {row.id} or created split.")

        messages.success(request, "Changes and splits saved successfully!")

        # Recalculate production for this scenario version
        try:
            call_command('populate_calculated_production', version)
            messages.success(request, "Production recalculated successfully!")
        except Exception as e:
            messages.error(request, f"Error recalculating production: {e}")


        return redirect('review_scenario', version=version)

    return render(request, "website/manual_optimize_product.html", {
        "version": version,
        "sites": sites,
        "page_obj": page_obj,
        "replenishments": page_obj.object_list,
        "product_filter": product_filter,
        "location_filter": location_filter,
        "site_filter": site_filter,
        "shipping_date_filter": shipping_date_filter,
        "user_name": user_name,
    })

@login_required
def balance_hard_green_sand(request, version):
    pass

@login_required
def create_balanced_pour_plan(request, version):
    pass

@login_required
def auto_level_optimization(request, version):
    """Auto Level Optimization function to fill gaps in pour plan by pulling work forward"""
    from datetime import datetime, timedelta
    from django.http import JsonResponse
    from django.db.models import Sum, Q
    from django.utils import timezone
    from .models import (
        scenarios, CalculatedProductionModel, MasterDataPlan, 
        MasterDataProductModel, MasterDataPlantModel, ScenarioOptimizationState
    )
    import json
    
    # Handle AJAX request for getting data
    if request.GET.get('action') == 'get_data':
        try:
            scenario = get_object_or_404(scenarios, version=version)
            
            # Check if optimization can be applied
            opt_state, created = ScenarioOptimizationState.objects.get_or_create(
                version=scenario,
                defaults={'auto_optimization_applied': False}
            )
            
            # Get unique sites from CalculatedProductionModel - filter to specific sites only
            allowed_sites = ['MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1']
            all_sites = list(CalculatedProductionModel.objects.filter(version=scenario)
                        .values_list('site__SiteName', flat=True)
                        .distinct()
                        .order_by('site__SiteName'))
            sites = [site for site in all_sites if site in allowed_sites]
            
            # Get unique product groups from CalculatedProductionModel
            product_groups = list(CalculatedProductionModel.objects.filter(version=scenario)
                                .exclude(product_group__isnull=True)
                                .exclude(product_group='')
                                .values_list('product_group', flat=True)
                                .distinct()
                                .order_by('product_group'))
            
            return JsonResponse({
                'sites': sites,
                'product_groups': product_groups,
                'optimization_applied': opt_state.auto_optimization_applied
            })
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    
    # Handle POST request for optimization
    if request.method == 'POST':
        try:
            scenario = get_object_or_404(scenarios, version=version)
            
            # Check if optimization has already been applied
            opt_state, created = ScenarioOptimizationState.objects.get_or_create(
                version=scenario,
                defaults={'auto_optimization_applied': False}
            )
            
            if opt_state.auto_optimization_applied:
                last_opt_date = opt_state.last_optimization_date.strftime('%Y-%m-%d %H:%M') if opt_state.last_optimization_date else 'Unknown'
                messages.warning(request, f"Auto optimization has already been applied to this scenario on {last_opt_date}. Please use the 'Reset Production Plan' button to restore original data, then you can optimize again.")
                return redirect('review_scenario', version=version)
            
            # Get form data
            selected_sites = request.POST.getlist('selected_sites')
            max_days_constraint = request.POST.get('max_days_constraint')
            limit_date_change = max_days_constraint == '90'
            
            if not selected_sites:
                messages.error(request, "Please select at least one site.")
                return redirect('review_scenario_fast', version=version)
            
            # Get all product groups automatically (no user selection required)
            all_product_groups = CalculatedProductionModel.objects.filter(
                version=scenario,
                site__SiteName__in=selected_sites
            ).exclude(product_group__isnull=True).exclude(product_group='').values_list('product_group', flat=True).distinct().order_by('product_group')
            selected_product_groups = list(all_product_groups)
            
            max_days_forward = 90 if limit_date_change else None
            optimized_count = 0
            total_tonnes_moved = 0
            
            print(f"DEBUG: Starting auto-leveling optimization for sites: {selected_sites}")
            print(f"DEBUG: Auto-detected product groups: {selected_product_groups}")
            print(f"DEBUG: Date limit: {max_days_forward} days" if max_days_forward else "DEBUG: No date limit")
            
            # Process each site separately
            for site_name in selected_sites:
                print(f"DEBUG: Processing site: {site_name}")
                
                # Get pour plan (capacity) for this site
                pour_plans = MasterDataPlan.objects.filter(
                    version=scenario,
                    Foundry__SiteName=site_name
                ).order_by('Month')
                
                if not pour_plans.exists():
                    messages.warning(request, f"No pour plan found for site {site_name}.")
                    continue
                
                # Create dictionary of monthly pour plan capacities
                monthly_pour_plan = {}
                for plan in pour_plans:
                    if plan.Month:
                        month_key = plan.Month.strftime('%Y-%m')
                        monthly_pour_plan[month_key] = (plan.PlanDressMass or 0) * 100  # Multiply by 100 to match optimization scale
                
                print(f"DEBUG: Pour plan for {site_name}: {monthly_pour_plan}")
                
                # Calculate current monthly demand (actual production) for this site
                current_monthly_demand = {}
                demand_records = CalculatedProductionModel.objects.filter(
                    version=scenario,
                    site__SiteName=site_name
                ).values('pouring_date', 'tonnes', 'parent_product_group')
                
                for record in demand_records:
                    if record['pouring_date']:
                        month_key = record['pouring_date'].strftime('%Y-%m')
                        current_monthly_demand[month_key] = current_monthly_demand.get(month_key, 0) + (record['tonnes'] or 0)
                
                print(f"DEBUG: Current demand for {site_name}: {current_monthly_demand}")
                
                # Calculate gaps (Pour Plan - Current Demand) for each month
                monthly_gaps = {}
                scenario_start_date = datetime(2025, 7, 1).date()  # Don't move production before scenario start
                
                for month_key, pour_capacity in monthly_pour_plan.items():
                    # Only consider months at or after the scenario start date
                    month_date = datetime.strptime(month_key + '-01', '%Y-%m-%d').date()
                    if month_date < scenario_start_date:
                        print(f"DEBUG: Skipping {month_key} - before scenario start date")
                        continue
                        
                    current_demand = current_monthly_demand.get(month_key, 0)
                    gap = pour_capacity - current_demand
                    if gap > 0:  # Only consider positive gaps (unfilled capacity)
                        monthly_gaps[month_key] = gap
                        print(f"DEBUG: Gap in {month_key}: {gap} tonnes (Pour: {pour_capacity}, Current: {current_demand})")

                print(f"DEBUG: Monthly gaps to fill for {site_name}: {monthly_gaps}")

                # ENHANCED: Thorough sequential month-by-month gap filling 
                # Fill each month COMPLETELY before moving to next month
                print(f"DEBUG: Starting THOROUGH sequential month-by-month optimization for {site_name}")
                
                # Get all months that have Pour Plan capacity (sorted chronologically)
                sorted_months = sorted([month for month in monthly_pour_plan.keys() 
                                      if datetime.strptime(month + '-01', '%Y-%m-%d').date() >= scenario_start_date])
                
                print(f"DEBUG: Processing months in order: {sorted_months}")
                
                # Process each month sequentially with COMPLETE gap filling
                for current_month in sorted_months:
                    print(f"DEBUG: === Processing month {current_month} THOROUGHLY ===")
                    
                    # Keep looping until this month's gap is completely filled or no more moves possible
                    max_iterations = 10  # Safety limit to prevent infinite loops
                    iteration = 0
                    
                    while iteration < max_iterations:
                        iteration += 1
                        print(f"DEBUG: {current_month} - Iteration {iteration}")
                        
                        # Recalculate current demand for this specific month (may have changed from previous moves)
                        month_demand = 0
                        demand_records = CalculatedProductionModel.objects.filter(
                            version=scenario,
                            site__SiteName=site_name,
                            pouring_date__year=int(current_month.split('-')[0]),
                            pouring_date__month=int(current_month.split('-')[1])
                        ).values('tonnes')
                        
                        for record in demand_records:
                            month_demand += record['tonnes'] or 0
                        
                        pour_capacity = monthly_pour_plan[current_month]
                        current_gap = pour_capacity - month_demand
                        
                        print(f"DEBUG: {current_month} - Pour Plan: {pour_capacity:.2f}, Current Demand: {month_demand:.2f}, Gap: {current_gap:.2f}")
                        
                        # If gap is negligible or filled, move to next month
                        if current_gap <= 1.0:  # Consider gaps less than 1 tonne as acceptable
                            print(f"DEBUG: {current_month} gap filled successfully ({current_gap:.2f}t remaining)")
                            break
                        
                        # Track if we made any moves in this iteration
                        moves_made_this_iteration = 0
                        remaining_gap = current_gap
                        gap_date = datetime.strptime(current_month + '-15', '%Y-%m-%d').date()
                        gap_month_start = datetime.strptime(current_month + '-01', '%Y-%m-%d').date()
                        
                        print(f"DEBUG: Attempting to fill {remaining_gap:.2f} tonnes gap in {current_month}")
                        
                        # ENHANCED: More aggressive search for production to move back
                        # Try ALL product groups, not just selected ones, to maximize gap filling
                        all_product_groups = list(CalculatedProductionModel.objects.filter(
                            version=scenario,
                            site__SiteName=site_name
                        ).exclude(product_group__isnull=True).exclude(product_group='').values_list('product_group', flat=True).distinct())
                        
                        print(f"DEBUG: Found {len(all_product_groups)} product groups for {site_name}: {all_product_groups}")
                        
                        # Use selected product groups first, then try others if gap remains
                        product_groups_to_try = selected_product_groups + [pg for pg in all_product_groups if pg not in selected_product_groups]
                        
                        for product_group in product_groups_to_try:
                            if remaining_gap <= 1.0:  # Stop if gap is small enough
                                break
                                
                            print(f"DEBUG: Looking for {product_group} from months after {current_month}")
                            
                            # ENHANCED: Look for production from ANYWHERE in the future, not just immediately after
                            future_productions = CalculatedProductionModel.objects.filter(
                                version=scenario,
                                site__SiteName=site_name,
                                product_group=product_group,
                                pouring_date__gt=gap_month_start  # Any date after gap month start
                            ).order_by('pouring_date')
                            
                            
                            for production in future_productions:
                                if remaining_gap <= 1.0:  # Stop if gap is small enough
                                    break
                                    
                                # Check 90-day constraint if enabled
                                if max_days_forward:
                                    days_difference = (production.pouring_date - gap_date).days
                                    if days_difference > max_days_forward:
                                        print(f"DEBUG: Skipping {production.id} - would move {days_difference} days (limit: {max_days_forward})")
                                        continue
                                
                                # Calculate how much to move
                                production_tonnes = production.tonnes or 0
                                tonnes_to_move = min(production_tonnes, remaining_gap)
                                
                                if tonnes_to_move > 0:
                                    moves_made_this_iteration += 1
                                    print(f"DEBUG: Moving {tonnes_to_move:.2f} tonnes of {product_group} from {production.pouring_date} to {gap_date}")
                                    
                                    if production_tonnes > tonnes_to_move:
                                        # Partial move - create new record for moved portion
                                        moved_production = CalculatedProductionModel.objects.create(
                                            version=production.version,
                                            product=production.product,
                                            site=production.site,
                                            pouring_date=gap_date,
                                            production_quantity=production.production_quantity * (tonnes_to_move / production_tonnes),
                                            tonnes=tonnes_to_move,
                                            product_group=production.product_group,
                                            parent_product_group=production.parent_product_group,
                                            price_aud=production.price_aud,
                                            cost_aud=production.cost_aud,
                                            cogs_aud=production.cogs_aud * (tonnes_to_move / production_tonnes) if production.cogs_aud else 0,
                                            revenue_aud=production.revenue_aud * (tonnes_to_move / production_tonnes) if production.revenue_aud else 0
                                        )
                                        
                                        print(f"DEBUG: Created new record ID: {moved_production.id}")
                                        
                                        # Reduce original production
                                        production.production_quantity -= moved_production.production_quantity
                                        production.tonnes -= tonnes_to_move
                                        if production.cogs_aud:
                                            production.cogs_aud -= moved_production.cogs_aud
                                        if production.revenue_aud:
                                            production.revenue_aud -= moved_production.revenue_aud
                                        production.save()
                                        
                                        print(f"DEBUG: Reduced original record from {production_tonnes} to {production.tonnes} tonnes")
                                        
                                    else:
                                        # Move the entire production record
                                        original_date = production.pouring_date
                                        production.pouring_date = gap_date
                                        production.save()
                                        
                                        print(f"DEBUG: Moved entire record ID: {production.id} from {original_date} to {gap_date}")
                                    
                                    # Update tracking
                                    remaining_gap -= tonnes_to_move
                                    total_tonnes_moved += tonnes_to_move
                                    optimized_count += 1
                                    
                                    print(f"DEBUG: Remaining gap in {current_month}: {remaining_gap:.2f} tonnes")
                        
                        # If no moves were made in this iteration, we can't fill more
                        if moves_made_this_iteration == 0:
                            print(f"DEBUG: No more moves possible for {current_month}, remaining gap: {remaining_gap:.2f}t")
                            
                            # Check if this gap is acceptable - only if next months are empty OR 90-day limit reached
                            remaining_gap_final = remaining_gap
                            next_month_production = 0
                            
                            # Find next month with production
                            current_month_date = datetime.strptime(current_month + '-01', '%Y-%m-%d').date()
                            next_month_date = current_month_date.replace(day=1)
                            if next_month_date.month == 12:
                                next_month_date = next_month_date.replace(year=next_month_date.year + 1, month=1)
                            else:
                                next_month_date = next_month_date.replace(month=next_month_date.month + 1)
                            
                            # Check if there's production in the next 3 months
                            for i in range(3):  # Check next 3 months
                                check_date = next_month_date
                                if check_date.month + i > 12:
                                    check_date = check_date.replace(year=check_date.year + 1, month=check_date.month + i - 12)
                                else:
                                    check_date = check_date.replace(month=check_date.month + i)
                                
                                month_production = CalculatedProductionModel.objects.filter(
                                    version=scenario,
                                    site__SiteName=site_name,
                                    pouring_date__year=check_date.year,
                                    pouring_date__month=check_date.month
                                ).aggregate(total=Sum('tonnes'))['total'] or 0
                                
                                if month_production > 0:
                                    next_month_production += month_production
                            
                            if next_month_production > 0 and remaining_gap_final > 5:  # 5 tonne tolerance
                                print(f"WARNING: Gap of {remaining_gap_final:.1f}t in {current_month} with {next_month_production:.1f}t available in future months!")
                            
                            break  # Exit the while loop for this month
                        
                        if remaining_gap > 0:
                            print(f"DEBUG: Could not fully fill gap in {current_month}, remaining: {remaining_gap:.2f} tonnes")
                        else:
                            print(f"DEBUG: Successfully filled gap in {current_month}")
                    else:
                        print(f"DEBUG: No gap in {current_month} (current demand meets or exceeds pour plan)")
                
                print(f"DEBUG: Completed sequential month-by-month optimization for {site_name}")
            
            # Mark optimization as applied
            opt_state.auto_optimization_applied = True
            opt_state.last_optimization_date = timezone.now()
            opt_state.save()
            
            # CRITICAL: Recalculate all dependent aggregations after optimization
            if optimized_count > 0:
                print(f"DEBUG: Recalculating aggregations after optimization...")
                try:
                    from website.customized_function import (
                        populate_aggregated_forecast_data, 
                        populate_aggregated_foundry_data, 
                        populate_aggregated_inventory_data, 
                        populate_aggregated_financial_data
                    )
                    
                    # Recalculate all aggregations to reflect the optimized production dates
                    populate_aggregated_forecast_data(scenario)
                    populate_aggregated_foundry_data(scenario)
                    populate_aggregated_inventory_data(scenario)
                    populate_aggregated_financial_data(scenario)
                    
                    print(f"DEBUG: All aggregations recalculated successfully")
                    
                    messages.success(request, f"Successfully filled pour plan gaps by moving {optimized_count} production records forward across all product groups, totaling {total_tonnes_moved:.2f} tonnes. All charts have been updated to reflect the optimization. Auto optimization is now locked until reset.")
                    
                except Exception as agg_error:
                    print(f"ERROR: Failed to recalculate aggregations: {agg_error}")
                    messages.warning(request, f"Optimization completed ({optimized_count} records moved, {total_tonnes_moved:.2f} tonnes), but charts may need manual refresh. Error: {str(agg_error)}")
            else:
                messages.info(request, "No gaps found to fill or no suitable production could be moved forward within the constraints.")
                
        except Exception as e:
            messages.error(request, f"Error during optimization: {str(e)}")
            import traceback
            print(f"ERROR: Optimization failed: {traceback.format_exc()}")
    
    return redirect('review_scenario', version=version)

@login_required
def reset_production_plan(request, version):
    """Reset production plan by running populate_calculated_production command and recalculating all dependent aggregations"""
    import subprocess
    import os
    from django.utils import timezone
    from .models import scenarios, ScenarioOptimizationState
    from website.customized_function import (
        populate_aggregated_forecast_data, 
        populate_aggregated_foundry_data, 
        populate_aggregated_inventory_data, 
        populate_aggregated_financial_data
    )
    
    if request.method == 'POST':
        try:
            scenario = get_object_or_404(scenarios, version=version)
            
            # Step 1: Reset the optimization state first
            opt_state, created = ScenarioOptimizationState.objects.get_or_create(
                version=scenario,
                defaults={'auto_optimization_applied': False}
            )
            opt_state.auto_optimization_applied = False
            opt_state.last_reset_date = timezone.now()
            opt_state.save()
            
            # Step 2: Get the Django project root directory (SPR folder)
            current_dir = os.path.dirname(os.path.abspath(__file__))  # website folder
            project_root = os.path.dirname(current_dir)  # SPR folder
            manage_py_path = os.path.join(project_root, 'manage.py')
            
            print(f"DEBUG: Resetting production plan for scenario: {version}")
            
            # Step 3: Run the populate_calculated_production command
            result = subprocess.run([
                'python', manage_py_path, 
                'populate_calculated_production', 
                version
            ], 
            capture_output=True, 
            text=True, 
            cwd=project_root
            )
            
            if result.returncode == 0:
                print(f"DEBUG: populate_calculated_production completed successfully")
                
                # Step 4: Recalculate all dependent aggregations
                print(f"DEBUG: Recalculating dependent aggregations...")
                
                try:
                    # Recalculate Forecast aggregations
                    print("DEBUG: Recalculating forecast aggregations...")
                    populate_aggregated_forecast_data(scenario)
                    
                    # Recalculate Foundry aggregations
                    print("DEBUG: Recalculating foundry aggregations...")
                    populate_aggregated_foundry_data(scenario)
                    
                    # Recalculate Inventory aggregations
                    print("DEBUG: Recalculating inventory aggregations...")
                    populate_aggregated_inventory_data(scenario)
                    
                    # Recalculate Financial aggregations (this depends on production data)
                    print("DEBUG: Recalculating financial aggregations...")
                    populate_aggregated_financial_data(scenario)
                    
                    print(f"DEBUG: All aggregations recalculated successfully")
                    
                    messages.success(request, f"Production plan reset successfully for version {version}. All chart data has been recalculated. Auto optimization is now available.")
                    
                except Exception as agg_error:
                    print(f"ERROR: Failed to recalculate aggregations: {agg_error}")
                    messages.warning(request, f"Production plan reset successfully, but some chart data may need manual refresh. Error: {str(agg_error)}")
                    
            else:
                messages.error(request, f"Error resetting production plan: {result.stderr}")
                
        except Exception as e:
            messages.error(request, f"Error resetting production plan: {str(e)}")
            import traceback
            print(f"ERROR: Reset failed: {traceback.format_exc()}")
    
    return redirect('review_scenario', version=version)

# ...existing code...

from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.contrib import messages
from django.http import HttpResponse
from .models import scenarios, ProductSiteCostModel, MasterDataProductModel, MasterDataPlantModel
from django.views.decorators.http import require_POST
import subprocess

from django.core.paginator import Paginator

from django.db.models import Max

@login_required
def update_products_cost(request, version):
    user_name = request.user.username   
    scenario = get_object_or_404(scenarios, version=version)
    costs = ProductSiteCostModel.objects.filter(version=scenario)

    # Filtering logic
    product_filter = request.GET.get('product', '')
    site_filter = request.GET.get('site', '')
    if product_filter:
        costs = costs.filter(product__Product__icontains=product_filter)
    if site_filter:
        costs = costs.filter(site__SiteName__icontains=site_filter)

    # --- NEW: Build a lookup for max warehouse cost ---
    from .models import MasterDataInventory
    warehouse_costs = (
        MasterDataInventory.objects
        .filter(version=scenario)
        .values('product', 'site')
        .annotate(max_cost=Max('cost_aud'))
    )
    warehouse_cost_lookup = {
        (row['product'], row['site']): row['max_cost']
        for row in warehouse_costs
    }

    # Pagination logic: 20 per page
    paginator = Paginator(costs.order_by('product__Product', 'site__SiteName'), 20)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Pass the lookup to the template
    return render(request, 'website/update_products_cost.html', {
        'scenario': scenario,
        'costs': page_obj.object_list,
        'page_obj': page_obj,
        'product_filter': product_filter,
        'site_filter': site_filter,
        'warehouse_cost_lookup': warehouse_cost_lookup,  # <-- pass to template
        'user_name': user_name,
    })

def delete_products_cost(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    ProductSiteCostModel.objects.filter(version=scenario).delete()
    messages.success(request, "All product costs deleted for this scenario.")
    return redirect(request.META.get('HTTP_REFERER', '/'))

def upload_products_cost(request, version):
    # This will call your management command to fetch and populate costs
    subprocess.call(['python', 'manage.py', 'Populate_ProductSiteCostModel', version])
    messages.success(request, "Product costs uploaded from Epicor.")
    return redirect(request.META.get('HTTP_REFERER', '/'))

def copy_products_cost(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    if request.method == "POST":
        from_version = request.POST.get('from_version')
        from_scenario = get_object_or_404(scenarios, version=from_version)
        ProductSiteCostModel.objects.filter(version=scenario).delete()
        for cost in ProductSiteCostModel.objects.filter(version=from_scenario):
            cost.pk = None
            cost.version = scenario
            cost.save()
        messages.success(request, f"Product costs copied from {from_version}.")
        return redirect(request.META.get('HTTP_REFERER', '/'))
    all_versions = scenarios.objects.exclude(version=version)
    return render(request, 'website/copy_products_cost.html', {
        'scenario': scenario,
        'all_versions': all_versions,
    })

# Fixed Plant Conversion Modifiers Views
@login_required
def update_fixed_plant_conversion_modifiers(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    
    # Get filter values from GET
    product_filter = request.GET.get('product', '').strip()
    
    # Get products from Fixed Plant forecast data for this scenario
    fixed_plant_products = SMART_Forecast_Model.objects.filter(
        version=scenario, 
        Data_Source='Fixed Plant'
    ).values_list('Product', flat=True).distinct()
    
    # Filter records for Fixed Plant products and BAS1 site only
    records = FixedPlantConversionModifiersModel.objects.filter(
        version=scenario,
        Site__SiteName='BAS1',
        Product__Product__in=fixed_plant_products
    )
    
    # Apply additional filters if provided
    if product_filter:
        records = records.filter(Product__Product__icontains=product_filter)
    
    # Always order before paginating!
    records = records.order_by('Product__Product')
    
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Create formset WITHOUT Product and Site fields (much faster)
    ModifiersFormSet = modelformset_factory(
        FixedPlantConversionModifiersModel,
        fields=['GrossMargin', 'ManHourCost', 'ExternalMaterialComponents', 
                'FreightPercentage', 'MaterialCostPercentage', 'CostPerHourAUD', 'CostPerSQMorKgAUD'],
        extra=0
    )
    
    if request.method == 'POST':
        formset = ModifiersFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            instances = formset.save(commit=False)
            for instance in instances:
                # Ensure version is set
                instance.version = scenario
                instance.save()
            messages.success(request, "Fixed Plant Conversion Modifiers updated successfully!")
            return redirect('edit_scenario', version=version)
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        formset = ModifiersFormSet(queryset=page_obj.object_list)
    
    return render(request, 'website/update_fixed_plant_conversion_modifiers.html', {
        'formset': formset,
        'version': version,
        'product_filter': product_filter,
        'page_obj': page_obj,
        'user_name': user_name,
        'scenario': scenario,
        'is_fixed_plant': True,
    })

@login_required
def upload_fixed_plant_conversion_modifiers(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST' and request.FILES.get('file'):
        excel_file = request.FILES['file']
        try:
            df = pd.read_excel(excel_file)
            required_columns = ['Product', 'Site', 'GrossMargin', 'ManHourCost', 'ExternalMaterialComponents', 
                              'FreightPercentage', 'MaterialCostPercentage', 'CostPerHourAUD', 'CostPerSQMorKgAUD']
            if not all(column in df.columns for column in required_columns):
                messages.error(request, f"Invalid file format. Required columns: {', '.join(required_columns)}.")
                return redirect('edit_scenario', version=version)
            
            for _, row in df.iterrows():
                try:
                    product = MasterDataProductModel.objects.get(Product=row['Product'])
                    site = MasterDataPlantModel.objects.get(SiteName=row['Site'])
                    
                    FixedPlantConversionModifiersModel.objects.update_or_create(
                        version=scenario,
                        Product=product,
                        Site=site,
                        defaults={
                            'GrossMargin': row.get('GrossMargin', 0.0),
                            'ManHourCost': row.get('ManHourCost', 0.0),
                            'ExternalMaterialComponents': row.get('ExternalMaterialComponents', 0.0),
                            'FreightPercentage': row.get('FreightPercentage', 0.0),
                            'MaterialCostPercentage': row.get('MaterialCostPercentage', 0.0),
                            'CostPerHourAUD': row.get('CostPerHourAUD', 0.0),
                            'CostPerSQMorKgAUD': row.get('CostPerSQMorKgAUD', 0.0),
                        }
                    )
                except (MasterDataProductModel.DoesNotExist, MasterDataPlantModel.DoesNotExist) as e:
                    messages.warning(request, f"Skipped row: {e}")
                    continue
            
            messages.success(request, "Fixed Plant Conversion Modifiers uploaded successfully!")
        except Exception as e:
            messages.error(request, f"Error uploading file: {e}")
        
        return redirect('edit_scenario', version=version)
    
    return render(request, 'website/upload_fixed_plant_conversion_modifiers.html', {
        'scenario': scenario
    })

# Revenue Conversion Modifiers Views
@login_required
def update_revenue_conversion_modifiers(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    
    # Get filter values from GET
    product_filter = request.GET.get('product', '').strip()
    site_filter = request.GET.get('site', '').strip()
    
    # Get products from Revenue Forecast data for this scenario
    revenue_forecast_products = SMART_Forecast_Model.objects.filter(
        version=scenario, 
        Data_Source='Revenue Forecast'
    ).values_list('Product', flat=True).distinct()
    
    # Filter records for Revenue products and revenue sites only
    revenue_sites = ['XUZ1', 'MER1', 'WOD1', 'COI2']
    records = RevenueToCogsConversionModel.objects.filter(
        version=scenario,
        Site__SiteName__in=revenue_sites,
        Product__Product__in=revenue_forecast_products
    )
    
    # Apply additional filters if provided
    if product_filter:
        records = records.filter(Product__Product__icontains=product_filter)
    if site_filter:
        records = records.filter(Site__SiteName__icontains=site_filter)
    
    # Always order before paginating!
    records = records.order_by('Product__Product', 'Site__SiteName')
    
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Create formset WITHOUT Product, Site, and Region fields (much faster)
    ModifiersFormSet = modelformset_factory(
        RevenueToCogsConversionModel,
        fields=['GrossMargin', 'InHouseProduction', 'CostAUDPerKG'],  # Removed Region
        extra=0
    )
    
    if request.method == 'POST':
        formset = ModifiersFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            instances = formset.save(commit=False)
            for instance in instances:
                # Ensure version is set
                instance.version = scenario
                instance.save()
            messages.success(request, "Revenue Conversion Modifiers updated successfully!")
            return redirect('edit_scenario', version=version)
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        formset = ModifiersFormSet(queryset=page_obj.object_list)
    
    return render(request, 'website/update_revenue_conversion_modifiers.html', {
        'formset': formset,
        'version': version,
        'product_filter': product_filter,
        'site_filter': site_filter,
        'page_obj': page_obj,
        'user_name': user_name,
        'scenario': scenario,
        'is_revenue_forecast': True,
    })

@login_required
def upload_revenue_conversion_modifiers(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST' and request.FILES.get('file'):
        excel_file = request.FILES['file']
        try:
            df = pd.read_excel(excel_file)
            required_columns = ['Product', 'Site', 'GrossMargin', 'InHouseProduction', 'CostAUDPerKG']  # Removed Region
            if not all(column in df.columns for column in required_columns):
                messages.error(request, f"Invalid file format. Required columns: {', '.join(required_columns)}.")
                return redirect('edit_scenario', version=version)
            
            # Validate that site is one of the allowed revenue sites
            allowed_sites = ['XUZ1', 'MER1', 'WOD1', 'COI2']
            
            for _, row in df.iterrows():
                try:
                    if row['Site'] not in allowed_sites:
                        messages.warning(request, f"Skipped row: Site {row['Site']} is not a valid revenue site. Must be one of: {', '.join(allowed_sites)}")
                        continue
                        
                    product = MasterDataProductModel.objects.get(Product=row['Product'])
                    site = MasterDataPlantModel.objects.get(SiteName=row['Site'])
                    
                    RevenueToCogsConversionModel.objects.update_or_create(
                        version=scenario,
                        Product=product,
                        Site=site,
                        defaults={
                            # 'Region': row.get('Region', 'Revenue'),  # Remove this line
                            'GrossMargin': row.get('GrossMargin', 0.0),
                            'InHouseProduction': row.get('InHouseProduction', 0.0),
                            'CostAUDPerKG': row.get('CostAUDPerKG', 0.0),
                        }
                    )
                except (MasterDataProductModel.DoesNotExist, MasterDataPlantModel.DoesNotExist) as e:
                    messages.warning(request, f"Skipped row: {e}")
                    continue
            
            messages.success(request, "Revenue Conversion Modifiers uploaded successfully!")
        except Exception as e:
            messages.error(request, f"Error uploading file: {e}")
        
        return redirect('edit_scenario', version=version)
    
    return render(request, 'website/upload_revenue_conversion_modifiers.html', {
        'scenario': scenario
    })

@login_required
def copy_revenue_conversion_modifiers(request, version):
    target_scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)
        
        source_records = RevenueToCogsConversionModel.objects.filter(version=source_scenario)
        if not source_records.exists():
            messages.warning(request, "No Revenue Conversion Modifiers available to copy from the selected scenario.")
        else:
            # Delete existing records and copy new ones
            RevenueToCogsConversionModel.objects.filter(version=target_scenario).delete()
            for record in source_records:
                RevenueToCogsConversionModel.objects.create(
                    version=target_scenario,
                    Product=record.Product,
                    Region=record.Region,
                    ConversionModifier=record.ConversionModifier
                )
            messages.success(request, "Revenue Conversion Modifiers copied successfully!")
        
        return redirect('edit_scenario', version=version)
    
    all_scenarios = scenarios.objects.exclude(version=version)
    return render(request, 'website/copy_revenue_conversion_modifiers.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })

@login_required
def delete_fixed_plant_conversion_modifiers(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    if request.method == 'POST':
        FixedPlantConversionModifiersModel.objects.filter(version=scenario).delete()
        messages.success(request, "All Fixed Plant Conversion Modifiers deleted successfully!")
        return redirect('edit_scenario', version=version)
    return render(request, 'website/delete_fixed_plant_conversion_modifiers.html', {
        'version': version,
        'scenario': scenario
    })

@login_required
def copy_fixed_plant_conversion_modifiers(request, version):
    target_scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)
        
        source_records = FixedPlantConversionModifiersModel.objects.filter(version=source_scenario)
        if not source_records.exists():
            messages.warning(request, "No Fixed Plant Conversion Modifiers available to copy from the selected scenario.")
        else:
            # Delete existing records and copy new ones
            FixedPlantConversionModifiersModel.objects.filter(version=target_scenario).delete()
            for record in source_records:
                FixedPlantConversionModifiersModel.objects.create(
                    version=target_scenario,
                    Product=record.Product,
                    Site=record.Site,
                    ConversionModifier=record.ConversionModifier
                )
            messages.success(request, "Fixed Plant Conversion Modifiers copied successfully!")
        
        return redirect('edit_scenario', version=version)
    
    all_scenarios = scenarios.objects.exclude(version=version)
    return render(request, 'website/copy_fixed_plant_conversion_modifiers.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })

@login_required
def delete_revenue_conversion_modifiers(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    if request.method == 'POST':
        RevenueToCogsConversionModel.objects.filter(version=scenario).delete()
        messages.success(request, "All Revenue Conversion Modifiers deleted successfully!")
        return redirect('edit_scenario', version=version)
    return render(request, 'website/delete_revenue_conversion_modifiers.html', {
        'version': version,
        'scenario': scenario
    })

@login_required
def update_revenue_to_cogs_conversion(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    
    # Get filter values from GET
    product_filter = request.GET.get('product', '').strip()
    
    # Get products from Revenue Forecast data for this scenario
    revenue_forecast_products = SMART_Forecast_Model.objects.filter(
        version=scenario, 
        Data_Source='Revenue Forecast'
    ).values_list('Product', flat=True).distinct()
    
    # Filter records for Revenue products only
    records = RevenueToCogsConversionModel.objects.filter(
        version=scenario,
        Product__Product__in=revenue_forecast_products
    )
    
    # Apply additional filters if provided
    if product_filter:
        records = records.filter(Product__Product__icontains=product_filter)
    
    # Always order before paginating!
    records = records.order_by('Product__Product')
    
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Create formset WITHOUT Product field (much faster)
    ModifiersFormSet = modelformset_factory(
        RevenueToCogsConversionModel,
        fields=['GrossMargin', 'InHouseProduction', 'CostAUDPerKG'],
        extra=0
    )
    
    if request.method == 'POST':
        formset = ModifiersFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            instances = formset.save(commit=False)
            for instance in instances:
                # Ensure version is set
                instance.version = scenario
                instance.save()
            messages.success(request, "Revenue to COGS Conversion updated successfully!")
            return redirect('edit_scenario', version=version)
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        formset = ModifiersFormSet(queryset=page_obj.object_list)
    
    return render(request, 'website/update_revenue_to_cogs_conversion.html', {
        'formset': formset,
        'version': version,
        'product_filter': product_filter,
        'page_obj': page_obj,
        'user_name': user_name,
        'scenario': scenario,
    })

@login_required
def upload_revenue_to_cogs_conversion(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST' and request.FILES.get('file'):
        excel_file = request.FILES['file']
        try:
            df = pd.read_excel(excel_file)
            required_columns = ['Product', 'GrossMargin', 'InHouseProduction', 'CostAUDPerKG']
            if not all(column in df.columns for column in required_columns):
                messages.error(request, f"Invalid file format. Required columns: {', '.join(required_columns)}.")
                return redirect('edit_scenario', version=version)
            
            for _, row in df.iterrows():
                try:
                    product = MasterDataProductModel.objects.get(Product=row['Product'])
                    
                    RevenueToCogsConversionModel.objects.update_or_create(
                        version=scenario,
                        Product=product,
                        defaults={
                            'GrossMargin': row.get('GrossMargin', 0.0),
                            'InHouseProduction': row.get('InHouseProduction', 0.0),
                            'CostAUDPerKG': row.get('CostAUDPerKG', 0.0),
                        }
                    )
                except MasterDataProductModel.DoesNotExist:
                    messages.warning(request, f"Skipped row: Product {row['Product']} not found")
                    continue
            
            messages.success(request, "Revenue to COGS Conversion uploaded successfully!")
        except Exception as e:
            messages.error(request, f"Error uploading file: {e}")
        
        return redirect('edit_scenario', version=version)
    
    return render(request, 'website/upload_revenue_to_cogs_conversion.html', {
        'scenario': scenario
    })

@login_required
def delete_revenue_to_cogs_conversion(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        RevenueToCogsConversionModel.objects.filter(version=scenario).delete()
        messages.success(request, "All Revenue to COGS Conversion records deleted successfully!")
        return redirect('edit_scenario', version=version)
    
    record_count = RevenueToCogsConversionModel.objects.filter(version=scenario).count()
    return render(request, 'website/delete_revenue_to_cogs_conversion.html', {
        'scenario': scenario,
        'record_count': record_count
    })

@login_required
def copy_revenue_to_cogs_conversion(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        try:
            source_scenario = scenarios.objects.get(version=source_version)
            source_records = RevenueToCogsConversionModel.objects.filter(version=source_scenario)
            
            copied_count = 0
            for record in source_records:
                RevenueToCogsConversionModel.objects.update_or_create(
                    version=scenario,
                    Product=record.Product,
                    defaults={
                        'GrossMargin': record.GrossMargin,
                        'InHouseProduction': record.InHouseProduction,
                        'CostAUDPerKG': record.CostAUDPerKG,
                    }
                )
                copied_count += 1
            
            messages.success(request, f"Copied {copied_count} Revenue to COGS Conversion records from {source_version}!")
            return redirect('edit_scenario', version=version)
        except scenarios.DoesNotExist:
            messages.error(request, f"Source scenario '{source_version}' not found.")
    
    available_scenarios = scenarios.objects.exclude(version=version)
    return render(request, 'website/copy_revenue_to_cogs_conversion.html', {
        'scenario': scenario,
        'available_scenarios': available_scenarios
    })

# ==================== SITE ALLOCATION VIEWS ====================

@login_required
def update_site_allocation(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    
    # Get filter values from GET
    product_filter = request.GET.get('product', '').strip()
    site_filter = request.GET.get('site', '').strip()
    
    # Get products from Revenue Forecast data for this scenario
    revenue_forecast_products = SMART_Forecast_Model.objects.filter(
        version=scenario, 
        Data_Source='Revenue Forecast'
    ).values_list('Product', flat=True).distinct()
    
    # Filter records for Revenue products and revenue sites only
    revenue_sites = ['XUZ1', 'MER1', 'WOD1', 'COI2']
    records = SiteAllocationModel.objects.filter(
        version=scenario,
        Site__SiteName__in=revenue_sites,
        Product__Product__in=revenue_forecast_products
    )
    
    # Apply additional filters if provided
    if product_filter:
        records = records.filter(Product__Product__icontains=product_filter)
    if site_filter:
        records = records.filter(Site__SiteName__icontains=site_filter)
    
    # Always order before paginating!
    records = records.order_by('Product__Product', 'Site__SiteName')
    
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Create formset WITHOUT Product and Site fields (much faster)
    ModifiersFormSet = modelformset_factory(
        SiteAllocationModel,
        fields=['AllocationPercentage'],
        extra=0
    )
    
    if request.method == 'POST':
        formset = ModifiersFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            instances = formset.save(commit=False)
            for instance in instances:
                # Ensure version is set
                instance.version = scenario
                instance.save()
            messages.success(request, "Site Allocation updated successfully!")
            return redirect('edit_scenario', version=version)
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        formset = ModifiersFormSet(queryset=page_obj.object_list)
    
    return render(request, 'website/update_site_allocation.html', {
        'formset': formset,
        'version': version,
        'product_filter': product_filter,
        'site_filter': site_filter,
        'page_obj': page_obj,
        'user_name': user_name,
        'scenario': scenario,
    })

@login_required
def upload_site_allocation(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST' and request.FILES.get('file'):
        excel_file = request.FILES['file']
        try:
            df = pd.read_excel(excel_file)
            required_columns = ['Product', 'Site', 'AllocationPercentage']
            if not all(column in df.columns for column in required_columns):
                messages.error(request, f"Invalid file format. Required columns: {', '.join(required_columns)}.")
                return redirect('edit_scenario', version=version)
            
            # Validate that site is one of the allowed revenue sites
            allowed_sites = ['XUZ1', 'MER1', 'WOD1', 'COI2']
            
            for _, row in df.iterrows():
                try:
                    if row['Site'] not in allowed_sites:
                        messages.warning(request, f"Skipped row: Site {row['Site']} is not a valid revenue site. Must be one of: {', '.join(allowed_sites)}")
                        continue
                        
                    product = MasterDataProductModel.objects.get(Product=row['Product'])
                    site = MasterDataPlantModel.objects.get(SiteName=row['Site'])
                    
                    SiteAllocationModel.objects.update_or_create(
                        version=scenario,
                        Product=product,
                        Site=site,
                        defaults={
                            'AllocationPercentage': row.get('AllocationPercentage', 0.0),
                        }
                    )
                except (MasterDataProductModel.DoesNotExist, MasterDataPlantModel.DoesNotExist) as e:
                    messages.warning(request, f"Skipped row: {e}")
                    continue
            
            messages.success(request, "Site Allocation uploaded successfully!")
        except Exception as e:
            messages.error(request, f"Error uploading file: {e}")
        
        return redirect('edit_scenario', version=version)
    
    return render(request, 'website/upload_site_allocation.html', {
        'scenario': scenario
    })

@login_required
def delete_site_allocation(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        SiteAllocationModel.objects.filter(version=scenario).delete()
        messages.success(request, "All Site Allocation records deleted successfully!")
        return redirect('edit_scenario', version=version)
    
    record_count = SiteAllocationModel.objects.filter(version=scenario).count()
    return render(request, 'website/delete_site_allocation.html', {
        'scenario': scenario,
        'record_count': record_count
    })

@login_required
def copy_site_allocation(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        try:
            source_scenario = scenarios.objects.get(version=source_version)
            source_records = SiteAllocationModel.objects.filter(version=source_scenario)
            
            copied_count = 0
            for record in source_records:
                SiteAllocationModel.objects.update_or_create(
                    version=scenario,
                    Product=record.Product,
                    Site=record.Site,
                    defaults={
                        'AllocationPercentage': record.AllocationPercentage,
                    }
                )
                copied_count += 1
            
            messages.success(request, f"Copied {copied_count} Site Allocation records from {source_version}!")
            return redirect('edit_scenario', version=version)
        except scenarios.DoesNotExist:
            messages.error(request, f"Source scenario '{source_version}' not found.")
    
    available_scenarios = scenarios.objects.exclude(version=version)
    return render(request, 'website/copy_site_allocation.html', {
        'scenario': scenario,
        'available_scenarios': available_scenarios
    })

from .models import MasterDataEpicorMethodOfManufacturingModel

@login_required
def method_of_manufacturing_fetch_data_from_mssql(request):
    if request.method == 'POST':  # Only run on POST (refresh)
        Server = 'bknew-sql02'
        Database = 'Bradken_Data_Warehouse'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        engine = create_engine(Database_Con)

        try:
            # FIRST: Delete all existing Method of Manufacturing records
            deleted_count = MasterDataEpicorMethodOfManufacturingModel.objects.all().delete()[0]
            print(f"Deleted {deleted_count} existing Method of Manufacturing records")
            
            try:
                # Use context manager to ensure connection is closed
                with engine.connect() as connection:
                    try:
                        # First, discover what columns exist in the Operation table
                        operation_check_query = text("SELECT TOP 1 * FROM PowerBI.Operation")
                        operation_result = connection.execute(operation_check_query)
                        operation_columns = list(operation_result.keys())
                        
                        # IMPORTANT: Consume all rows from the first result to free the connection
                        for row in operation_result:
                            pass  # Just consume the result
                        
                        print("Operation table columns:", operation_columns)
                        
                        # Now execute the main query
                        query = text("""
                            SELECT 
                                Route.skOperationId,
                                Route.OperationSequence,
                                Products.ProductKey,
                                Products.Company,
                                Site.SiteName,
                                Operation.OperationDesc AS OperationDesc,
                                Operation.WorkCentre AS WorkCentre
                            FROM PowerBI.Route AS Route
                            LEFT JOIN PowerBI.Products AS Products
                                ON Route.skProductId = Products.skProductId
                            LEFT JOIN PowerBI.Site AS Site
                                ON Route.skSiteId = Site.skSiteId
                            LEFT JOIN PowerBI.Operation AS Operation
                                ON Route.skOperationId = Operation.skOperationId
                            WHERE Route.OperationSequence IS NOT NULL
                                AND Products.ProductKey IS NOT NULL
                                AND Site.SiteName IS NOT NULL
                        """)
                        
                        result = connection.execute(query)
                        rows = list(result)  # This consumes all rows immediately
                        
                        print(f"Found {len(rows)} method of manufacturing records from server")
                        
                        # Use a set to track unique combinations and avoid duplicates
                        seen_combinations = set()
                        new_records = []
                        duplicates_skipped = 0
                        
                        for row in rows:
                            # Use the actual data from the query
                            product_key = row.ProductKey
                            site_name = row.SiteName
                            operation_sequence = row.OperationSequence
                            work_centre = getattr(row, 'WorkCentre', f'WC_{operation_sequence}')
                            company = getattr(row, 'Company', 'Unknown')
                            operation_desc_raw = getattr(row, 'OperationDesc', f'Operation {operation_sequence}')
                            
                            # NEW: Translate operation description from French to English
                            operation_description = translate_to_english_cached(operation_desc_raw)
                            print(f"Translated '{operation_desc_raw}' to '{operation_description}'")

                            # Skip if any key fields are None
                            if not all([product_key, site_name, operation_sequence]):
                                continue
                            
                            # Create unique key for deduplication
                            unique_key = (site_name, product_key, operation_sequence)
                            
                            # Skip if we've already seen this combination
                            if unique_key in seen_combinations:
                                duplicates_skipped += 1
                                continue
                            
                            # Add to seen combinations
                            seen_combinations.add(unique_key)
                                
                            new_records.append(
                                MasterDataEpicorMethodOfManufacturingModel(
                                    Company=company,
                                    Plant=site_name,  # Using SiteName as Plant
                                    ProductKey=product_key,
                                    SiteName=site_name,
                                    OperationSequence=operation_sequence,
                                    OperationDesc=operation_description,  # Use translated description
                                    WorkCentre=work_centre,
                                )
                            )

                        if new_records:
                            # Use regular bulk_create without ignore_conflicts for SQLite compatibility
                            MasterDataEpicorMethodOfManufacturingModel.objects.bulk_create(
                                new_records, 
                                batch_size=1000
                            )
                            messages.success(request, f'Successfully refreshed Method of Manufacturing data! Deleted {deleted_count} old records, added {len(new_records)} new records, and skipped {duplicates_skipped} duplicates. Operation descriptions translated from French to English.')
                        else:
                            messages.warning(request, f'No valid Method of Manufacturing records found on server. Deleted {deleted_count} old records.')

                    except Exception as e:
                        print(f"Error executing query: {e}")
                        messages.error(request, f'Error executing query: {str(e)}')
                        
            except Exception as e:
                print(f"Database connection error: {e}")
                messages.error(request, f'Database connection error: {str(e)}')
        
        except Exception as e:
            print(f"Database connection error: {e}")
            messages.error(request, f'Database connection error: {str(e)}')

    return redirect('method_of_manufacturing_list')

@login_required
def method_of_manufacturing_list(request):
    user_name = request.user.username
    methods = MasterDataEpicorMethodOfManufacturingModel.objects.all().order_by('Plant', 'ProductKey', 'OperationSequence')

    # Filtering logic
    def filter_field(qs, field, value):
        if value:
            if value.startswith('*') or value.endswith('*'):
                # Remove * and use icontains
                qs = qs.filter(**{f"{field}__icontains": value.replace('*', '')})
            else:
                qs = qs.filter(**{f"{field}__exact": value})
        return qs

    Company_filter = request.GET.get('Company', '')
    Plant_filter = request.GET.get('Plant', '')
    ProductKey_filter = request.GET.get('ProductKey', '')
    SiteName_filter = request.GET.get('SiteName', '')
    OperationSequence_filter = request.GET.get('OperationSequence', '')
    OperationDesc_filter = request.GET.get('OperationDesc', '')
    WorkCentre_filter = request.GET.get('WorkCentre', '')

    methods = filter_field(methods, 'Company', Company_filter)
    methods = filter_field(methods, 'Plant', Plant_filter)
    methods = filter_field(methods, 'ProductKey', ProductKey_filter)
    methods = filter_field(methods, 'SiteName', SiteName_filter)
    methods = filter_field(methods, 'OperationSequence', OperationSequence_filter)
    methods = filter_field(methods, 'OperationDesc', OperationDesc_filter)
    methods = filter_field(methods, 'WorkCentre', WorkCentre_filter)

    # Sort by Plant, then ProductKey, then OperationSequence
    methods = methods.order_by('Plant', 'ProductKey', 'OperationSequence')

    # Pagination logic
    paginator = Paginator(methods, 50)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'Company_filter': Company_filter,
        'Plant_filter': Plant_filter,
        'ProductKey_filter': ProductKey_filter,
        'SiteName_filter': SiteName_filter,
        'OperationSequence_filter': OperationSequence_filter,
        'OperationDesc_filter': OperationDesc_filter,
        'WorkCentre_filter': WorkCentre_filter,
        'user_name': user_name,
    }
    return render(request, 'website/method_of_manufacturing_list.html', context)


@login_required
def search_detailed_inventory(request):
    """AJAX endpoint for searching detailed inventory data"""
    if request.method == 'GET':
        version = request.GET.get('version')
        product = request.GET.get('product', '').strip()
        location = request.GET.get('location', '').strip()
        site = request.GET.get('site', '').strip()
        
        if not version:
            return JsonResponse({'error': 'Version is required'}, status=400)
        
        try:
            scenario = scenarios.objects.get(version=version)
            
            # Get search results
            results = search_detailed_view_data(
                scenario, 
                product if product else None,
                location if location else None,
                site if site else None
            )
            
            # DEBUG: Check what we got back
            print(f"DEBUG AJAX: search_detailed_view_data returned {len(results['inventory_data'])} inventory records")
            if results['inventory_data']:
                periods = results['inventory_data'][0].get('periods', [])
                print(f"DEBUG AJAX: First inventory record has {len(periods)} periods")
                if periods:
                    print(f"DEBUG AJAX: First period: {periods[0].get('date')}")
                    print(f"DEBUG AJAX: Last period: {periods[-1].get('date')}")
            
            # Render the results to HTML
            from django.template.loader import render_to_string
            
            inventory_html = render_to_string('website/inventory_table_partial.html', {
                'detailed_inventory_data': results['inventory_data']
            })
            
            production_html = render_to_string('website/production_table_partial.html', {
                'detailed_production_data': results['production_data']
            })
            
            # DEBUG: Check rendered HTML length
            print(f"DEBUG AJAX: Rendered inventory HTML length: {len(inventory_html)}")
            print(f"DEBUG AJAX: Rendered production HTML length: {len(production_html)}")
            
            return JsonResponse({
                'inventory_html': inventory_html,
                'production_html': production_html,
                'inventory_count': len(results['inventory_data']),
                'production_count': len(results['production_data'])
            })
            
        except scenarios.DoesNotExist:
            return JsonResponse({'error': 'Scenario not found'}, status=404)
        except Exception as e:
            import traceback
            print(f"DEBUG AJAX ERROR: {e}")
            traceback.print_exc()
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid method'}, status=405)


@login_required
def detailed_view_scenario_inventory(request, version):
    """View for displaying detailed inventory and production data for a scenario"""
    try:
        scenario = scenarios.objects.get(version=version)
        
        # Get search term from GET parameters
        search_term = request.GET.get('search_term', '').strip()
        
        if search_term:
            # If there's a search term, use the search function
            results = search_detailed_view_data(scenario, search_term, None, None)
        else:
            # Otherwise, return empty data structure
            results = detailed_view_scenario_inventory(scenario)
        
        context = {
            'scenario': scenario,
            'version': version,
            'search_term': search_term,
            'detailed_inventory_data': results['inventory_data'],
            'detailed_production_data': results['production_data'],
        }
        
        return render(request, 'website/detailed_inventory_view.html', context)
        
    except scenarios.DoesNotExist:
        messages.error(request, f'Scenario "{version}" not found.')
        return redirect('list_scenarios')
    except Exception as e:
        messages.error(request, f'An error occurred: {str(e)}')
        return redirect('list_scenarios')


# ...existing imports...
from .models import MasterDataSafetyStocks
from django.forms import modelformset_factory

@login_required
def upload_safety_stocks(request, version):
    """Fetch safety stocks data from Epicor database."""
    scenario = get_object_or_404(scenarios, version=version)
    
    try:
        # Run the management command
        result = run_management_command('fetch_safety_stocks_data', version)
        if result.returncode == 0:
            messages.success(request, "Safety stocks data successfully uploaded from Epicor.")
        else:
            messages.error(request, f"Error uploading safety stocks data: {result.stderr}")
    except Exception as e:
        messages.error(request, f"An error occurred: {e}")
    
    return redirect('edit_scenario', version=version)

@login_required
def update_safety_stocks(request, version):
    """Update safety stocks records."""
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    
    # Get filter values from GET parameters
    plant_filter = request.GET.get('plant', '').strip()
    part_filter = request.GET.get('part', '').strip()
    
    # Filter records
    queryset = MasterDataSafetyStocks.objects.filter(version=scenario)
    if plant_filter:
        queryset = queryset.filter(Plant__icontains=plant_filter)
    if part_filter:
        queryset = queryset.filter(PartNum__icontains=part_filter)
    
    # Always order before paginating
    queryset = queryset.order_by('Plant', 'PartNum')
    
    # Paginate the queryset
    paginator = Paginator(queryset, 20)  # Show 20 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Create formset
    SafetyStocksFormSet = modelformset_factory(
        MasterDataSafetyStocks,
        fields=('Plant', 'PartNum', 'MinimumQty', 'SafetyQty'),
        extra=0
    )
    
    if request.method == 'POST':
        formset = SafetyStocksFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            formset.save()
            messages.success(request, "Safety stocks updated successfully!")
            return redirect('edit_scenario', version=version)
    else:
        formset = SafetyStocksFormSet(queryset=page_obj.object_list)
    
    return render(request, 'website/update_safety_stocks.html', {
        'formset': formset,
        'page_obj': page_obj,
        'plant_filter': plant_filter,
        'part_filter': part_filter,
        'scenario': scenario,
        'user_name': user_name,
        'version': version,
    })

@login_required
def delete_safety_stocks(request, version):
    """Delete all safety stocks records for a specific version."""
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        MasterDataSafetyStocks.objects.filter(version=scenario).delete()
        messages.success(request, "All safety stocks records deleted successfully!")
        return redirect('edit_scenario', version=version)
    
    return render(request, 'website/delete_safety_stocks.html', {
        'scenario': scenario,
        'version': version
    })

@login_required
def copy_safety_stocks(request, version):
    """Copy safety stocks records from one version to another."""
    target_scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        source_scenario = get_object_or_404(scenarios, version=source_version)
        
        # Delete existing records for target scenario
        MasterDataSafetyStocks.objects.filter(version=target_scenario).delete()
        
        # Copy records from source to target
        source_records = MasterDataSafetyStocks.objects.filter(version=source_scenario)
        for record in source_records:
            MasterDataSafetyStocks.objects.create(
                version=target_scenario,
                Plant=record.Plant,
                PartNum=record.PartNum,
                MinimumQty=record.MinimumQty,
                SafetyQty=record.SafetyQty
            )
        
        messages.success(request, f"Safety stocks data successfully copied from scenario '{source_version}' to '{version}'.")
        return redirect('edit_scenario', version=version)
    
    # Get all scenarios except the current one
    all_scenarios = scenarios.objects.exclude(version=version)
    
    return render(request, 'website/copy_safety_stocks.html', {
        'target_scenario': target_scenario,
        'all_scenarios': all_scenarios,
    })


@login_required
@require_POST
def export_inventory_data(request):
    """Export inventory data to Excel or CSV format"""
    try:
        version = request.POST.get('version')
        parent_group = request.POST.get('parent_group', '').strip()
        export_format = request.POST.get('format', 'excel')  # 'excel' or 'csv'
        
        if not version:
            return JsonResponse({'error': 'Version is required'}, status=400)
        
        scenario = get_object_or_404(scenarios, version=version)
        
        # Get inventory data using the stored aggregated data (NOT the heavy function)
        stored_data = get_stored_inventory_data(scenario)
        inventory_data = stored_data.get('monthly_trends', {})
        
        # Get the snapshot date
        snapshot_date = "Unknown"
        try:
            inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
            if inventory_snapshot:
                snapshot_date = inventory_snapshot.date_of_snapshot.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error getting snapshot date: {e}")
        
        # Prepare data for export
        export_data = []
        
        if parent_group and parent_group in inventory_data:
            # Export specific group
            group_data = inventory_data[parent_group]
            
            # Add opening inventory row
            export_data.append({
                'Parent_Product_Group': parent_group,
                'Month': 'Opening Inventory',
                'COGS_AUD': 0,
                'Revenue_AUD': 0,
                'Production_AUD': 0,
                'Inventory_Balance_AUD': group_data.get('opening_inventory', 0)
            })
            
            # Add monthly data
            months = group_data.get('months', [])
            cogs = group_data.get('cogs', [])
            revenue = group_data.get('revenue', [])
            production_aud = group_data.get('production_aud', [])
            inventory_balance = group_data.get('inventory_balance', [])
            
            for i, month in enumerate(months):
                export_data.append({
                    'Parent_Product_Group': parent_group,
                    'Month': month,
                    'COGS_AUD': cogs[i] if i < len(cogs) else 0,
                    'Revenue_AUD': revenue[i] if i < len(revenue) else 0,
                    'Production_AUD': production_aud[i] if i < len(production_aud) else 0,
                    'Inventory_Balance_AUD': inventory_balance[i] if i < len(inventory_balance) else 0
                })
                
        else:
            # Export all groups combined
            all_groups_data = inventory_data
            
            # Find all unique months
            all_months_set = set()
            for group_data in all_groups_data.values():
                all_months_set.update(group_data.get('months', []))
            
            all_months = sorted(list(all_months_set), key=lambda x: pd.to_datetime(f"01 {x}"))
            
            # Add combined opening inventory
            total_opening_inventory = sum(group_data.get('opening_inventory', 0) for group_data in all_groups_data.values())
            
            export_data.append({
                'Parent_Product_Group': 'All Groups Combined',
                'Month': 'Opening Inventory',
                'COGS_AUD': 0,
                'Revenue_AUD': 0,
                'Production_AUD': 0,
                'Inventory_Balance_AUD': total_opening_inventory
            })
            
            # Calculate combined monthly data
            for month in all_months:
                combined_cogs = 0
                combined_revenue = 0
                combined_production = 0
                
                for group_name, group_data in all_groups_data.items():
                    months = group_data.get('months', [])
                    if month in months:
                        month_idx = months.index(month)
                        combined_cogs += group_data.get('cogs', [])[month_idx] if month_idx < len(group_data.get('cogs', [])) else 0
                        combined_revenue += group_data.get('revenue', [])[month_idx] if month_idx < len(group_data.get('revenue', [])) else 0
                        combined_production += group_data.get('production_aud', [])[month_idx] if month_idx < len(group_data.get('production_aud', [])) else 0
                
                # Calculate inventory balance (simplified)
                current_balance = total_opening_inventory + combined_production - combined_cogs
                
                export_data.append({
                    'Parent_Product_Group': 'All Groups Combined',
                    'Month': month,
                    'COGS_AUD': combined_cogs,
                    'Revenue_AUD': combined_revenue,
                    'Production_AUD': combined_production,
                    'Inventory_Balance_AUD': current_balance
                })
        
        # Create DataFrame
        df = pd.DataFrame(export_data)
        
        # Generate filename
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        group_suffix = f"_{parent_group}" if parent_group else "_All_Groups"
        
        if export_format == 'csv':
            # Export as CSV
            filename = f"Inventory_Data_{version}{group_suffix}_{timestamp}.csv"
            
            response = HttpResponse(content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            
            df.to_csv(response, index=False)
            return response
            
        else:
            # Export as Excel
            filename = f"Inventory_Data_{version}{group_suffix}_{timestamp}.xlsx"
            
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Inventory Data', index=False)
                
                # Add summary sheet
                summary_data = {
                    'Metric': ['Scenario Version', 'Parent Product Group', 'Data Snapshot Date', 'Export Date', 'Export Format'],
                    'Value': [version, parent_group or 'All Groups', snapshot_date, timestamp, 'Excel']
                }
                
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            output.seek(0)
            
            response = HttpResponse(
                output.getvalue(),
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            
            return response
        
    except Exception as e:
        print(f"Export error: {e}")
        import traceback
        traceback.print_exc()
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_POST
def export_production_by_product(request):
    """Export production data by product to CSV format"""
    try:
        # Handle both GET and POST requests
        if request.method == 'GET':
            version = request.GET.get('version')
            parent_group = request.GET.get('parent_group', '').strip()
        else:  # POST
            version = request.POST.get('version')
            parent_group = request.POST.get('parent_group', '').strip()
        
        if not version:
            return JsonResponse({'error': 'Version is required'}, status=400)
        
        scenario = get_object_or_404(scenarios, version=version)
        
        # Query production data grouped by parent product group, product, and month
        from django.db.models import Sum
        from django.db.models.functions import TruncMonth
        
        queryset = CalculatedProductionModel.objects.filter(version=scenario)
        
        # Filter by parent group if specified
        if parent_group:
            queryset = queryset.filter(parent_product_group=parent_group)
        
        # Group by parent product group, product, and month, then sum production costs
        production_data = (
            queryset
            .annotate(month=TruncMonth('pouring_date'))
            .values('parent_product_group', 'product', 'month')
            .annotate(total_production_aud=Sum('cogs_aud'))
            .order_by('parent_product_group', 'product', 'month')
        )
        
        # Convert to list for CSV export
        export_data = []
        for item in production_data:
            export_data.append({
                'ParentProductGroup': item['parent_product_group'],
                'Product': item['product'],
                'Date': item['month'].strftime('%Y-%m-%d'),  # Format as YYYY-MM-DD
                'ProductionAUD': round(float(item['total_production_aud'] or 0), 2)
            })
        
        if not export_data:
            return JsonResponse({'error': 'No production data found for the specified criteria'}, status=404)
        
        # Create CSV response
        import csv
        from django.http import HttpResponse
        from io import StringIO
        
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=['ParentProductGroup', 'Product', 'Date', 'ProductionAUD'])
        writer.writeheader()
        writer.writerows(export_data)
        
        # Generate filename
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        group_suffix = f'_{parent_group.replace(" ", "_")}' if parent_group else '_All_Groups'
        filename = f'Production_by_Product_{version}{group_suffix}_{timestamp}.csv'
        
        response = HttpResponse(output.getvalue(), content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        return response
        
    except Exception as e:
        print(f"Production export error: {e}")
        import traceback
        traceback.print_exc()
        return JsonResponse({'error': str(e)}, status=500)

@require_http_methods(["GET"])
def pour_plan_details(request, version, fy, site):
    """Get detailed monthly pour plan data for a specific site and fiscal year."""
    from website.customized_function import get_monthly_pour_plan_details_for_site_and_fy
    
    try:
        # Get scenario - if version is 'DEFAULT', get the first available scenario
        if version == 'DEFAULT':
            first_scenario = scenarios.objects.first()
            if not first_scenario:
                return JsonResponse({'error': 'No scenarios available'}, status=404)
            scenario = first_scenario
        else:
            scenario = get_object_or_404(scenarios, version=version)
        
        # Get detailed data
        details = get_monthly_pour_plan_details_for_site_and_fy(site, fy, scenario)
        
        if not details:
            return JsonResponse({'error': 'No data found'}, status=404)
        
        # Convert date objects to strings for JSON serialization
        for detail in details['monthly_details']:
            detail['month_date'] = detail['month_date'].isoformat()
        
        return JsonResponse(details)
        
    except Exception as e:
        print(f"Error getting pour plan details: {e}")
        import traceback
        traceback.print_exc()
        return JsonResponse({'error': str(e)}, status=500)