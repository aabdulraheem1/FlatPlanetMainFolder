from django.shortcuts import render, redirect
import pandas as pd
from django.core.files.storage import FileSystemStorage
from .models import SMART_Forecast_Model, scenarios, MasterDataHistoryOfProductionModel, MasterDataCastToDespatchModel, MasterdataIncoTermsModel, MasterDataIncotTermTypesModel, Revenue_Forecast_Model
import pandas as pd
from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect
from .forms import UploadFileForm, ScenarioForm, SMARTForecastForm
from .models import SMART_Forecast_Model, scenarios, MasterDataOrderBook, MasterDataCapacityModel, MasterDataCommentModel, MasterDataHistoryOfProductionModel, MasterDataIncotTermTypesModel, MasterdataIncoTermsModel, MasterDataPlan,MasterDataProductAttributesModel, MasterDataSalesAllocationToPlantModel, MasterDataSalesModel, MasterDataSKUTransferModel, MasterDataScheduleModel, AggregatedForecast, MasterDataForecastRegionModel, MasterDataCastToDespatchModel, CalcualtedReplenishmentModel, CalculatedProductionModel, MasterDataFreightModel    
from django.contrib.auth.decorators import login_required
import pyodbc
from django.shortcuts import render
from .models import MasterDataProductModel
from sqlalchemy import create_engine, text
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404, redirect
from .models import MasterDataProductModel, MasterDataProductPictures, MasterDataPlantModel, AggregatedForecast
from django.urls import reverse
from .forms import ProductForm, ProductPictureForm, MasterDataPlantsForm
import requests
from django.core.files.storage import FileSystemStorage


from django.shortcuts import render, get_object_or_404, redirect

import sys
import subprocess
from django.conf import settings

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


    # Check if there is data for Master Data Incoterm Types
    master_data_incoterm_types_has_data = MasterDataIncotTermTypesModel.objects.filter(version=scenario).exists()
    master_data_inco_terms_has_data = MasterdataIncoTermsModel.objects.filter(version=scenario).exists()
    master_data_casto_to_despatch_days_has_data = MasterDataCastToDespatchModel.objects.filter(version=scenario).exists()
    incoterms = MasterDataIncotTermTypesModel.objects.filter(version=scenario)  # Retrieve all incoterms for the scenario

    # Check if there is data for MasterDataPlan
    pour_plan_data_has_data = MasterDataPlan.objects.filter( version=scenario).exists()

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

        df = pd.read_excel(file_path)
        print("Excel DataFrame head:", df.head())
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

        for _, row in df.iterrows():
            SMART_Forecast_Model.objects.create(
                version=version,
                Data_Source=data_source,
                Forecast_Region=row.get('Forecast_Region') if pd.notna(row.get('Forecast_Region')) else None,
                Product_Group=row.get('Product_Group') if pd.notna(row.get('Product_Group')) else None,
                Product=row.get('Product') if pd.notna(row.get('Product')) else None,
                ProductFamilyDescription=row.get('ProductFamilyDescription') if pd.notna(row.get('ProductFamilyDescription')) else None,
                Customer_code=row.get('Customer_code') if pd.notna(row.get('Customer_code')) else None,
                Location=row.get('Location') if pd.notna(row.get('Location')) else None,
                Forecasted_Weight_Curr=row.get('Forecasted_Weight_Curr') if pd.notna(row.get('Forecasted_Weight_Curr')) else None,
                PriceAUD=row.get('PriceAUD') if pd.notna(row.get('PriceAUD')) else None,
                DP_Cycle=row.get('DP_Cycle') if pd.notna(row.get('DP_Cycle')) else None,
                Period_AU=row.get('Period_AU') if pd.notna(row.get('Period_AU')) else None,
                Qty=row.get('Qty') if pd.notna(row.get('Qty')) else None
            )

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
from django.db.models import Sum
from .models import AggregatedForecast, CalculatedProductionModel, scenarios

from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.db.models.functions import TruncMonth
from django.db.models import Sum
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
            * record.Yield
            * (1 - (record.WasterPercentage or 0) / 100)
        ) if record.AvailableDays and record.heatsperdays and record.TonsPerHeat and record.Yield else 0
        dress_mass_data.append({'month': record.Month, 'dress_mass': dress_mass})
    
    return dress_mass_data

import json
from collections import defaultdict
from django.db.models.functions import TruncMonth
from django.db.models import Sum
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
            * record.Yield
            * (1 - (record.WasterPercentage or 0) / 100)
        ) if record.AvailableDays and record.heatsperdays and record.TonsPerHeat and record.Yield else 0
        dress_mass_data.append({'month': record.Month, 'dress_mass': dress_mass})
    return dress_mass_data

from collections import defaultdict
from django.db.models.functions import TruncMonth
from django.db.models import Sum
import json

from collections import defaultdict
from django.db.models.functions import TruncMonth
from django.db.models import Sum
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
            * record.Yield
            * (1 - (record.WasterPercentage or 0) / 100)
        ) if record.AvailableDays and record.heatsperdays and record.TonsPerHeat and record.Yield else 0
        dress_mass_data.append({'month': record.Month, 'dress_mass': dress_mass})
    return dress_mass_data

from datetime import date
from django.db.models import Sum
from django.utils.safestring import mark_safe
import json
@login_required
def review_scenario(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)

    def get_production_data_by_group(site_name, scenario_version):
        queryset = (
            CalculatedProductionModel.objects
            .filter(site__SiteName=site_name, version=scenario_version)
            .annotate(month=TruncMonth('pouring_date'))
            .values('month', 'product__ProductGroup')
            .annotate(total_tonnes=Sum('tonnes'))
            .order_by('month', 'product__ProductGroup')
        )
        # Build data structure: {group: {month: total}}, labels: [months]
        data = {}
        labels_set = set()
        for entry in queryset:
            month = entry['month'].strftime('%Y-%m')
            group = entry['product__ProductGroup'] or 'Unknown'
            labels_set.add(month)
            data.setdefault(group, {})[month] = entry['total_tonnes']
        labels = sorted(labels_set)
        # Filter out product groups with all zero totals
        filtered_data = {}
        for group, month_dict in data.items():
            values = [month_dict.get(label, 0) for label in labels]
            if any(v != 0 for v in values):
                filtered_data[group] = month_dict
        # Convert to chart.js format
        datasets = []
        colors = [
            'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
            'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)'
        ]
        for idx, (group, month_dict) in enumerate(filtered_data.items()):
            datasets.append({
                'label': group,
                'data': [month_dict.get(label, 0) for label in labels],
                'backgroundColor': colors[idx % len(colors)],
                'borderColor': colors[idx % len(colors)],
                'borderWidth': 1,
                'stack': 'tonnes'
            })
        return {'labels': labels, 'datasets': datasets}

    def get_top_products_per_month_by_group(site_name):
        queryset = (
            CalculatedProductionModel.objects
            .filter(site__SiteName=site_name, version=scenario.version)
            .annotate(month=TruncMonth('pouring_date'))
            .values('month', 'product__ProductGroup', 'product__Product')
            .annotate(total_tonnes=Sum('tonnes'))
            .order_by('month', 'product__ProductGroup', '-total_tonnes')
        )
        # Structure: {month: {group: [(product, tonnes), ...]}}
        month_group_products = defaultdict(lambda: defaultdict(list))
        for entry in queryset:
            month = entry['month'].strftime('%Y-%m')
            group = entry['product__ProductGroup'] or 'Unknown'
            product = entry['product__Product']
            tonnes = entry['total_tonnes']
            month_group_products[month][group].append((product, tonnes))
        # Keep only top 10 per group
        for month in month_group_products:
            for group in month_group_products[month]:
                month_group_products[month][group] = sorted(
                    month_group_products[month][group], key=lambda x: x[1], reverse=True
                )[:10]
        return month_group_products

    # Data for charts
    mt_joli_data = get_production_data_by_group('MTJ1', scenario.version)
    mt_joli_dress_mass_data = get_dress_mass_data('MTJ1', scenario.version)
    coimbatore_data = get_production_data_by_group('COI2', scenario.version)
    coimbatore_dress_mass_data = get_dress_mass_data('COI2', scenario.version)
    xuzhou_data = get_production_data_by_group('XUZ1', scenario.version)
    xuzhou_dress_mass_data = get_dress_mass_data('XUZ1', scenario.version)
    merlimau_data = get_production_data_by_group('MER1', scenario.version)
    merlimau_dress_mass_data = get_dress_mass_data('MER1', scenario.version)
    # --- WOD1 ---
    wod1_data = get_production_data_by_group('WOD1', scenario.version)
    wod1_chart_data = wod1_data
    wod1_top_products = get_top_products_per_month_by_group('WOD1')
    wod1_top_products_json = json.dumps(wod1_top_products)

# --- WUN1 ---
    wun1_data = get_production_data_by_group('WUN1', scenario.version)
    wun1_chart_data = wun1_data
    wun1_top_products = get_top_products_per_month_by_group('WUN1')
    wun1_top_products_json = json.dumps(wun1_top_products)

    mt_joli_chart_data = mt_joli_data
    coimbatore_chart_data = coimbatore_data
    xuzhou_chart_data = xuzhou_data
    merlimau_chart_data = merlimau_data

    # Top 10 products per month by group for each foundry
    mt_joli_top_products = get_top_products_per_month_by_group('MTJ1')
    mt_joli_top_products_json = json.dumps(mt_joli_top_products)
    coimbatore_top_products = get_top_products_per_month_by_group('COI2')
    coimbatore_top_products_json = json.dumps(coimbatore_top_products)
    xuzhou_top_products = get_top_products_per_month_by_group('XUZ1')
    xuzhou_top_products_json = json.dumps(xuzhou_top_products)
    merlimau_top_products = get_top_products_per_month_by_group('MER1')
    merlimau_top_products_json = json.dumps(merlimau_top_products)

    def get_supplier_data_by_group(site_name, scenario_version):
        queryset = (
            CalculatedProductionModel.objects
            .filter(site__SiteName=site_name, version=scenario_version)
            .annotate(month=TruncMonth('pouring_date'))
            .values('month', 'product__ProductGroup')
            .annotate(total_tonnes=Sum('tonnes'))
            .order_by('month', 'product__ProductGroup')
        )
        data = {}
        labels_set = set()
        for entry in queryset:
            month = entry['month'].strftime('%Y-%m')
            group = entry['product__ProductGroup'] or 'Unknown'
            labels_set.add(month)
            data.setdefault(group, {})[month] = entry['total_tonnes']
        labels = sorted(labels_set)
        # Filter out product groups with all zero totals
        filtered_data = {}
        for group, month_dict in data.items():
            values = [month_dict.get(label, 0) for label in labels]
            if any(v != 0 for v in values):
                filtered_data[group] = month_dict
        # Convert to chart.js format
        datasets = []
        colors = [
            'rgba(75,192,192,0.6)', 'rgba(255,99,132,0.6)', 'rgba(255,206,86,0.6)',
            'rgba(54,162,235,0.6)', 'rgba(153,102,255,0.6)', 'rgba(255,159,64,0.6)'
        ]
        for idx, (group, month_dict) in enumerate(filtered_data.items()):
            datasets.append({
                'label': group,
                'data': [month_dict.get(label, 0) for label in labels],
                'backgroundColor': colors[idx % len(colors)],
                'borderColor': colors[idx % len(colors)],
                'borderWidth': 1,
                'stack': 'tonnes'
            })
        return {'labels': labels, 'datasets': datasets}

    def get_supplier_top_products_by_group(site_name, scenario_version):
        queryset = (
            CalculatedProductionModel.objects
            .filter(site__SiteName=site_name, version=scenario_version)
            .annotate(month=TruncMonth('pouring_date'))
            .values('month', 'product__ProductGroup', 'product__Product')
            .annotate(total_tonnes=Sum('tonnes'))
            .order_by('month', 'product__ProductGroup', '-total_tonnes')
        )
        month_group_products = defaultdict(lambda: defaultdict(list))
        for entry in queryset:
            month = entry['month'].strftime('%Y-%m')
            group = entry['product__ProductGroup'] or 'Unknown'
            product = entry['product__Product']
            tonnes = entry['total_tonnes']
            month_group_products[month][group].append((product, tonnes))
        # Keep only top 10 per group
        for month in month_group_products:
            for group in month_group_products[month]:
                month_group_products[month][group] = sorted(
                    month_group_products[month][group], key=lambda x: x[1], reverse=True
                )[:10]
        return month_group_products

    # In your view:
    supplier_a_chart_data = get_supplier_data_by_group('HBZJBF02', scenario.version)
    supplier_a_top_products = get_supplier_top_products_by_group('HBZJBF02', scenario.version)
    supplier_a_top_products_json = json.dumps(supplier_a_top_products)

    # Add this inside your review_scenario view, before the context dict
    sites = ["MTJ1", "COI2", "XUZ1", "MER1", "WUN1", "WOD1", "CHI1",]
    fy_ranges = {
        "FY25": (date(2025, 4, 1), date(2026, 3, 31)),
        "FY26": (date(2026, 4, 1), date(2027, 3, 31)),
        "FY27": (date(2027, 4, 1), date(2028, 3, 31)),
    }

    demand_plan = {}
    for fy, (start, end) in fy_ranges.items():
        demand_plan[fy] = {}
        for site in sites:
            total = CalculatedProductionModel.objects.filter(
                version=scenario,
                site=site,  # or site__SiteName=site if ForeignKey
                pouring_date__gte=start,
                pouring_date__lte=end
            ).aggregate(total=Sum('tonnes'))['total'] or 0
            demand_plan[fy][site] = round(total)

    from django.utils.safestring import mark_safe

    def build_monthly_table(rows):
        table = "<table class='table table-sm table-bordered mb-0'><tr><th>Month</th><th>Tonnes</th></tr>"
        for row in rows:
            table += f"<tr><td>{row['month'].strftime('%b %Y')}</td><td>{row['total']:,}</td></tr>"
        table += "</table>"
        # Remove newlines and escape double quotes
        table = table.replace('\n', '').replace('\r', '').replace('"', '&quot;')
        return mark_safe(table)

    monthly_plan = defaultdict(lambda: defaultdict(list))
    for fy, (start, end) in fy_ranges.items():
        for site in sites:
            qs = (
                CalculatedProductionModel.objects
                .filter(
                    version=scenario,
                    site=site,
                    pouring_date__gte=start,
                    pouring_date__lte=end
                )
                .annotate(month=TruncMonth('pouring_date'))
                .values('month')
                .annotate(total=Sum('tonnes'))
                .order_by('month')
            )
            monthly_plan[fy][site] = list(qs)

    # ...existing code...

    from django.db.models import Q

    pour_plan = {}
    for fy, (start, end) in fy_ranges.items():
        pour_plan[fy] = {}
        for site in sites:
            # Get all plans for this site and FY
            plans = MasterDataPlan.objects.filter(
                version=scenario,
                Foundry__SiteName=site,
                Month__gte=start,
                Month__lte=end
            )
            # Sum the PlanDressMass property for each plan
            total = sum(plan.PlanDressMass for plan in plans)
            pour_plan[fy][site] = round(total)

 

    monthly_table_html = {}
    for fy, (start, end) in fy_ranges.items():
        monthly_table_html[fy] = {}
        for site in sites:
            qs = (
                CalculatedProductionModel.objects
                .filter(
                    version=scenario,
                    site=site,
                    pouring_date__gte=start,
                    pouring_date__lte=end
                )
                .annotate(month=TruncMonth('pouring_date'))
                .values('month')
                .annotate(total=Sum('tonnes'))
                .order_by('month')
            )
            monthly_table_html[fy][site] = build_monthly_table(qs)

    from datetime import datetime
    from django.db.models import Q

    from datetime import datetime

    # --- Mt Joli ---
    mt_joli_months = mt_joli_chart_data['labels']
    mt_joli_monthly_pour_plan = []
    for month in mt_joli_months:
        month_date = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year + 1, month=1, day=1)
        else:
            next_month = month_date.replace(month=month_date.month + 1, day=1)
        plans = MasterDataPlan.objects.filter(
            version=scenario,
            Foundry__SiteName='MTJ1',
            Month__gte=month_date,
            Month__lt=next_month
        )
        value = sum(plan.PlanDressMass for plan in plans)
        mt_joli_monthly_pour_plan.append(round(value))

    # --- Coimbatore ---
    coimbatore_months = coimbatore_chart_data['labels']
    coimbatore_monthly_pour_plan = []
    for month in coimbatore_months:
        month_date = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year + 1, month=1, day=1)
        else:
            next_month = month_date.replace(month=month_date.month + 1, day=1)
        plans = MasterDataPlan.objects.filter(
            version=scenario,
            Foundry__SiteName='COI2',
            Month__gte=month_date,
            Month__lt=next_month
        )
        value = sum(plan.PlanDressMass for plan in plans)
        coimbatore_monthly_pour_plan.append(round(value))

    # --- Xuzhou ---
    xuzhou_months = xuzhou_chart_data['labels']
    xuzhou_monthly_pour_plan = []
    for month in xuzhou_months:
        month_date = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year + 1, month=1, day=1)
        else:
            next_month = month_date.replace(month=month_date.month + 1, day=1)
        plans = MasterDataPlan.objects.filter(
            version=scenario,
            Foundry__SiteName='XUZ1',
            Month__gte=month_date,
            Month__lt=next_month
        )
        value = sum(plan.PlanDressMass for plan in plans)
        xuzhou_monthly_pour_plan.append(round(value))

    # --- Merlimau ---
    merlimau_months = merlimau_chart_data['labels']
    merlimau_monthly_pour_plan = []
    for month in merlimau_months:
        month_date = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year + 1, month=1, day=1)
        else:
            next_month = month_date.replace(month=month_date.month + 1, day=1)
        plans = MasterDataPlan.objects.filter(
            version=scenario,
            Foundry__SiteName='MER1',
            Month__gte=month_date,
            Month__lt=next_month
        )
        value = sum(plan.PlanDressMass for plan in plans)
        merlimau_monthly_pour_plan.append(round(value))

    
    wod1_months = wod1_chart_data['labels']
    wod1_monthly_pour_plan = []
    for month in wod1_months:
        month_date = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year + 1, month=1, day=1)
        else:
            next_month = month_date.replace(month=month_date.month + 1, day=1)
        plans = MasterDataPlan.objects.filter(
            version=scenario,
            Foundry__SiteName='WOD1',
            Month__gte=month_date,
            Month__lt=next_month
        )
        value = sum(plan.PlanDressMass for plan in plans)
        wod1_monthly_pour_plan.append(round(value))

    # --- WUN1 ---
    wun1_months = wun1_chart_data['labels']
    wun1_monthly_pour_plan = []
    for month in wun1_months:
        month_date = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year + 1, month=1, day=1)
        else:
            next_month = month_date.replace(month=month_date.month + 1, day=1)
        plans = MasterDataPlan.objects.filter(
            version=scenario,
            Foundry__SiteName='WUN1',
            Month__gte=month_date,
            Month__lt=next_month
        )
        value = sum(plan.PlanDressMass for plan in plans)
        wun1_monthly_pour_plan.append(round(value))
    

    context = {
        'version': scenario.version,
        'user_name': user_name,
        'mt_joli_chart_data': mt_joli_chart_data,
        'mt_joli_top_products_json': mt_joli_top_products_json,
        'coimbatore_chart_data': coimbatore_chart_data,
        'coimbatore_top_products_json': coimbatore_top_products_json,
        'xuzhou_chart_data': xuzhou_chart_data,
        'xuzhou_top_products_json': xuzhou_top_products_json,
        'merlimau_chart_data': merlimau_chart_data,
        'merlimau_top_products_json': merlimau_top_products_json,
        'supplier_a_chart_data': supplier_a_chart_data,
        'supplier_a_top_products_json': supplier_a_top_products_json,
        'demand_plan': demand_plan,
        'monthly_plan': monthly_plan,
        'monthly_table_html': mark_safe(json.dumps(monthly_table_html)),
        'pour_plan': pour_plan,
        'mt_joli_monthly_pour_plan': mt_joli_monthly_pour_plan,
        'mt_joli_monthly_pour_plan': mt_joli_monthly_pour_plan,
        'coimbatore_monthly_pour_plan': coimbatore_monthly_pour_plan,
        'xuzhou_monthly_pour_plan': xuzhou_monthly_pour_plan,
        'merlimau_monthly_pour_plan': merlimau_monthly_pour_plan,
        'wod1_chart_data': wod1_chart_data,
        'wod1_top_products_json': wod1_top_products_json,
        'wod1_monthly_pour_plan': wod1_monthly_pour_plan,
        'wun1_chart_data': wun1_chart_data,
        'wun1_top_products_json': wun1_top_products_json,
        'wun1_monthly_pour_plan': wun1_monthly_pour_plan,
    }
    print("monthly_table_html:", monthly_table_html)
    return render(request, 'website/review_scenario.html', context)

from django.db.models import Sum
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
        # SQL query to fetch data
        query = text("""
            SELECT DISTINCT
                Site.SiteName AS site,
                Product.ProductKey AS productkey
            FROM PowerBI.SalesOrders AS SalesOrders
            INNER JOIN PowerBI.Products AS Product ON SalesOrders.skProductId = Product.skProductId
            INNER JOIN PowerBI.Site AS Site ON SalesOrders.skSiteId = Site.skSiteId
            WHERE Site.SiteName IN ('MTJ1', 'COI2', 'XUZ1', 'MER1', 'WOD1', 'WUN1')
            AND (SalesOrders.OnOrderQty IS NOT NULL AND SalesOrders.OnOrderQty > 0)
        """)

        # Execute the query
        result = connection.execute(query)

        # Populate the MasterDataOrderBook model
        for row in result:
            MasterDataOrderBook.objects.update_or_create(
                version=scenario,
                site=row.site,
                productkey=row.productkey
            )

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

    # Create a formset for the current page
    OnHandStockFormSet = modelformset_factory(
        MasterDataInventory,
        fields=('site', 'product', 'onhandstock_qty', 'intransitstock_qty', 'wip_stock_qty'),
        extra=0
    )
    formset = OnHandStockFormSet(queryset=page_obj.object_list)

    if request.method == 'POST':
        formset = OnHandStockFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            formset.save()
            return redirect('edit_scenario', version=version)

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
    smart_forecast_products = SMART_Forecast_Model.objects.filter(version=scenario).values_list('Product', flat=True)

    if request.method == 'POST':
        # Get the snapshot date entered by the user
        snapshot_date = request.POST.get('snapshot_date')

        if not snapshot_date:
            return render(request, 'website/update_on_hand_stock.html', {
                'error': 'Please enter a valid snapshot date.',
                'scenario': scenario
            })

        # Delete existing data for the given version
        MasterDataInventory.objects.filter(version=scenario).delete()

        with engine.connect() as connection:
            # Query to join tables and fetch inventory data
            query = text("""
                SELECT 
                    Products.ProductKey AS product,
                    Site.SiteName AS site,
                    Inventory.StockOnHand AS onhandstock_qty,
                    Inventory.StockInTransit AS intransitstock_qty
                FROM PowerBI.[Inventory Daily History] AS Inventory
                INNER JOIN PowerBI.Site AS Site
                    ON Inventory.skSiteId = Site.skSiteId
                INNER JOIN PowerBI.Dates AS Dates
                    ON Inventory.skReportDateId = Dates.skDateId
                INNER JOIN PowerBI.Products AS Products
                    ON Inventory.skProductId = Products.skProductId
                WHERE Dates.DateValue = :snapshot_date
            """)

            # Execute the query
            inventory_data = connection.execute(query, {'snapshot_date': snapshot_date}).fetchall()

            # Query to join tables and fetch WIP data
            wip_query = text("""
                SELECT 
                    Products.ProductKey AS product,
                    Site.SiteName AS site,
                    SUM(WIP.WIPQty) AS wip_stock_qty
                FROM PowerBI.[Work In Progress] AS WIP
                INNER JOIN PowerBI.Site AS Site
                    ON WIP.skSiteId = Site.skSiteId
                INNER JOIN PowerBI.Dates AS Dates
                    ON WIP.skReportDateId = Dates.skDateId
                INNER JOIN PowerBI.Products AS Products
                    ON WIP.skProductId = Products.skProductId
                WHERE Dates.DateValue = :snapshot_date
                GROUP BY Products.ProductKey, Site.SiteName
            """)

            # Execute the WIP query
            wip_data = connection.execute(wip_query, {'snapshot_date': snapshot_date}).fetchall()

            # Create a dictionary for WIP data for quick lookup
            wip_dict = {(row.product, row.site): row.wip_stock_qty for row in wip_data}

            # Store the data in the MasterDataInventory model
            for row in inventory_data:
                # Process only if the product is in SMART_Forecast_Model for the current scenario
                if row.product not in smart_forecast_products:
                    continue

                wip_stock_qty = wip_dict.get((row.product, row.site), 0)  # Default to 0 if no WIP data

                # Fetch the MasterDataPlantModel instance for the site
                try:
                    plant = MasterDataPlantModel.objects.get(SiteName=row.site)
                except MasterDataPlantModel.DoesNotExist:
                    # Skip this record if the site does not exist in MasterDataPlantModel
                    continue

                MasterDataInventory.objects.create(
                    version=scenario,
                    date_of_snapshot=snapshot_date,
                    product=row.product,
                    site=plant,  # Use the MasterDataPlantModel instance
                    site_region=plant.PlantRegion,  # Populated from MasterDataPlantModel
                    onhandstock_qty=row.onhandstock_qty,
                    intransitstock_qty=row.intransitstock_qty,
                    wip_stock_qty=wip_stock_qty,
                )

        # Redirect to the scenario edit page after successful update
        return redirect('edit_scenario', version=version)

    # Render the form to enter the snapshot date
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

def incoterm_upload(request):
    if request.method == 'POST' and request.FILES['file']:
        excel_file = request.FILES['file']
        try:
            df = pd.read_excel(excel_file)
            required_columns = [' version', 'IncoTerm', 'IncoTermCaregory']
            if not all(col in df.columns for col in required_columns):
                return HttpResponse("Invalid file format. Required columns:  version, IncoTerm, IncoTermCaregory.")
            for _, row in df.iterrows():
                MasterDataIncotTermTypesModel.objects.update_or_create(
                    version_id=row[' version'],
                    IncoTerm=row['IncoTerm'],
                    defaults={'IncoTermCaregory': row['IncoTermCaregory']}
                )
            return redirect('incoterm_list')
        except Exception as e:
            return HttpResponse(f"Error processing file: {e}")
    return render(request, 'website/incoterm_upload.html')

from django.shortcuts import render, redirect, get_object_or_404
from django.forms import modelformset_factory
from .models import MasterdataIncoTermsModel, scenarios
from .forms import MasterdataIncoTermsForm

def master_data_inco_terms_update_formset(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    IncoTermsFormSet = modelformset_factory(MasterdataIncoTermsModel, form=MasterdataIncoTermsForm, extra=0)

    if request.method == 'POST':
        formset = IncoTermsFormSet(request.POST, queryset=MasterdataIncoTermsModel.objects.filter(version=scenario))
        if formset.is_valid():
            formset.save()
            return redirect('edit_scenario', version=version)
    else:
        formset = IncoTermsFormSet(queryset=MasterdataIncoTermsModel.objects.filter(version=scenario))

    return render(request, 'website/master_data_inco_terms_formset.html', {'formset': formset, 'scenario': scenario,
                                                                           'user_name': user_name, 'version': version})

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
                    incoterm_obj = MasterDataIncotTermTypesModel.objects.get(IncoTerm=incoterm_value)
                except MasterDataIncotTermTypesModel.DoesNotExist:
                    messages.error(request, f"Incoterm '{incoterm_value}' does not exist in Incoterm Types.")
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
                    MasterdataIncoTermsModel.objects.create(
                        version=target_scenario,
                        CustomerCode=record.CustomerCode,
                        Incoterm=record.Incoterm
                    )
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
    if request.method == 'POST' and request.FILES['file']:
        csv_file = request.FILES['file']
        decoded_file = csv_file.read().decode('utf-8').splitlines()
        reader = csv.DictReader(decoded_file)

        for row in reader:
            MasterDataPlan.objects.update_or_create(
                 version_id=version,
                Foundry=row['Foundry'],
                defaults={
                    'Sub version': row.get('Sub version'),
                    'PouringDaysperweek': row.get('PouringDaysperweek'),
                    'CalendarDays': row.get('CalendarDays'),
                    'Month': row.get('Month'),
                    'Yield': row.get('Yield'),
                    'WasterPercentage': row.get('WasterPercentage'),
                    'PlanDressMass': row.get('PlanDressMass'),
                    'UnavailableDays': row.get('UnavailableDays'),
                    'AvailableDays': row.get('AvailableDays'),
                    'PlannedMaintenanceDays': row.get('PlannedMaintenanceDays'),
                    'PublicHolidays': row.get('PublicHolidays'),
                    'Weekends': row.get('Weekends'),
                    'OtherNonPouringDays': row.get('OtherNonPouringDays'),
                    'HeatsPerweek': row.get('HeatsPerweek'),
                    'heatsperdays': row.get('heatsperdays'),
                    'CastMass': row.get('CastMass'),
                    'TonsPerHeat': row.get('TonsPerHeat'),
                    'CastTonsPerDay': row.get('CastTonsPerDay'),
                }
            )
        messages.success(request, "Master Data Plan uploaded successfully!")
        return redirect('edit_scenario', version=version)

    return render(request, 'website/upload_master_data_plan.html', {'version': version})

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
        for form in formset.extra_forms:
            form.empty_permitted = True

        if formset.is_valid():
            instances = formset.save(commit=False)
            duplicates = []
            for instance in instances:
                if not instance.Foundry or not instance.Month:
                    continue
                instance.version = scenario
                # Check for duplicates before saving
                exists = MasterDataPlan.objects.filter(
                    Foundry=instance.Foundry,
                    Month=instance.Month,
                    version=scenario
                ).exclude(pk=instance.pk).exists()
                if exists:
                    duplicates.append(f"{instance.Foundry} - {instance.Month}")
                else:
                    try:
                        instance.save()
                    except IntegrityError:
                        duplicates.append(f"{instance.Foundry} - {instance.Month}")

            for instance in formset.deleted_objects:
                instance.delete()

            if duplicates:
                messages.error(
                    request,
                    "Duplicate entries detected for Foundry/Month: " +
                    ", ".join(duplicates) +
                    ". Each combination of Foundry, Month, and Scenario must be unique."
                )
            else:
                messages.success(request, "Pour Plan Data updated successfully!")
                plans = MasterDataPlan.objects.filter(version=scenario)
                formset = MasterDataPlanFormSet(queryset=plans)
        else:
            messages.error(request, "There were errors in the form. Please correct them.")
    else:
        formset = MasterDataPlanFormSet(queryset=plans)

    return render(request, 'website/update_pour_plan_data.html', {
        'formset': formset,
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

from django.core.management import call_command
from django.contrib import messages
from django.shortcuts import redirect
from website.models import AggregatedForecast, CalcualtedReplenishmentModel, CalculatedProductionModel, scenarios

from django.core.management import call_command
from django.contrib import messages
from django.shortcuts import redirect
from website.models import AggregatedForecast, CalcualtedReplenishmentModel, CalculatedProductionModel, scenarios


from django.db import transaction

import subprocess
from django.conf import settings

def run_management_command(command, *args):
    manage_py = settings.BASE_DIR / 'manage.py'
    cmd = ['python', str(manage_py), command] + [str(arg) for arg in args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

@login_required
@transaction.non_atomic_requests
def calculate_model(request, version):
    """
    Run the management commands to calculate the model for the given version.
    """
    try:
        # Step 1: Run the first command: populate_aggregated_forecast
        result = run_management_command('populate_aggregated_forecast', version)
        if result.returncode != 0:
            messages.error(request, f"Error in populate_aggregated_forecast: {result.stderr}")
            return redirect('list_scenarios')
        messages.success(request, f"Aggregated forecast has been successfully populated for version '{version}'.")

        # Step 2: Conditionally delete related records in CalcualtedReplenishmentModel
        if CalcualtedReplenishmentModel.objects.filter(version=version).exists():
            CalcualtedReplenishmentModel.objects.filter(version=version).delete()
            messages.success(request, f"Existing records in CalcualtedReplenishmentModel for version '{version}' have been deleted.")
        else:
            messages.warning(request, f"No existing records found in CalcualtedReplenishmentModel for version '{version}'.")

        # Step 3: Run the second command: populate_calculated_replenishment
        result = run_management_command('populate_calculated_replenishment', version)
        if result.returncode != 0:
            messages.error(request, f"Error in populate_calculated_replenishment: {result.stderr}")
            return redirect('list_scenarios')
        messages.success(request, f"Calculated replenishment has been successfully populated for version '{version}'.")

        # Step 4: Conditionally delete related records in CalculatedProductionModel
        if CalculatedProductionModel.objects.filter(version=version).exists():
            CalculatedProductionModel.objects.filter(version=version).delete()
            messages.success(request, f"Existing records in CalculatedProductionModel for version '{version}' have been deleted.")
        else:
            messages.warning(request, f"No existing records found in CalculatedProductionModel for version '{version}'.")

        # Step 5: Run the third command: populate_calculated_production
        result = run_management_command('populate_calculated_production', version)
        if result.returncode != 0:
            messages.error(request, f"Error in populate_calculated_production: {result.stderr}")
            return redirect('list_scenarios')
        messages.success(request, f"Calculated production has been successfully populated for version '{version}'.")

    except Exception as e:
        messages.error(request, f"An error occurred while calculating the model: {e}")

    # Redirect back to the list of scenarios
    return redirect('list_scenarios')

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

    # Prepare initial data for the formset
    initial_data = [
        {
            'Product': rec.Product.Product if rec.Product else '',
            'Site': rec.Site.SiteName if rec.Site else '',
            'ShippingDate': rec.ShippingDate,
            'Percentage': rec.Percentage,
            'id': rec.id,
        }
        for rec in records
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
                # Delete all current records for this scenario and re-create from formset
                MasterDataManuallyAssignProductionRequirement.objects.filter(version=scenario).delete()
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
                    return redirect('update_manually_assign_production_requirement', version=version)
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
        }
    )

@login_required
def delete_manually_assign_production_requirement(request, version):
    # Placeholder view for deleting manually assigned production requirement
    return HttpResponse("Delete Manually Assign Production Requirement - version: {}".format(version))

@login_required
def upload_manually_assign_production_requirement(request, version):
    # Placeholder view for uploading manually assigned production requirement
    return HttpResponse("Upload Manually Assign Production Requirement - version: {}".format(version))

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
                return redirect('update_manually_assign_production_requirement', version=version)
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

# ...existing code...

