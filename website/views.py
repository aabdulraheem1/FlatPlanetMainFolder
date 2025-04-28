from django.shortcuts import render, redirect
import pandas as pd
from django.core.files.storage import FileSystemStorage
from .models import SMART_Forecast_Model, scenarios, MasterDataHistoryOfProductionModel, MasterDataCastToDespatchModel
import pandas as pd
from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect
from .forms import UploadFileForm, ScenarioForm, SMARTForecastForm
from .models import SMART_Forecast_Model
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

@login_required
def welcomepage(request):
    user_name = request.user.username
    
    return render(request, 'website/welcome_page.html', { 'user_name': user_name})

@login_required
def create_scenario(request):
    
    if request.method == 'POST':
        form = ScenarioForm(request.POST)
        if form.is_valid():
            scenario = form.save(commit=False)
            scenario.created_by = request.user.username
            scenario.save()            
            return redirect('welcomepage')  # Redirect to a relevant view after saving
    else:
        form = ScenarioForm()
    
    return render(request, 'website/create_scenario.html', {'form': form})

@login_required
def fetch_data_from_mssql(request):
    # Connect to the database
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    connection = engine.connect()

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

    connection.close()
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

        if product_form.is_valid() and picture_form.is_valid():
            product_instance = product_form.save()
            
            # Delete old picture if a new one is uploaded
            

            picture_instance = picture_form.save(commit=False)
            picture_instance.product = product_instance
            picture_instance.save()
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
    
@login_required
def plants_fetch_data_from_mssql(request):
    # Connect to the database
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    connection = engine.connect()

    # Fetch new data from the database
    query = text("SELECT * from PowerBI.Site where RowEndDate IS NULL")
    result = connection.execute(query)

    SiteName_dict = {}

    for row in result:  
        SiteName_dict[row.SiteName] = {
            'Company': row.Company,
            'Country': row.Country,
            'Location': row.Location,
            'PlantRegion': row.PlantRegion,
            'SiteType': row.SiteType,
        }

    # Update or create records in the model
    for site, data in SiteName_dict.items():
        MasterDataPlantModel.objects.update_or_create(
            SiteName=site,
            defaults=data
        )

    connection.close()
    return redirect('PlantsList')

@login_required
def fetch_data_from_mssql(request):
    # Connect to the database
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    connection = engine.connect()

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

    connection.close()
    return redirect('ProductsList')

@login_required
def plants_list(request):
    user_name = request.user.username
    sites = MasterDataPlantModel.objects.all().order_by('SiteName')

    # Filtering logic
    Site_filter = request.GET.get('SiteName', '')
    Company_filter = request.GET.get('Company', '')
    Location_filter = request.GET.get('Location', '')
    SiteType_filter = request.GET.get('SiteType', '')
    

    if Site_filter:
        sites = sites.filter(SiteName__icontains=Site_filter)
    if Company_filter:
        sites = sites.filter(Company__icontains=Company_filter)
    if Location_filter:
        sites = sites.filter(Location__icontains=Location_filter)
    if SiteType_filter:
        sites = sites.filter(SiteType__icontains=SiteType_filter)
    

    # Pagination logic
    paginator = Paginator(sites, 15)  # Show 20 products per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'Site_Filter': Site_filter,
        'Company_Filter': Company_filter,
        'Location_Filter': Location_filter,
        'SiteType_Filter': SiteType_filter,
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
def delete_forecast(request, id):
    forecast = get_object_or_404(SMART_Forecast_Model, id=id)
    scenario_id = forecast.version.id
    forecast.delete()
    return redirect('manage_forecasts', scenario_id=scenario_id)

@login_required
def edit_scenario(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)

    # Check if there is data related to the scenario
    product_allocation_order_book = MasterDataOrderBook.objects.filter(version=scenario).exists()
    production_allocation_pouring_history = MasterDataHistoryOfProductionModel.objects.filter(version=scenario).exists()

    # Check if there is data for SMART and Not in SMART forecasts
    smart_forecast_data = SMART_Forecast_Model.objects.filter(version=scenario, Data_Source='SMART').exists()
    not_in_smart_forecast_data = SMART_Forecast_Model.objects.filter(version=scenario, Data_Source='Not in SMART').exists()
    on_hand_stock_in_transit = MasterDataInventory.objects.filter(version=scenario).exists()
    master_data_freight_has_data = MasterDataFreightModel.objects.filter(version=scenario).exists()

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
        'on_hand_stock_in_transit': on_hand_stock_in_transit,
        'master_data_freight_has_data': master_data_freight_has_data,
        'missing_regions': missing_regions,  # Pass missing regions to the template
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

from django.core.exceptions import ObjectDoesNotExist
from django.shortcuts import redirect, render
from django.core.files.storage import FileSystemStorage
import pandas as pd

@login_required
def upload_forecast(request, forecast_type):
    if request.method == 'POST' and request.FILES['file']:
        file = request.FILES['file']
        fs = FileSystemStorage()
        filename = fs.save(file.name, file)
        file_path = fs.path(filename)

        # Read the Excel file
        df = pd.read_excel(file_path)

        try:
            # Get the scenario version
            version = scenarios.objects.get(version=request.POST.get('version'))
        except ObjectDoesNotExist:
            # Handle the case where the scenario does not exist
            return render(request, 'upload_forecast.html', {
                'error_message': 'Scenario matching query does not exist.'
            })

        # Set the data source based on the forecast type
        data_source = 'SMART' if forecast_type == 'SMART' else 'Not in SMART'

        # Iterate over the rows and save to the model
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

        return redirect('edit_scenario', version=version.version)  # Redirect to a relevant view after upload

    return render(request, 'upload_forecast.html')

from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.csrf import csrf_protect
from django.core.paginator import Paginator
from .models import SMART_Forecast_Model
from .forms import SMARTForecastFormSet, ForecastFilterForm

@csrf_protect
def edit_forecasts(request, version, forecast_type):
    scenario = get_object_or_404(scenarios, version=version)
    
    if forecast_type == 'SMART':
        queryset = SMART_Forecast_Model.objects.filter(version=scenario, Data_Source='SMART').order_by('id')
    else:
        queryset = SMART_Forecast_Model.objects.filter(version=scenario, Data_Source='Not in SMART').order_by('id')

    filter_form = ForecastFilterForm(request.GET)
    if filter_form.is_valid():
        if filter_form.cleaned_data['forecast_region']:
            queryset = queryset.filter(Forecast_Region=filter_form.cleaned_data['forecast_region'])
        if filter_form.cleaned_data['product_group']:
            queryset = queryset.filter(Product_Group=filter_form.cleaned_data['product_group'])
        if filter_form.cleaned_data['customer_code']:
            queryset = queryset.filter(Customer_code=filter_form.cleaned_data['customer_code'])

    paginator = Paginator(queryset, 20)  # Show 20 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    if request.method == 'POST':
        formset = SMARTForecastFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            formset.save()
            return redirect('edit_scenario', version=version)
        else:
            print(formset.errors)  # Debugging: Print formset errors if not valid
            form_errors = formset.errors
    else:
        formset = SMARTForecastFormSet(queryset=page_obj.object_list)

    return render(request, 'website/edit_forecasts.html', {
        'formset': formset,
        'scenario': scenario,
        'page_obj': page_obj,
        'filter_form': filter_form,
        'form_errors': form_errors if request.method == 'POST' else None
    })


from django.shortcuts import render
from django.core.serializers.json import DjangoJSONEncoder
import json

@login_required
def review_scenario(request, version):
    user_name = request.user.username

    # Fetch the version object
    version = scenarios.objects.get(version=version)

    # Fetch data from AggregatedForecast model filtered by the version
    forecasts = AggregatedForecast.objects.filter(version=version)

    # Process data to structure it for the chart
    chart_data = {}
    region_data = {}
    for forecast in forecasts:
        period = forecast.period.strftime('%Y-%m-%d')
        parent_group = forecast.parent_product_group_description
        tonnes = forecast.tonnes
        region = forecast.forecast_region

        if period not in chart_data:
            chart_data[period] = {}
        if parent_group not in chart_data[period]:
            chart_data[period][parent_group] = 0
        chart_data[period][parent_group] += tonnes

        if period not in region_data:
            region_data[period] = {}
        if region not in region_data[period]:
            region_data[period][region] = 0
        region_data[period][region] += tonnes

    # Convert data to JSON format
    chart_data_json = json.dumps(chart_data, cls=DjangoJSONEncoder)
    region_data_json = json.dumps(region_data, cls=DjangoJSONEncoder)

    return render(request, 'website/review_scenario.html', {
        'version': version.version,
        'chart_data_parent_product_group': chart_data_json,
        'chart_data_forecast_region': region_data_json,
        'user_name': user_name,
    })




# signals.py
from django.db.models.signals import pre_save
from django.dispatch import receiver
from .models import SMART_Forecast_Model, MasterDataProductModel

@receiver(pre_save, sender=SMART_Forecast_Model)
def calculate_tonnes(sender, instance, **kwargs):
    dress_mass = MasterDataProductModel.objects.filter(Product=instance.Product).values_list('DressMass', flat=True).first()
    if instance.Qty is not None and dress_mass is not None:
        instance.Tonnes = instance.Qty * dress_mass
    else:
        instance.Tonnes = (instance.PriceAUD * 0.65) / 5 if instance.PriceAUD is not None else 0


from django.shortcuts import render, get_object_or_404
from .models import SMART_Forecast_Model, MasterDataProductModel

@login_required
def ScenarioWarningList(request, version):
    user_name = request.user.username
    scenario = get_object_or_404(scenarios, version=version)
    
    # Products in forecast but not in master data
    forecast_products = SMART_Forecast_Model.objects.values_list('Product', flat=True).distinct()
    products_not_in_master_data = forecast_products.exclude(Product__in=MasterDataProductModel.objects.values_list('Product', flat=True))
    
    products_without_dress_mass = MasterDataProductModel.objects.filter(Product__in=forecast_products).filter(DressMass__isnull=True) | MasterDataProductModel.objects.filter(Product__in=forecast_products).filter(DressMass=0)
    
    # Regions in forecast but not defined in the freight model
    forecast_regions = SMART_Forecast_Model.objects.filter(version=scenario).values_list('Forecast_Region', flat=True).distinct()
    defined_regions = MasterDataFreightModel.objects.filter(version=scenario).values_list('ForecastRegion__Forecast_region', flat=True).distinct()
    missing_regions = set(forecast_regions) - set(defined_regions)

    context = {
        'scenario': scenario,
        'products_not_in_master_data': products_not_in_master_data,
        'products_without_dress_mass': products_without_dress_mass,
        'missing_regions': missing_regions,  # Add missing regions to the context
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
    connection = engine.connect()

    try:
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
    finally:
        # Close the database connection
        connection.close()

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
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Database connection details
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    connection = engine.connect()

    try:
        # SQL query to fetch data
        query = text("""
            WITH LatestProduction AS (
                SELECT DISTINCT
                    Site.SiteName AS Foundry,
                    Product.ProductKey AS Product,
                    TRY_CONVERT(DATE, Dates.DateValue) AS ProductionMonth,
                    HeatProducts.CastQty AS ProductionQty,
                    ROW_NUMBER() OVER (
                        PARTITION BY Product.ProductKey
                        ORDER BY TRY_CONVERT(DATE, Dates.DateValue) DESC
                    ) AS RowNum
                FROM PowerBI.Products AS Product
                INNER JOIN PowerBI.HeatProducts AS HeatProducts
                    ON Product.skProductId = HeatProducts.skProductId
                INNER JOIN PowerBI.Site AS Site
                    ON HeatProducts.SkSiteId = Site.skSiteId
                INNER JOIN PowerBI.Dates AS Dates
                    ON HeatProducts.TapTime = Dates.DateValue
                WHERE Site.SiteName IN ('MTJ1', 'COI2', 'XUZ1', 'WOD1', 'MER1', 'WUN1')
            )
            SELECT Foundry, Product, ProductionMonth, ProductionQty
            FROM LatestProduction
            WHERE RowNum = 1
        """)

        # Execute the query
        result = connection.execute(query)

        # Populate the MasterDataHistoryOfProductionModel
        for row in result:
            MasterDataHistoryOfProductionModel.objects.update_or_create(
                version=scenario,
                Product=row.Product,
                defaults={
                    'Foundry': row.Foundry,
                    'ProductionMonth': row.ProductionMonth,
                    'ProductionQty': row.ProductionQty,
                }
            )
    finally:
        # Close the database connection
        connection.close()

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
    # Get the current scenario
    scenario = get_object_or_404(scenarios, version=version)

    # Filter records
    production_filter = request.GET.get('production', '')  # Get the filter value from the query string
    queryset = MasterDataHistoryOfProductionModel.objects.filter(version=scenario)
    if production_filter:
        queryset = queryset.filter(Product__icontains=production_filter)  # Filter by product name

    # Apply ordering before slicing
    queryset = queryset.order_by('id')  # Ensure the queryset is ordered before pagination

    # Paginate the queryset
    paginator = Paginator(queryset, 10)  # Show 10 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Create a formset for the current page
    ProductionHistoryFormSet = modelformset_factory(
        MasterDataHistoryOfProductionModel,
        fields=('Foundry', 'Product', 'ProductionMonth', 'ProductionQty'),
        extra=0
    )
    formset = ProductionHistoryFormSet(queryset=page_obj.object_list)

    if request.method == 'POST':
        formset = ProductionHistoryFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            formset.save()
            return redirect('edit_scenario', version=version)

    return render(request, 'website/update_production_history.html', {
        'formset': formset,
        'page_obj': page_obj,
        'production_filter': production_filter,
        'scenario': scenario,
    })

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
    connection = engine.connect()

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

        # Query to join tables and fetch inventory data
        query = text(f"""
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
        wip_query = text(f"""
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

@login_required
def forecast_region_list(request):
    """
    View to display the list of forecast regions.
    """
    forecast_regions = MasterDataForecastRegionModel.objects.all()
    return render(request, 'website/forecast_region_list.html', {'forecast_regions': forecast_regions})


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

from django.forms import modelformset_factory

@login_required
def update_master_data_freight(request, version):
    """
    View to update and add new Master Data Freight records.
    """
    # Get the records for the given version
    freight_records = MasterDataFreightModel.objects.filter(version=version)

    # Create a formset for the records, allowing extra empty forms for new entries
    FreightFormSet = modelformset_factory(
        MasterDataFreightModel,
        fields=['ForecastRegion', 'ManufacturingSite', 'PlantToDomesticPortDays', 'OceanFreightDays', 'PortToCustomerDays'],
        extra=1  # Allow one extra form for adding new records
    )

    if request.method == 'POST':
        formset = FreightFormSet(request.POST, queryset=freight_records)
        if formset.is_valid():
            # Save the formset
            instances = formset.save(commit=False)
            for instance in instances:
                instance.version = version  # Ensure the version is set for new records
                instance.save()
            return redirect('edit_scenario', version=version)
    else:
        formset = FreightFormSet(queryset=freight_records)

    return render(request, 'website/update_master_data_freight.html', {'formset': formset, 'version': version})


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
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        if source_version:
            source_records = MasterDataFreightModel.objects.filter(version=source_version)
            for record in source_records:
                record.pk = None  # Create a new record
                record.version = version
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
    if request.method == 'POST':
        source_version = request.POST.get('source_version')
        if source_version:
            source_records = MasterDataCastToDespatchModel.objects.filter(version=source_version)
            for record in source_records:
                record.pk = None  # Create a new record
                record.version = version
                record.save()
            return redirect('edit_scenario', version=version)
    return render(request, 'website/copy_master_data_casto_to_despatch_days.html', {'version': version})


















