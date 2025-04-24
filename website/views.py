from django.shortcuts import render, redirect
import pandas as pd
from django.core.files.storage import FileSystemStorage
from .models import SMART_Forecast_Model, scenarios
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
from .models import MasterDataProductModel, MasterDataProductPictures, MasterDataPlantModel
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
    scenario = get_object_or_404(scenarios, version=version)
    
    if request.method == 'POST':
        form = ScenarioForm(request.POST, instance=scenario)
        if form.is_valid():
            form.save()
            return redirect('list_scenarios')
        else:
            print(form.errors)  # Debugging: Print form errors if not valid
    else:
        form = ScenarioForm(instance=scenario)

    # Fetch models data to pass to the template
 

    return render(request, 'website/edit_scenario.html', {
        'scenario': scenario,
        'scenario_form': form,
      
    })


@login_required
def list_scenarios(request):
    all_scenarios = scenarios.objects.all()
    return render(request, 'website/list_scenarios.html', {'scenarios': all_scenarios})

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

from django.shortcuts import render, get_object_or_404
from .models import scenarios, SMART_Forecast_Model

def review_scenario(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    # Retrieve forecasts with pre-calculated Tonnes
    forecasts = SMART_Forecast_Model.objects.filter(version=scenario)
    chart_data = {}
    
    for forecast in forecasts:
        period = forecast.Period_AU.strftime('%Y-%m') if forecast.Period_AU else 'Unknown'
        product_group = forecast.Product_Group
        
        if period not in chart_data:
            chart_data[period] = {}
        if product_group not in chart_data[period]:
            chart_data[period][product_group] = 0
        chart_data[period][product_group] += forecast.Tonnes
    
    return render(request, 'website/review_scenario.html', {'scenario': scenario, 'chart_data': chart_data})


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

def ScenarioWarningList(request, version):
    scenario = get_object_or_404(scenarios, version=version)
    
    # Products in forecast but not in master data
    forecast_products = SMART_Forecast_Model.objects.values_list('Product', flat=True).distinct()
    products_not_in_master_data = forecast_products.exclude(Product__in=MasterDataProductModel.objects.values_list('Product', flat=True))
    
    products_without_dress_mass = MasterDataProductModel.objects.filter(Product__in=forecast_products).filter(DressMass__isnull=True) | MasterDataProductModel.objects.filter(Product__in=forecast_products).filter(DressMass=0)    
    
    context = {
        'scenario': scenario,
        'products_not_in_master_data': products_not_in_master_data,
        'products_without_dress_mass': products_without_dress_mass,
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























    


    
    
   
    



    