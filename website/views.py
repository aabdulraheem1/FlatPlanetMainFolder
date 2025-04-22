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

@login_required
def manage_forecasts(request, scenario_id):
    scenario = get_object_or_404(scenarios, id=scenario_id)
    forecasts = SMART_Forecast_Model.objects.filter(version=scenario)

    if request.method == 'POST':
        if 'upload_file' in request.POST:
            form = UploadFileForm(request.POST, request.FILES)
            if form.is_valid():
                file = request.FILES['file']
                df = pd.read_excel(file)
                SMART_Forecast_Model.objects.filter(version=scenario).delete()
                for index, row in df.iterrows():
                    SMART_Forecast_Model.objects.create(
                        version=scenario,
                        Forecast_Region=row['Forecast_Region'],
                        Product_Group=row['Product_Group'],
                        Product=row['Product'],
                        ProductFamilyDescription=row['ProductFamilyDescription'],
                        Customer_code=row['Customer_code'],
                        Location=row['Location'],
                        Forecasted_Weight_Curr=row['Forecasted_Weight_Curr'],
                        PriceAUD=row['PriceAUD'],
                        DP_Cycle=row['DP_Cycle'],
                        Period_AU=row['Period_AU'],
                        Qty=row['Qty']
                    )
                return redirect('manage_forecasts', scenario_id=scenario.id)
        elif 'edit_entry' in request.POST:
            form = SMARTForecastForm(request.POST)
            if form.is_valid():
                form.save()
                return redirect('manage_forecasts', scenario_id=scenario.id)
    else:
        upload_form = UploadFileForm()
        edit_form = SMARTForecastForm()

    return render(request, 'website/manage_forecasts.html', {
        'scenario': scenario,
        'forecasts': forecasts,
        'upload_form': upload_form,
        'edit_form': edit_form
    })

@login_required
def edit_forecast(request, id):
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

    return render(request, 'website/edit_scenario.html', {
        'scenario': scenario,
        'scenario_form': form
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















    


    
    
   
    



    