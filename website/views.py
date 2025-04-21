from django.shortcuts import render, redirect
import pandas as pd
from django.core.files.storage import FileSystemStorage
from .models import SMART_Forecast_Model
import pandas as pd
from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect
from .forms import UploadFileForm, ScenarioForm
from .models import SMART_Forecast_Model
from django.contrib.auth.decorators import login_required
import pyodbc
from django.shortcuts import render
from .models import MasterDataProductModel
from sqlalchemy import create_engine, text
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404, redirect
from .models import MasterDataProductModel, MasterDataProductPictures

from .forms import ProductForm, ProductPictureForm

from django.shortcuts import render, get_object_or_404, redirect








@login_required
def welcomepage(request):
    user_name = request.user.username
    
    return render(request, 'website/welcome_page.html', { 'user_name': user_name})




@login_required
def create_scenario(request):
    smart_forecast_exists = False
    if request.method == 'POST':
        form = ScenarioForm(request.POST)
        if form.is_valid():
            scenario = form.save(commit=False)
            scenario.created_by = request.user.username
            scenario.save()
            smart_forecast_exists = SMART_Forecast_Model.objects.filter(version=scenario).exists()
            return redirect('some_view_name')  # Redirect to a relevant view after saving
    else:
        form = ScenarioForm()
    
    return render(request, 'website/create_scenario.html', {'form': form, 'smart_forecast_exists': smart_forecast_exists})



@login_required
def fetch_data_from_mssql(request):

    # Delete all records in the model
    MasterDataProductModel.objects.all().delete()


    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    connection = engine.connect()

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
    products = MasterDataProductModel.objects.all()

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
    paginator = Paginator(products, 10)  # Show 20 products per page
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
        'product_picture': product_picture
    })






















    


    
    
   
    



    