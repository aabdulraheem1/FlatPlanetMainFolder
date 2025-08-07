from django.shortcuts import render, redirect
from django.contrib import messages
from django.db import transaction
from django.utils import timezone
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, date
from website.models import ReceiptedQuantity, MasterDataPlantModel, MasterDataProductModel
from django.core.paginator import Paginator
from django.db.models import Q
import traceback
from django.contrib.auth.decorators import login_required

@login_required
def receipted_quantities_refresh(request):
    """View to refresh receipted quantities data from PowerBI database"""

    user_name = request.user.username if request.user.is_authenticated else 'Guest'
    
    if request.method == 'POST':
        try:
            # Database connection
            Server = 'bknew-sql02'
            Database = 'Bradken_Data_Warehouse'
            Driver = 'ODBC Driver 17 for SQL Server'
            Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
            engine = create_engine(Database_Con)
            
            # Clear all existing receipted quantity records before refresh
            deleted_count = ReceiptedQuantity.objects.all().count()
            ReceiptedQuantity.objects.all().delete()
            
            # SQL Query to fetch receipted quantities only for outsource suppliers
            outsource_suppliers = MasterDataPlantModel.objects.filter(
                mark_as_outsource_supplier=True
            ).values_list('SiteName', flat=True)
            
            if not outsource_suppliers:
                messages.warning(request, "No outsource suppliers found. Please mark suppliers as outsource suppliers first.")
                return redirect('receipted_quantities_refresh')
            
            # Convert to list for SQL IN clause
            supplier_list = "', '".join(outsource_suppliers)
            
            query = text(f"""
                SELECT 
                    pr.[Transaction Qty] AS TransactionQty,
                    d.DateValue,
                    p.ProductKey,
                    p.DressMass,
                    s.VendorID,
                    s.TradingName
                FROM [PowerBI].[PO Receipts] pr
                INNER JOIN PowerBI.Products p ON pr.skProductId = p.skProductId
                INNER JOIN PowerBI.Supplier s ON pr.skSupplierId = s.skSupplierId
                INNER JOIN PowerBI.Dates d ON pr.skReceiptDateId = d.skDateId  
                WHERE d.DateValue >= '2024-01-01'  -- Filter for recent data
                AND s.VendorID IN ('{supplier_list}')  -- Only outsource suppliers
                ORDER BY d.DateValue DESC
            """)
            
            # Execute query and get data
            df = pd.read_sql(query, engine)
            
            if df.empty:
                messages.warning(request, "No data found for outsource suppliers in PowerBI database.")
                return redirect('receipted_quantities_refresh')
            
            # Process and save data
            saved_count = 0
            error_count = 0
            
            with transaction.atomic():
                
                for index, row in df.iterrows():
                    try:
                        # Convert date
                        receipt_date = pd.to_datetime(row['DateValue']).date()
                        month_of_supply = receipt_date.replace(day=1)  # First day of month
                        
                        # Calculate purchased tonnes
                        dress_mass = row['DressMass'] if pd.notna(row['DressMass']) else 0
                        transaction_qty = row['TransactionQty'] if pd.notna(row['TransactionQty']) else 0
                        purchased_tonnes = transaction_qty * dress_mass / 1000 if dress_mass > 0 else 0  # Convert to tonnes
                        
                        # Get supplier (should already be marked as outsource)
                        supplier = None
                        if pd.notna(row['VendorID']):
                            try:
                                supplier = MasterDataPlantModel.objects.get(
                                    SiteName=row['VendorID'],
                                    mark_as_outsource_supplier=True  # Ensure it's an outsource supplier
                                )
                            except MasterDataPlantModel.DoesNotExist:
                                # Skip this record as supplier is not marked as outsource
                                print(f"Skipping {row['VendorID']} - not marked as outsource supplier")
                                error_count += 1
                                continue
                        
                        if supplier and pd.notna(row['ProductKey']):
                            # Create new record (no need to check for existing since we deleted all)
                            ReceiptedQuantity.objects.create(
                                supplier=supplier,
                                product=row['ProductKey'],
                                purchased_qty=transaction_qty,
                                purchased_tonnes=purchased_tonnes,
                                month_of_supply=month_of_supply,
                                receipt_date=receipt_date,
                                dress_mass=dress_mass
                            )
                            saved_count += 1
                        else:
                            error_count += 1
                            
                    except Exception as e:
                        print(f"Error processing row {index}: {e}")
                        error_count += 1
                        continue
            
            messages.success(request, f"Data refresh completed! Deleted: {deleted_count} old records, Imported: {saved_count} new records, Errors: {error_count}")
            
        except Exception as e:
            print(f"Database error: {e}")
            traceback.print_exc()
            messages.error(request, f"Error connecting to database: {str(e)}")
    
    # Get current data for display (only outsource suppliers)
    receipted_data = ReceiptedQuantity.objects.select_related('supplier').filter(
        supplier__mark_as_outsource_supplier=True
    ).order_by('-month_of_supply', 'supplier__SiteName')
    
    # Apply filters
    supplier_filter = request.GET.get('supplier', '')
    product_filter = request.GET.get('product', '')
    month_filter = request.GET.get('month', '')
    
    if supplier_filter:
        receipted_data = receipted_data.filter(supplier__SiteName__icontains=supplier_filter)
    if product_filter:
        receipted_data = receipted_data.filter(product__icontains=product_filter)
    if month_filter:
        try:
            month_date = datetime.strptime(month_filter, '%Y-%m').date()
            receipted_data = receipted_data.filter(month_of_supply=month_date)
        except ValueError:
            pass
    
    # Pagination
    paginator = Paginator(receipted_data, 20)  # Show 20 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Get unique values for filters
    suppliers = MasterDataPlantModel.objects.filter(
        mark_as_outsource_supplier=True,
        SiteName__in=ReceiptedQuantity.objects.values_list('supplier__SiteName', flat=True).distinct()
    ).order_by('SiteName')
    
    products = ReceiptedQuantity.objects.values_list('product', flat=True).distinct().order_by('product')
    months = ReceiptedQuantity.objects.values_list('month_of_supply', flat=True).distinct().order_by('-month_of_supply')
    
    context = {
        'page_obj': page_obj,
        'suppliers': suppliers,
        'products': products,
        'months': months,
        'supplier_filter': supplier_filter,
        'product_filter': product_filter,
        'month_filter': month_filter,
        'total_records': receipted_data.count(),
        'user_name': user_name,
    }
    
    return render(request, 'website/receipted_quantities_refresh.html', context)
