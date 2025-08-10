"""
OPTIMIZED Customer Data Integration
Reads customer invoice data from local MasterDataProductModel for fast access
Customer data is populated locally via fetch_data_from_mssql endpoint from PowerBI sources
"""

import polars as pl
import logging

logger = logging.getLogger(__name__)

def get_latest_customer_invoices(single_product=None):
    """
    OPTIMIZED: Read customer invoice data from local MasterDataProductModel (FAST!)
    This replaces slow PowerBI database queries with fast local database access.
    Customer data is populated locally by the fetch_data_from_mssql endpoint.
    
    Args:
        single_product (str): If provided, filter results for this product only (much faster)
    Returns: Polars DataFrame with columns [ProductKey, CustomerName, InvoiceDate]
    """
    try:
        print("📋 Reading customer data from local MasterDataProductModel (optimized)...")
        
        # Import here to avoid circular imports
        from website.models import MasterDataProductModel
        
        if single_product:
            print(f"   🎯 Filtering for single product: {single_product}")
            # Fast query for single product
            products = MasterDataProductModel.objects.filter(
                Product=single_product,
                latest_customer_name__isnull=False
            ).values('Product', 'latest_customer_name', 'latest_invoice_date')
        else:
            # Fast query for all products with customer data
            products = MasterDataProductModel.objects.filter(
                latest_customer_name__isnull=False
            ).values('Product', 'latest_customer_name', 'latest_invoice_date')
        
        # Convert Django queryset to list for Polars
        data_list = []
        for product in products:
            data_list.append({
                'ProductKey': product['Product'],
                'CustomerName': product['latest_customer_name'],
                'InvoiceDate': product['latest_invoice_date']
            })
        
        if len(data_list) > 0:
            # Create Polars DataFrame
            df = pl.DataFrame(data_list)
            if single_product:
                print(f"✅ Retrieved {len(df)} customer records for product {single_product} from local database")
            else:
                print(f"✅ Retrieved {len(df)} customer records from local database")
            return df
        else:
            if single_product:
                print(f"⚠️  No customer data found for product {single_product} in local database")
                print("   💡 Run /fetch-data/ URL to populate customer data from PowerBI")
            else:
                print("⚠️  No customer data found in local database")
                print("   💡 Run /fetch-data/ URL to populate customer data from PowerBI")
            return pl.DataFrame(schema={'ProductKey': pl.Utf8, 'CustomerName': pl.Utf8, 'InvoiceDate': pl.Date})
            
    except Exception as e:
        print(f"❌ Error reading customer data from local database: {str(e)}")
        logger.error(f"Local customer data error: {str(e)}")
        return pl.DataFrame(schema={'ProductKey': pl.Utf8, 'CustomerName': pl.Utf8, 'InvoiceDate': pl.Date})


def get_customer_mapping_dict(single_product=None):
    """
    OPTIMIZED: Get customer data as a dictionary from local MasterDataProductModel (FAST!)
    This replaces slow PowerBI queries with fast local database access.
    
    Args:
        single_product (str): If provided, filter results for this product only (much faster)
    Returns: dict {product_id: {'customer_name': 'Customer Name', 'invoice_date': datetime.date}}
    """
    try:
        print("⚡ Reading customer data from local database (optimized approach)...")
        
        # Import here to avoid circular imports
        from website.models import MasterDataProductModel
        
        if single_product:
            print(f"   🎯 Filtering for single product: {single_product}")
            # Fast query for single product
            products = MasterDataProductModel.objects.filter(
                Product=single_product,
                latest_customer_name__isnull=False
            ).values('Product', 'latest_customer_name', 'latest_invoice_date')
        else:
            # Fast query for all products with customer data
            products = MasterDataProductModel.objects.filter(
                latest_customer_name__isnull=False
            ).values('Product', 'latest_customer_name', 'latest_invoice_date')
        
        # Create mapping dictionary (same format as original function)
        customer_mapping = {}
        for product_data in products:
            customer_mapping[product_data['Product']] = {
                'customer_name': product_data['latest_customer_name'],
                'invoice_date': product_data['latest_invoice_date']
            }
        
        if single_product:
            print(f"✅ Created fast customer mapping for {len(customer_mapping)} product(s) - filtered for {single_product}")
        else:
            print(f"✅ Created fast customer mapping for {len(customer_mapping)} products from local database")
            
        if len(customer_mapping) == 0:
            print("⚠️  No customer data found in local database")
            print("   💡 Run /fetch-data/ URL to populate customer data from PowerBI")
            
        return customer_mapping
        
    except Exception as e:
        print(f"❌ Error creating customer mapping from local database: {str(e)}")
        logger.error(f"Customer mapping error: {str(e)}")
        return {}


def test_powerbi_connection():
    """Test function to verify optimized local database connectivity"""
    print("🧪 Testing optimized customer data integration...")
    
    customer_mapping = get_customer_mapping_dict()
    
    if customer_mapping:
        print("✅ Optimized local database integration successful!")
        print(f"📊 Sample data (first 5 products):")
        for i, (product, data) in enumerate(customer_mapping.items()):
            if i >= 5:  # Show only first 5
                break
            print(f"   {product}: {data['customer_name']} ({data['invoice_date']})")
        return True
    else:
        print("⚠️  No customer data found in local database")
        print("   💡 Run /fetch-data/ URL to populate customer data from PowerBI")
        return False


if __name__ == "__main__":
    # Test the integration
    test_powerbi_connection()
