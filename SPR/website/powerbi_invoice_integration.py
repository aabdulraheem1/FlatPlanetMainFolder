"""
REAL PowerBI Invoice Data Integration
Connects to actual PowerBI database to fetch genuine invoice data
NO FALLBACK DATA - Only returns data if it exists in PowerBI invoice tables
"""

import polars as pl
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

def get_latest_customer_invoices(single_product=None):
    """
    REAL PowerBI Integration: Query actual PowerBI invoice tables for genuine invoice data.
    NO FAKE DATA - Only returns records if they exist in PowerBI.FactInvoice or similar tables.
    
    Args:
        single_product (str): If provided, filter results for this product only (much faster)
    Returns: Polars DataFrame with columns [ProductKey, CustomerName, InvoiceDate]
    """
    try:
        print("� Connecting to REAL PowerBI database for invoice data...")
        
        # PowerBI Database connection
        Server = 'bknew-sql02'
        Database = 'Bradken_Data_Warehouse'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        
        engine = create_engine(Database_Con)
        
        # Query REAL PowerBI invoice data (not fake local data)
        if single_product:
            print(f"   🎯 Searching PowerBI invoices for product: {single_product}")
            query = """
            SELECT TOP 1000
                fi.ProductKey,
                c.CustomerName,
                fi.InvoiceDate
            FROM PowerBI.FactInvoice fi
            LEFT JOIN PowerBI.Customers c ON fi.CustomerKey = c.CustomerKey
            WHERE fi.ProductKey = ?
            AND fi.InvoiceDate IS NOT NULL
            AND c.CustomerName IS NOT NULL
            ORDER BY fi.InvoiceDate DESC
            """
            df = pd.read_sql(query, engine, params=(single_product,))
        else:
            print("   📊 Querying ALL PowerBI invoice records...")
            query = """
            SELECT 
                fi.ProductKey,
                c.CustomerName,
                MAX(fi.InvoiceDate) as InvoiceDate
            FROM PowerBI.FactInvoice fi
            LEFT JOIN PowerBI.Customers c ON fi.CustomerKey = c.CustomerKey
            WHERE fi.ProductKey IS NOT NULL
            AND fi.InvoiceDate IS NOT NULL
            AND c.CustomerName IS NOT NULL
            GROUP BY fi.ProductKey, c.CustomerName
            """
            df = pd.read_sql(query, engine)
        
        engine.dispose()
        
        if len(df) > 0:
            # Convert to Polars DataFrame
            polars_df = pl.from_pandas(df)
            if single_product:
                print(f"✅ Found {len(polars_df)} REAL invoice records for {single_product} in PowerBI")
            else:
                print(f"✅ Found {len(polars_df)} REAL invoice records in PowerBI database")
            return polars_df
        else:
            if single_product:
                print(f"⚠️  No REAL invoice data found for {single_product} in PowerBI database")
                print("   � This product has never been invoiced (NEW product)")
            else:
                print("⚠️  No REAL invoice data found in PowerBI database")
            return pl.DataFrame(schema={'ProductKey': pl.Utf8, 'CustomerName': pl.Utf8, 'InvoiceDate': pl.Date})
            
    except Exception as e:
        print(f"❌ Error connecting to PowerBI database: {str(e)}")
        logger.error(f"PowerBI database error: {str(e)}")
        return pl.DataFrame(schema={'ProductKey': pl.Utf8, 'CustomerName': pl.Utf8, 'InvoiceDate': pl.Date})


def get_customer_mapping_dict(single_product=None):
    """
    REAL PowerBI Integration: Get customer invoice data as dictionary from actual PowerBI tables.
    NO FAKE DATA - Only returns data if it exists in PowerBI invoice tables.
    
    Args:
        single_product (str): If provided, filter results for this product only (much faster)
    Returns: dict {product_id: {'customer_name': 'Customer Name', 'invoice_date': datetime.date}}
    """
    try:
        print("🔗 Connecting to REAL PowerBI database for customer mapping...")
        
        # Get data from real PowerBI tables
        customer_df = get_latest_customer_invoices(single_product)
        
        if len(customer_df) > 0:
            # Create mapping dictionary from REAL PowerBI data
            customer_mapping = {}
            for row in customer_df.iter_rows(named=True):
                customer_mapping[row['ProductKey']] = {
                    'customer_name': row['CustomerName'],
                    'invoice_date': row['InvoiceDate']
                }
            
            if single_product:
                print(f"✅ Created REAL customer mapping for {len(customer_mapping)} product(s) - filtered for {single_product}")
            else:
                print(f"✅ Created REAL customer mapping for {len(customer_mapping)} products from PowerBI database")
                
            return customer_mapping
        else:
            print("⚠️  No REAL invoice data found in PowerBI database")
            if single_product:
                print(f"   � Product {single_product} has never been invoiced (NEW product)")
            return {}
        
    except Exception as e:
        print(f"❌ Error creating customer mapping from PowerBI database: {str(e)}")
        logger.error(f"PowerBI customer mapping error: {str(e)}")
        return {}


def test_powerbi_connection():
    """Test function to verify REAL PowerBI database connectivity"""
    print("🧪 Testing REAL PowerBI invoice data integration...")
    
    customer_mapping = get_customer_mapping_dict()
    
    if customer_mapping:
        print("✅ REAL PowerBI database integration successful!")
        print(f"📊 Sample REAL invoice data (first 5 products):")
        for i, (product, data) in enumerate(customer_mapping.items()):
            if i >= 5:  # Show only first 5
                break
            print(f"   {product}: {data['customer_name']} ({data['invoice_date']})")
        return True
    else:
        print("⚠️  No REAL invoice data found in PowerBI database")
        print("   📋 This means no products have been invoiced, or PowerBI connection failed")
        return False


def clear_fake_invoice_data():
    """
    Clear any fake/test invoice data from local database.
    This ensures we only use REAL PowerBI invoice data going forward.
    """
    try:
        print("🧹 Clearing fake/test invoice data from local database...")
        
        # Import here to avoid circular imports
        from website.models import MasterDataProductModel
        from django.utils import timezone
        
        # Clear all invoice data - will be repopulated with REAL data only
        updated_count = MasterDataProductModel.objects.filter(
            latest_invoice_date__isnull=False
        ).update(
            latest_customer_name=None,
            latest_invoice_date=None,
            customer_data_last_updated=timezone.now(),
            product_type=None  # Will be set correctly based on REAL data
        )
        
        print(f"✅ Cleared fake invoice data from {updated_count} products")
        print("   📋 Products will now be classified based on REAL PowerBI invoice data only")
        
        return updated_count
        
    except Exception as e:
        print(f"❌ Error clearing fake invoice data: {str(e)}")
        logger.error(f"Clear fake data error: {str(e)}")
        return 0


if __name__ == "__main__":
    # Test the integration
    test_powerbi_connection()
