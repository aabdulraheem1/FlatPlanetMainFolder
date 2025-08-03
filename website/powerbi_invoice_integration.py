"""
PowerBI Invoice Integration
Fetches latest customer invoice data from external PowerBI databases
Uses Polars for fast data processing
"""

import polars as pl
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

def get_latest_customer_invoices():
    """
    Fetch latest customer invoice data from both PowerBI databases using Polars
    Returns: Polars DataFrame with columns [Product, CustomerName, InvoiceDate]
    """
    
    # Database configurations
    databases = [
        {
            'name': 'bknew-sql02',
            'server': 'bknew-sql02',
            'database': 'Bradken_Data_Warehouse',
            'driver': 'ODBC Driver 17 for SQL Server'
        },
        {
            'name': 'bkgcc-sql',
            'server': 'bkgcc-sql',
            'database': 'Bradken_Data_Warehouse',
            'driver': 'ODBC Driver 17 for SQL Server'
        }
    ]
    
    all_invoice_data = []
    
    for db_config in databases:
        try:
            print(f"🔗 Connecting to {db_config['name']}...")
            
            # Create connection string
            connection_string = f"mssql+pyodbc://@{db_config['server']}/{db_config['database']}?driver={db_config['driver']}"
            engine = create_engine(connection_string)
            
            # SQL query to get latest customer invoices
            sql_query = """
            WITH LatestInvoices AS (
                SELECT 
                    p.ProductKey,
                    c.CustomerName,
                    d.DateValue as InvoiceDate,
                    ROW_NUMBER() OVER (PARTITION BY p.ProductKey ORDER BY d.DateValue DESC) as rn
                FROM PowerBI.Invoices i
                INNER JOIN PowerBI.Products p ON i.skProductId = p.skProductId
                INNER JOIN PowerBI.Dates d ON i.skInvoiceDateId = d.skDateId
                INNER JOIN PowerBI.Customers c ON i.skCustomerId = c.skCustomerId
                WHERE c.CustomerType = 'EXTERNAL CUSTOMER'
                    AND p.ProductKey IS NOT NULL
                    AND c.CustomerName IS NOT NULL
                    AND d.DateValue IS NOT NULL
            )
            SELECT 
                ProductKey,
                CustomerName,
                InvoiceDate
            FROM LatestInvoices 
            WHERE rn = 1
            ORDER BY ProductKey
            """
            
            # Execute query and convert to Polars DataFrame
            df = pl.read_database(sql_query, engine)
            
            if len(df) > 0:
                print(f"✅ Retrieved {len(df)} latest customer invoices from {db_config['name']}")
                all_invoice_data.append(df)
            else:
                print(f"⚠️  No invoice data found in {db_config['name']}")
            
            engine.dispose()
            
        except Exception as e:
            print(f"❌ Error connecting to {db_config['name']}: {str(e)}")
            logger.error(f"Database connection error for {db_config['name']}: {str(e)}")
            continue
    
    # Combine data from both databases using Polars
    if all_invoice_data:
        combined_df = pl.concat(all_invoice_data)
        
        # If same product appears in both databases, keep the most recent invoice
        final_df = combined_df.sort('InvoiceDate', descending=True).unique(subset=['ProductKey'], keep='first')
        
        print(f"🎯 Final dataset: {len(final_df)} unique products with latest customer invoices")
        return final_df
    else:
        print("❌ No invoice data retrieved from any database")
        return pl.DataFrame(schema={'ProductKey': pl.Utf8, 'CustomerName': pl.Utf8, 'InvoiceDate': pl.Date})


def get_customer_mapping_dict():
    """
    Get customer invoice data as a dictionary for fast lookup using Polars
    Returns: dict {product_id: {'customer': 'Customer Name', 'date': datetime.date}}
    """
    try:
        df = get_latest_customer_invoices()
        
        customer_mapping = {}
        for row in df.iter_rows(named=True):
            customer_mapping[row['ProductKey']] = {
                'customer_name': row['CustomerName'],
                'invoice_date': row['InvoiceDate'] if row['InvoiceDate'] is not None else None
            }
        
        print(f"📋 Created customer mapping for {len(customer_mapping)} products")
        return customer_mapping
        
    except Exception as e:
        print(f"❌ Error creating customer mapping: {str(e)}")
        logger.error(f"Customer mapping error: {str(e)}")
        return {}


def test_powerbi_connection():
    """Test function to verify PowerBI database connectivity"""
    print("🧪 Testing PowerBI database connections...")
    
    customer_mapping = get_customer_mapping_dict()
    
    if customer_mapping:
        print("✅ PowerBI integration test successful!")
        print(f"📊 Sample data (first 5 products):")
        for i, (product, data) in enumerate(customer_mapping.items()):
            if i >= 5:  # Show only first 5
                break
            print(f"   {product}: {data['customer_name']} ({data['invoice_date']})")
        return True
    else:
        print("❌ PowerBI integration test failed - no data retrieved")
        return False


if __name__ == "__main__":
    # Test the integration
    test_powerbi_connection()
