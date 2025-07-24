#!/usr/bin/env python
import os
import sys
import django

# Setup Django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MasterDataOrderBook, scenarios
from sqlalchemy import create_engine, text
from django.db.models import Count

def test_upload_logic():
    # Get scenario
    scenario = scenarios.objects.get(version='Jul 25 SPR Inv')
    
    # Check current state
    print("=== BEFORE NEW UPLOAD LOGIC ===")
    duplicates_before = MasterDataOrderBook.objects.filter(version=scenario).values('productkey').annotate(
        count=Count('id')
    ).filter(count__gt=1).count()
    total_before = MasterDataOrderBook.objects.filter(version=scenario).count()
    print(f'Duplicates: {duplicates_before}')
    print(f'Total records: {total_before}')
    
    # Show a specific example
    example_product = 'DELA90X320-6L'
    example_records = MasterDataOrderBook.objects.filter(version=scenario, productkey=example_product)
    print(f'\nExample product {example_product} current assignments:')
    for record in example_records:
        print(f'  Site: {record.site}')
    
    # Apply new upload logic
    print("\n=== APPLYING NEW UPLOAD LOGIC ===")
    
    # Database connection
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    
    with engine.connect() as connection:
        # New deduplication query
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
        
        result = connection.execute(query)
        
        # Delete existing records
        MasterDataOrderBook.objects.filter(version=scenario).delete()
        
        # Create new deduplicated records
        bulk_records = []
        processed_products = set()
        
        for row in result:
            if row.productkey not in processed_products:
                bulk_records.append(MasterDataOrderBook(
                    version=scenario,
                    site=row.site,
                    productkey=row.productkey
                ))
                processed_products.add(row.productkey)
        
        if bulk_records:
            MasterDataOrderBook.objects.bulk_create(bulk_records)
    
    # Check results
    print("\n=== AFTER NEW UPLOAD LOGIC ===")
    duplicates_after = MasterDataOrderBook.objects.filter(version=scenario).values('productkey').annotate(
        count=Count('id')
    ).filter(count__gt=1).count()
    total_after = MasterDataOrderBook.objects.filter(version=scenario).count()
    print(f'Duplicates: {duplicates_after}')
    print(f'Total records: {total_after}')
    
    # Check the same example product
    example_records_after = MasterDataOrderBook.objects.filter(version=scenario, productkey=example_product)
    print(f'\nExample product {example_product} after deduplication:')
    for record in example_records_after:
        print(f'  Site: {record.site}')
    
    print(f'\n=== SUMMARY ===')
    print(f'Duplicates eliminated: {duplicates_before - duplicates_after}')
    print(f'Records reduced: {total_before - total_after}')

if __name__ == "__main__":
    test_upload_logic()
