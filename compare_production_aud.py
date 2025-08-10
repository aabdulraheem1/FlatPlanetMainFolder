import os
import django

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import CalculatedProductionModel, InventoryProjectionModel
from django.db import connection
from decimal import Decimal

def compare_production_aud_tables():
    print("=== Comparing production_aud values between tables ===\n")
    
    # Get production_aud values from CalculatedProductionModel
    print("1. CalculatedProductionModel production_aud values:")
    production_records = CalculatedProductionModel.objects.filter(
        version__version="Jul 25 SPR"
    ).values('product__Product', 'parent_product_group', 'production_aud').order_by('product__Product')
    
    production_dict = {}
    production_by_group = {}
    total_production = Decimal('0')
    
    for record in production_records:
        product = record['product__Product']
        group = record['parent_product_group']
        value = Decimal(str(record['production_aud'] or 0))
        
        if product not in production_dict:
            production_dict[product] = Decimal('0')
        production_dict[product] += value
        
        if group not in production_by_group:
            production_by_group[group] = Decimal('0')
        production_by_group[group] += value
        
        total_production += value
    
    print(f"   Total records: {len(production_records)}")
    print(f"   Unique products: {len(production_dict)}")
    print(f"   Unique groups: {len(production_by_group)}")
    print(f"   Total production_aud: ${total_production:,.2f}")
    
    # Show top 10 product groups by production value
    top_groups = sorted(production_by_group.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\n   Top 10 groups by production_aud:")
    for group, value in top_groups:
        print(f"     {group}: ${value:,.2f}")
    
    # Show top 10 products by production value
    top_production = sorted(production_dict.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\n   Top 10 products by production_aud:")
    for product, value in top_production:
        print(f"     {product}: ${value:,.2f}")
    
    print("\n" + "="*60 + "\n")
    
    # Get production_aud values from InventoryProjectionModel
    print("2. InventoryProjectionModel production_aud values:")
    projection_records = InventoryProjectionModel.objects.filter(
        version__version="Jul 25 SPR"
    ).values('parent_product_group', 'production_aud').order_by('parent_product_group')
    
    projection_dict = {}
    total_projection = Decimal('0')
    
    for record in projection_records:
        group = record['parent_product_group']
        value = Decimal(str(record['production_aud'] or 0))
        
        if group not in projection_dict:
            projection_dict[group] = Decimal('0')
        projection_dict[group] += value
        total_projection += value
    
    print(f"   Total records: {len(projection_records)}")
    print(f"   Unique groups: {len(projection_dict)}")
    print(f"   Total production_aud: ${total_projection:,.2f}")
    
    # Show top 10 groups by production value
    top_projection = sorted(projection_dict.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\n   Top 10 groups by production_aud:")
    for group, value in top_projection:
        print(f"     {group}: ${value:,.2f}")
    
    print("\n" + "="*60 + "\n")
    
    # Compare the two datasets
    print("3. Comparison Analysis:")
    print(f"   Production total: ${total_production:,.2f}")
    print(f"   Projection total: ${total_projection:,.2f}")
    print(f"   Difference: ${abs(total_production - total_projection):,.2f}")
    
    # Compare by product groups (this is the valid comparison)
    print("\n   Comparing by Parent Product Groups:")
    common_groups = set(production_by_group.keys()) & set(projection_dict.keys())
    only_in_production = set(production_by_group.keys()) - set(projection_dict.keys())
    only_in_projection = set(projection_dict.keys()) - set(production_by_group.keys())
    
    print(f"   Common groups: {len(common_groups)}")
    print(f"   Groups only in Production: {len(only_in_production)}")
    if only_in_production:
        for group in sorted(only_in_production):
            print(f"     {group}: ${production_by_group[group]:,.2f}")
            
    print(f"   Groups only in Projection: {len(only_in_projection)}")
    if only_in_projection:
        for group in sorted(only_in_projection):
            print(f"     {group}: ${projection_dict[group]:,.2f}")
    
    # Check for value differences in common groups
    print("\n4. Value differences in common product groups:")
    differences = []
    
    for group in common_groups:
        prod_value = production_by_group[group]
        proj_value = projection_dict[group]
        diff = abs(prod_value - proj_value)
        
        if diff > Decimal('0.01'):  # Significant difference (more than 1 cent)
            differences.append((group, prod_value, proj_value, diff))
    
    if differences:
        print(f"   Found {len(differences)} groups with significant differences:")
        # Sort by difference amount
        differences.sort(key=lambda x: x[3], reverse=True)
        
        for group, prod_val, proj_val, diff in differences:
            print(f"     {group}:")
            print(f"       Production: ${prod_val:,.2f}")
            print(f"       Projection: ${proj_val:,.2f}")
            print(f"       Difference: ${diff:,.2f}")
    else:
        print("   No significant value differences found in common groups!")
    
    # Check specific Crawler Systems data
    print("\n" + "="*60 + "\n")
    print("5. Crawler Systems specific analysis:")
    
    # Get Crawler Systems production from CalculatedProductionModel
    crawler_production_total = production_by_group.get('Crawler Systems', Decimal('0'))
    print(f"   Crawler Systems in CalculatedProduction: ${crawler_production_total:,.2f}")
    
    # Get Crawler Systems projection from InventoryProjectionModel  
    crawler_projection_total = projection_dict.get('Crawler Systems', Decimal('0'))
    print(f"   Crawler Systems in InventoryProjection: ${crawler_projection_total:,.2f}")
    
    print(f"   Crawler Systems difference: ${abs(crawler_production_total - crawler_projection_total):,.2f}")
    
    # Get detailed Crawler Systems data from both tables
    with connection.cursor() as cursor:
        # Production data for Crawler Systems - detailed breakdown
        cursor.execute("""
            SELECT p.Product, SUM(cp.production_aud) as total_production
            FROM website_calculatedproductionmodel cp
            JOIN website_scenarios s ON cp.version_id = s.version
            JOIN website_masterdataproductmodel p ON cp.product_id = p.id
            WHERE s.version = 'Jul 25 SPR'
            AND cp.parent_product_group = 'Crawler Systems'
            GROUP BY p.Product
            ORDER BY total_production DESC
        """)
        crawler_production_detail = cursor.fetchall()
        
        # Projection data for Crawler Systems by month 
        cursor.execute("""
            SELECT ip.month, ip.production_aud
            FROM website_inventoryprojectionmodel ip
            JOIN website_scenarios s ON ip.version_id = s.version
            WHERE s.version = 'Jul 25 SPR'
            AND ip.parent_product_group = 'Crawler Systems'
            ORDER BY ip.month
        """)
        crawler_projection_detail = cursor.fetchall()
    
    print(f"\n   Crawler Systems products in CalculatedProduction: {len(crawler_production_detail)}")
    if crawler_production_detail:
        print("   Top products:")
        for product_code, value in crawler_production_detail[:10]:
            print(f"     {product_code}: ${value:,.2f}")
    
    print(f"\n   Crawler Systems monthly projections: {len(crawler_projection_detail)}")
    if crawler_projection_detail:
        print("   Monthly breakdown:")
        for month, value in crawler_projection_detail:
            print(f"     {month}: ${value:,.2f}")

if __name__ == "__main__":
    compare_production_aud_tables()
