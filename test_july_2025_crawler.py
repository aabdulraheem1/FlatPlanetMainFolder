import os
import django

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import CalculatedProductionModel, InventoryProjectionModel
from django.db import connection
from decimal import Decimal
from datetime import datetime

def test_july_2025_crawler_systems():
    print("=== JULY 2025 CRAWLER SYSTEMS COMPARISON ===\n")
    
    # 1. Get Crawler Systems production data for July 2025 from CalculatedProductionModel
    print("1. CalculatedProductionModel - July 2025 Crawler Systems:")
    july_production = CalculatedProductionModel.objects.filter(
        version__version="Jul 25 SPR",
        parent_product_group="Crawler Systems",
        pouring_date__year=2025,
        pouring_date__month=7
    ).values('product__Product', 'pouring_date', 'production_aud', 'site__SiteName')
    
    total_july_production = Decimal('0')
    production_by_date = {}
    production_by_product = {}
    production_by_site = {}
    
    for record in july_production:
        product = record['product__Product']
        date = record['pouring_date']
        site = record['site__SiteName']
        value = Decimal(str(record['production_aud'] or 0))
        
        total_july_production += value
        
        # Group by date
        if date not in production_by_date:
            production_by_date[date] = Decimal('0')
        production_by_date[date] += value
        
        # Group by product
        if product not in production_by_product:
            production_by_product[product] = Decimal('0')
        production_by_product[product] += value
        
        # Group by site
        if site not in production_by_site:
            production_by_site[site] = Decimal('0')
        production_by_site[site] += value
    
    print(f"   Total July 2025 records: {len(july_production)}")
    print(f"   Total July 2025 production_aud: ${total_july_production:,.2f}")
    print(f"   Unique products: {len(production_by_product)}")
    print(f"   Unique sites: {len(production_by_site)}")
    print(f"   Unique dates: {len(production_by_date)}")
    
    # Show top products
    if production_by_product:
        print("\n   Top 10 products by production_aud:")
        top_products = sorted(production_by_product.items(), key=lambda x: x[1], reverse=True)[:10]
        for product, value in top_products:
            print(f"     {product}: ${value:,.2f}")
    
    # Show by site
    if production_by_site:
        print("\n   Production by site:")
        for site, value in sorted(production_by_site.items(), key=lambda x: x[1], reverse=True):
            print(f"     {site}: ${value:,.2f}")
    
    # Show daily breakdown
    if production_by_date:
        print("\n   Daily production breakdown:")
        for date, value in sorted(production_by_date.items()):
            print(f"     {date}: ${value:,.2f}")
    
    print("\n" + "="*70 + "\n")
    
    # 2. Get Crawler Systems projection data for July 2025 from InventoryProjectionModel
    print("2. InventoryProjectionModel - July 2025 Crawler Systems:")
    
    july_projection = InventoryProjectionModel.objects.filter(
        version__version="Jul 25 SPR",
        parent_product_group="Crawler Systems",
        month__year=2025,
        month__month=7
    ).values('month', 'production_aud', 'opening_inventory_aud', 'closing_inventory_aud', 'cogs_aud', 'revenue_aud')
    
    total_july_projection = Decimal('0')
    projection_details = []
    
    for record in july_projection:
        month = record['month']
        prod_aud = Decimal(str(record['production_aud'] or 0))
        opening = Decimal(str(record['opening_inventory_aud'] or 0))
        closing = Decimal(str(record['closing_inventory_aud'] or 0))
        cogs = Decimal(str(record['cogs_aud'] or 0))
        revenue = Decimal(str(record['revenue_aud'] or 0))
        
        total_july_projection += prod_aud
        projection_details.append({
            'month': month,
            'production_aud': prod_aud,
            'opening_inventory_aud': opening,
            'closing_inventory_aud': closing,
            'cogs_aud': cogs,
            'revenue_aud': revenue
        })
    
    print(f"   Total July 2025 records: {len(july_projection)}")
    print(f"   Total July 2025 production_aud: ${total_july_projection:,.2f}")
    
    if projection_details:
        print("\n   Projection details:")
        for detail in projection_details:
            print(f"     Month: {detail['month']}")
            print(f"       Production AUD: ${detail['production_aud']:,.2f}")
            print(f"       Opening Inventory: ${detail['opening_inventory_aud']:,.2f}")
            print(f"       Closing Inventory: ${detail['closing_inventory_aud']:,.2f}")
            print(f"       COGS AUD: ${detail['cogs_aud']:,.2f}")
            print(f"       Revenue AUD: ${detail['revenue_aud']:,.2f}")
    
    print("\n" + "="*70 + "\n")
    
    # 3. Compare the values
    print("3. COMPARISON ANALYSIS:")
    print(f"   CalculatedProduction July 2025: ${total_july_production:,.2f}")
    print(f"   InventoryProjection July 2025:  ${total_july_projection:,.2f}")
    difference = abs(total_july_production - total_july_projection)
    print(f"   Difference: ${difference:,.2f}")
    
    if difference < Decimal('0.01'):
        print("   ✅ VALUES MATCH PERFECTLY!")
    elif difference < Decimal('100'):
        print("   ✅ VALUES MATCH (minor rounding difference)")
    else:
        print("   ❌ SIGNIFICANT DIFFERENCE FOUND!")
    
    print("\n" + "="*70 + "\n")
    
    # 4. Additional analysis - check if there are other months with Crawler Systems data
    print("4. OTHER MONTHS ANALYSIS:")
    
    # All Crawler Systems months in CalculatedProductionModel
    all_production_months = CalculatedProductionModel.objects.filter(
        version__version="Jul 25 SPR",
        parent_product_group="Crawler Systems"
    ).values('pouring_date__year', 'pouring_date__month').distinct().order_by('pouring_date__year', 'pouring_date__month')
    
    print(f"   Months with Crawler Systems production data: {len(all_production_months)}")
    for month_data in all_production_months:
        year = month_data['pouring_date__year']
        month = month_data['pouring_date__month']
        print(f"     {year}-{month:02d}")
    
    # All Crawler Systems months in InventoryProjectionModel
    all_projection_months = InventoryProjectionModel.objects.filter(
        version__version="Jul 25 SPR",
        parent_product_group="Crawler Systems"
    ).values('month__year', 'month__month').distinct().order_by('month__year', 'month__month')
    
    print(f"\n   Months with Crawler Systems projection data: {len(all_projection_months)}")
    for month_data in all_projection_months:
        year = month_data['month__year']
        month = month_data['month__month']
        print(f"     {year}-{month:02d}")

if __name__ == "__main__":
    test_july_2025_crawler_systems()
