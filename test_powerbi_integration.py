#!/usr/bin/env python
import os
import sys
import django

# Add the current directory to Python path
sys.path.append('.')

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

# Test the updated PowerBI query
from sqlalchemy import create_engine, text
from datetime import date

def test_powerbi_production_query():
    """Test the PowerBI production query for a specific site and month"""
    print("Testing PowerBI production query...")
    
    # Database connection
    Server = 'bknew-sql02'
    Database = 'Bradken_Data_Warehouse'
    Driver = 'ODBC Driver 17 for SQL Server'
    Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
    engine = create_engine(Database_Con)
    
    # Test COI2 for April 2025
    site = 'COI2'
    month_start = date(2025, 4, 1)
    month_end = date(2025, 4, 30)
    
    try:
        with engine.connect() as connection:
            query = text("""
                SELECT 
                    SUM(hp.CastQty * p.DressMass / 1000) as TotalTonnes,
                    COUNT(*) as RecordCount,
                    MIN(hp.TapTime) as MinDate,
                    MAX(hp.TapTime) as MaxDate
                FROM PowerBI.HeatProducts hp
                INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                WHERE hp.TapTime IS NOT NULL 
                    AND p.DressMass IS NOT NULL 
                    AND s.SiteName = :site_name
                    AND hp.TapTime >= :start_date
                    AND hp.TapTime <= :end_date
            """)
            
            result = connection.execute(query, {
                'site_name': site,
                'start_date': month_start,
                'end_date': month_end
            })
            
            row = result.fetchone()
            if row:
                print(f"Site: {site}")
                print(f"Month: {month_start.strftime('%b %Y')}")
                print(f"Records found: {row.RecordCount}")
                print(f"Total tonnage: {row.TotalTonnes}")
                print(f"Date range: {row.MinDate} to {row.MaxDate}")
                
                # Compare with what we were getting before
                print(f"\nThis should be much higher than the ~52 pieces we were getting before!")
                print(f"The calculation is: SUM(CastQty * DressMass / 1000)")
            else:
                print("No data found")
                
    except Exception as e:
        print(f"Error: {e}")

def test_pour_plan_calculation():
    """Test the updated pour plan calculation"""
    print("\n" + "="*50)
    print("Testing updated Pour Plan calculation...")
    
    # Import the updated function
    from website.customized_function import get_monthly_pour_plan_details_for_site_and_fy
    
    try:
        result = get_monthly_pour_plan_details_for_site_and_fy('COI2', 'FY25', 'scenario_1')
        print(f"Total actual (PowerBI): {result['total_actual']}")
        print(f"Total plan: {result['total_plan']}")
        print(f"Grand total: {result['grand_total']}")
        
        # Show some monthly details
        print("\nMonthly breakdown:")
        for detail in result['monthly_details'][:6]:  # First 6 months
            print(f"  {detail['month']}: {detail['value']} ({detail['type']})")
            
    except Exception as e:
        print(f"Error testing pour plan: {e}")

if __name__ == '__main__':
    test_powerbi_production_query()
    test_pour_plan_calculation()
