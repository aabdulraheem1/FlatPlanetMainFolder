from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = 'Add timestamp columns to ALL remaining models with version/scenario fields'

    def handle(self, *args, **options):
        # Additional models that were found but not in the original list
        additional_models = [
            'website_masterdatacommentmodel',
            'website_masterdataleadtimesmodel', 
            'website_masterdataproductattributesmodel',
            'website_masterdatasalesallocationtoplantmodel',
            'website_masterdatasalesmodel',
            'website_masterdataskutransfermodel',
            'website_revenuetocogsconversionmodel',
            'website_siteallocationmodel',
            'website_scenariooptimizationstate',
            'website_cachedcontroltowerdata',
            'website_cachedfoundrydata', 
            'website_cachedforecastdata',
            'website_cachedinventorydata',
            'website_cachedsupplierdata',
            'website_cacheddetailedinventorydata',
            'website_aggregatedfinancialchartdata',
            'website_inventoryprojectionmodel',
            'website_openinginventorysnapshot',
            'website_monthlypoureddatamodel',
            # Custom table name
            'MasterDataSafetyStocks',
        ]
        
        with connection.cursor() as cursor:
            for table_name in additional_models:
                try:
                    # Check if table exists
                    if table_name == 'MasterDataSafetyStocks':
                        # Custom table name
                        cursor.execute(f"""
                            SELECT COUNT(*) 
                            FROM INFORMATION_SCHEMA.TABLES 
                            WHERE TABLE_NAME = '{table_name}'
                        """)
                    else:
                        # Standard Django table names
                        cursor.execute(f"""
                            SELECT COUNT(*) 
                            FROM INFORMATION_SCHEMA.TABLES 
                            WHERE TABLE_NAME = '{table_name}'
                        """)
                    
                    table_exists = cursor.fetchone()[0] > 0
                    
                    if not table_exists:
                        self.stdout.write(self.style.WARNING(f'⚠️ Table {table_name} does not exist'))
                        continue
                    
                    # Add created_at column
                    cursor.execute(f"""
                        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'{table_name}') AND name = 'created_at')
                        BEGIN
                            ALTER TABLE {table_name} ADD created_at DATETIME2 DEFAULT GETDATE()
                            PRINT 'Added created_at to {table_name}'
                        END
                    """)
                    
                    # Add updated_at column  
                    cursor.execute(f"""
                        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'{table_name}') AND name = 'updated_at')
                        BEGIN
                            ALTER TABLE {table_name} ADD updated_at DATETIME2 DEFAULT GETDATE()
                            PRINT 'Added updated_at to {table_name}'
                        END
                    """)
                    
                    self.stdout.write(f'✅ Updated {table_name}')
                    
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f'⚠️ Could not update {table_name}: {e}'))
                    continue
        
        self.stdout.write(self.style.SUCCESS('🎯 Completed adding timestamps to ALL additional models!'))
