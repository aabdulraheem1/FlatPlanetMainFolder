from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = 'Add timestamp columns to MasterDataSafetyStocks table'

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            try:
                # Check if the table exists first
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'MasterDataSafetyStocks'
                """)
                table_exists = cursor.fetchone()[0] > 0
                
                if not table_exists:
                    self.stdout.write(self.style.WARNING('⚠️ MasterDataSafetyStocks table does not exist yet'))
                    self.stdout.write('💡 Create it first by running: python manage.py makemigrations && python manage.py migrate')
                    return
                
                # Add created_at column
                cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'MasterDataSafetyStocks') AND name = 'created_at')
                    BEGIN
                        ALTER TABLE MasterDataSafetyStocks ADD created_at DATETIME2 DEFAULT GETDATE()
                        PRINT 'Added created_at to MasterDataSafetyStocks'
                    END
                """)
                
                # Add updated_at column  
                cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'MasterDataSafetyStocks') AND name = 'updated_at')
                    BEGIN
                        ALTER TABLE MasterDataSafetyStocks ADD updated_at DATETIME2 DEFAULT GETDATE()
                        PRINT 'Added updated_at to MasterDataSafetyStocks'
                    END
                """)
                
                self.stdout.write(self.style.SUCCESS('✅ Successfully updated MasterDataSafetyStocks table'))
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Error updating MasterDataSafetyStocks: {e}'))
