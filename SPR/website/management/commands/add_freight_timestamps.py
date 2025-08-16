from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = 'Add timestamp columns to MasterDataFreightModel and other critical models'

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            try:
                # Add timestamp columns to MasterDataFreightModel
                cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'website_masterdatafreightmodel') AND name = 'created_at')
                    BEGIN
                        ALTER TABLE website_masterdatafreightmodel ADD created_at DATETIME2 DEFAULT GETDATE()
                        PRINT 'Added created_at column to website_masterdatafreightmodel'
                    END
                """)
                
                cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'website_masterdatafreightmodel') AND name = 'updated_at')
                    BEGIN
                        ALTER TABLE website_masterdatafreightmodel ADD updated_at DATETIME2 DEFAULT GETDATE()
                        PRINT 'Added updated_at column to website_masterdatafreightmodel'
                    END
                """)
                
                self.stdout.write(self.style.SUCCESS('✅ Successfully added timestamp columns to MasterDataFreightModel'))
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Error adding timestamp columns: {e}'))
