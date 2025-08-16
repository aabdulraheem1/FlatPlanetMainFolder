"""
Management command to manually add the missing columns to scenarios table in SQL Server.
NO FALLBACKS - Direct database modification only.
"""

from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = 'Manually add last_calculated and calculation_status columns to scenarios table'

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            try:
                # Check if columns already exist
                cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'website_scenarios' 
                    AND COLUMN_NAME IN ('last_calculated', 'calculation_status')
                """)
                existing_columns = [row[0] for row in cursor.fetchall()]
                
                if 'last_calculated' not in existing_columns:
                    self.stdout.write("Adding last_calculated column...")
                    cursor.execute("""
                        ALTER TABLE website_scenarios 
                        ADD last_calculated datetime2 NULL
                    """)
                    self.stdout.write(self.style.SUCCESS("✅ Added last_calculated column"))
                else:
                    self.stdout.write("last_calculated column already exists")
                
                if 'calculation_status' not in existing_columns:
                    self.stdout.write("Adding calculation_status column...")
                    cursor.execute("""
                        ALTER TABLE website_scenarios 
                        ADD calculation_status nvarchar(50) NOT NULL DEFAULT 'never_calculated'
                    """)
                    self.stdout.write(self.style.SUCCESS("✅ Added calculation_status column"))
                else:
                    self.stdout.write("calculation_status column already exists")
                
                self.stdout.write(self.style.SUCCESS("🎯 Database schema updated successfully!"))
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Error updating database: {e}"))
