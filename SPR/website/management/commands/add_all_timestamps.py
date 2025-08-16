from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = 'Add timestamp columns to ALL models with version/scenario fields'

    def handle(self, *args, **options):
        # List of all models with version/scenario fields that need timestamps
        models_to_update = [
            'website_smart_forecast_model',
            'website_revenue_forecast_model', 
            'website_masterdataorderbook',
            'website_masterdatacapacitymodel',
            'website_masterdatahistoryofproductionmodel',
            'website_masterdataincottermtypesmodel',
            'website_masterdataincotermsmodel',
            'website_masterdataplan',
            'website_masterdataschedulemodel',
            'website_aggregatedforecast',
            'website_masterdatainventory',
            'website_masterdatacasttodespatchmodel',
            'website_calcualtedreplenishmentmodel',
            'website_calculatedproductionmodel',
            'website_masterdataepicorsuppliermasterdatamodel',
            'website_masterdatamanuallyassignproductionrequirement',
            'website_productsitecostmodel',
            'website_fixedplantconversionmodifiersmodel',
            'website_masterdatasafetystocks',
            # Add any other models found
        ]
        
        with connection.cursor() as cursor:
            for table_name in models_to_update:
                try:
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
        
        self.stdout.write(self.style.SUCCESS('🎯 Completed adding timestamps to all models with version/scenario fields!'))
