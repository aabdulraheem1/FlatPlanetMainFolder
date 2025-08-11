from django.core.management.base import BaseCommand
from sqlalchemy import create_engine, text
from website.models import MasterDataSafetyStocks, scenarios

class Command(BaseCommand):
    help = 'Fetch safety stocks data from Epicor database and store it in MasterDataSafetyStocks'

    def add_arguments(self, parser):
        parser.add_argument('version', type=str, help='The scenario version')

    def handle(self, *args, **kwargs):
        version = kwargs['version']
        scenario = scenarios.objects.get(version=version)
        
        # Database connection details
        Server = 'bknew-sql02'
        Database = 'Bradken_Epicor_ODS'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        engine = create_engine(Database_Con)

        try:
            # Use context manager for connection
            with engine.connect() as connection:
                # SQL query to fetch safety stocks data
                query = text("""
                    SELECT 
                        Plant,
                        PartNum,
                        MinimumQty,
                        SafetyQty
                    FROM epicor.PartPlant
                    WHERE RowEndDate IS NULL
                """)

                # Execute the query
                self.stdout.write("Fetching safety stocks data from the database...")
                result = connection.execute(query)

                # Delete existing records for this version
                MasterDataSafetyStocks.objects.filter(version=scenario).delete()

                # Store the data in the model
                records_created = 0
                for row in result:
                    MasterDataSafetyStocks.objects.create(
                        version=scenario,
                        Plant=row.Plant,
                        PartNum=row.PartNum,
                        MinimumQty=row.MinimumQty,
                        SafetyQty=row.SafetyQty
                    )
                    records_created += 1

                self.stdout.write(self.style.SUCCESS(f"Successfully fetched and stored {records_created} safety stocks records."))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"An error occurred: {e}"))