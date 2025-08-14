from django.core.management.base import BaseCommand
from django.db import transaction
from sqlalchemy import create_engine, text
from website.models import MasterDataSafetyStocks, scenarios
import logging

class Command(BaseCommand):
    help = 'Fetch safety stocks data from Epicor database and store it in MasterDataSafetyStocks'

    def add_arguments(self, parser):
        parser.add_argument('version', type=str, help='The scenario version')

    def handle(self, *args, **kwargs):
        version = kwargs['version']
        
        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            self.stderr.write(self.style.ERROR(f"Scenario '{version}' does not exist."))
            return
        
        # Database connection details
        Server = 'bknew-sql02'
        Database = 'Bradken_Epicor_ODS'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        
        try:
            engine = create_engine(Database_Con)
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Failed to connect to database: {e}"))
            return

        try:
            # Use Django transaction to ensure atomicity
            with transaction.atomic():
                # Delete existing records for this version first
                deleted_count = MasterDataSafetyStocks.objects.filter(version=scenario).delete()
                self.stdout.write(f"Deleted {deleted_count[0]} existing records for scenario '{version}'")

                # Use context manager for SQLAlchemy connection
                with engine.connect() as connection:
                    # SQL query to fetch safety stocks data with DISTINCT to avoid duplicates
                    query = text("""
                        SELECT DISTINCT
                            Plant,
                            PartNum,
                            MinimumQty,
                            SafetyQty
                        FROM epicor.PartPlant
                        WHERE RowEndDate IS NULL
                        ORDER BY Plant, PartNum
                    """)

                    # Execute the query
                    self.stdout.write("Fetching safety stocks data from Epicor database...")
                    result = connection.execute(query)

                    # Collect records for bulk creation
                    records_to_create = []
                    records_processed = 0
                    seen_combinations = set()  # Track (Plant, PartNum) to avoid duplicates
                    duplicates_skipped = 0
                    
                    for row in result:
                        # Create a unique key for this combination
                        unique_key = (row.Plant, row.PartNum)
                        
                        # Skip if we've already seen this combination
                        if unique_key in seen_combinations:
                            duplicates_skipped += 1
                            continue
                            
                        seen_combinations.add(unique_key)
                        
                        records_to_create.append(
                            MasterDataSafetyStocks(
                                version=scenario,
                                Plant=row.Plant,
                                PartNum=row.PartNum,
                                MinimumQty=row.MinimumQty,
                                SafetyQty=row.SafetyQty
                            )
                        )
                        records_processed += 1
                        
                        # Bulk create in batches of 1000 for memory efficiency
                        if len(records_to_create) >= 1000:
                            MasterDataSafetyStocks.objects.bulk_create(records_to_create)
                            self.stdout.write(f"Created batch of {len(records_to_create)} records...")
                            records_to_create = []

                    # Create any remaining records
                    if records_to_create:
                        MasterDataSafetyStocks.objects.bulk_create(records_to_create)
                        self.stdout.write(f"Created final batch of {len(records_to_create)} records...")

                if duplicates_skipped > 0:
                    self.stdout.write(f"Skipped {duplicates_skipped} duplicate records from source data")

                self.stdout.write(self.style.SUCCESS(f"Successfully fetched and stored {records_processed} safety stocks records for scenario '{version}'."))
                
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"An error occurred: {e}"))
            # The transaction will be rolled back automatically due to the exception