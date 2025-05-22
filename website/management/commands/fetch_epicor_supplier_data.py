from django.core.management.base import BaseCommand
from sqlalchemy import create_engine, text
from website.models import MasterDataEpicorSupplierMasterDataModel

class Command(BaseCommand):
    help = 'Fetch data from Epicor database and store it in MasterDataEpicorSupplierMasterDataModel'

    def handle(self, *args, **kwargs):
        # Database connection details
        Server = 'bknew-sql02'
        Database = 'Bradken_Epicor_ODS'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        engine = create_engine(Database_Con)
        connection = engine.connect()

        try:
            # SQL query to join tables and fetch data
            query = text("""
                SELECT 
                    PartPlant.Company AS Company,
                    PartPlant.Plant AS Plant,
                    PartPlant.PartNum AS PartNum,
                    Vendor.VendorID AS VendorID
                FROM epicor.PartPlant AS PartPlant
                INNER JOIN epicor.Vendor AS Vendor
                    ON PartPlant.Company = Vendor.Company
                    AND PartPlant.VendorNum = Vendor.VendorNum
                WHERE PartPlant.RowEndDate IS NULL
            """)

            # Execute the query
            self.stdout.write("Fetching data from the database...")
            result = connection.execute(query)

            # Store the data in the model
            for row in result:
                MasterDataEpicorSupplierMasterDataModel.objects.update_or_create(
                    Company=row.Company,
                    Plant=row.Plant,
                    PartNum=row.PartNum,
                    defaults={'VendorID': row.VendorID}
                )

            self.stdout.write(self.style.SUCCESS("Data successfully fetched and stored in the database."))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"An error occurred: {e}"))
        finally:
            # Close the database connection
            connection.close()