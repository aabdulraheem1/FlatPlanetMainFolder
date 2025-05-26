from django.test import TestCase
from sqlalchemy import create_engine, text


# Connect to the database
Server = 'bknew-sql02'
Database = 'Bradken_Epicor_ODS'
Driver = 'ODBC Driver 17 for SQL Server'
Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
engine = create_engine(Database_Con)
connection = engine.connect()




# Fetch BOM data
query = text("SELECT TOP 3 * FROM epicor.PartMtl")
result = connection.execute(query)
rows = list(result)
print(rows)