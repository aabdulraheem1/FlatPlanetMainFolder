import pandas as pd
from sqlalchemy import create_engine

Server = 'bkgcc-sql'
Database = 'Bradken_Data_Warehouse'
Driver = 'ODBC Driver 17 for SQL Server'
Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
engine = create_engine(Database_Con)
connection = engine.connect()

try:
    sql = """
        SELECT TOP 10 *
        FROM PowerBI.HeatProducts
        WHERE skProductId = 9735343
    """
    df = pd.read_sql(sql, connection)
    print(df)
finally:
    connection.close()