# from django.test import TestCase
# import pandas as pd
# import sqlalchemy as sa
# from sqlalchemy import create_engine
# from .models import Product_Model


# # Create your tests here.


# Server ='bknew-sql02'
# Database = 'Bradken_Data_Warehouse'
# Driver = 'ODBC Driver 17 for SQL Server'
# Database_Con = f'mssql://@{Server}/{Database}?driver={Driver}'

# engine = create_engine(Database_Con)
# con_Bradken_Data_Warehouse_Epicor = engine.connect()

# df_Epicor_PowerBI_Products =pd.read_sql_query("Select * from PowerBI.Products where RowEndDate IS NULL",con_Bradken_Data_Warehouse_Epicor)
# df_Epicor_PowerBI_Site =pd.read_sql_query("Select * from PowerBI.Site where RowEndDate IS NULL",con_Bradken_Data_Warehouse_Epicor)
# df_Epicor_PowerBI_HeatProducts =pd.read_sql_query("Select * from PowerBI.HeatProducts",con_Bradken_Data_Warehouse_Epicor)
# df_Epicor_PowerBI_SalesReps =pd.read_sql_query("Select * from PowerBI.SalesReps",con_Bradken_Data_Warehouse_Epicor)

# Product_Model.objects.bulk_create([Product_Model(Product=row['ProductKey'], Product_Group=row['ProductGroupDescription']) for _, row in df_Epicor_PowerBI_Products.iterrows()
# ])

def testfunction(self):
    x = self.get_named_formset
    return print(x)

testfunction(self='1')