#import config
#import cx_Oracle
from connection import connect
import pandas as pd
import csv

connection = connect()

table = input('Enter keyword for table: ')
sql = "Select G3E_USERNAME, G3E_NAME from G3E_FEATURECOMPONENT a join G3E_COMPONENT b on a.G3E_CNO = b.G3E_CNO where b.G3E_TABLE like '%{table}%'".format(table = table)
df=pd.read_sql(sql,con=connection)

filename = table + "_names.csv"
csvdata = df.to_csv()
with open(filename,'w') as f:
    f.write(csvdata)

if connection:
    connection.close()
    print("Connection closed")