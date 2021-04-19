# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from connection import connect
import pandas as pd
import csv


# %%
# connect to Oracle DB
connection = connect()


# %%
feature = input('Feature name: ')
attribute = input('Attribute name: ')
print("Running")

# %%
try:
    sql = "select G3E_CNO from g3e_component where g3e_name like '{feature}'".format(feature = feature)
    df=pd.read_sql(sql,con=connection)
except Exception as e:
    print("Feature not found")
    print(e)


# %%
try:
    sql = "select G3E_ANO from g3e_attribute where g3e_cno = {cno} and g3e_field like '{attribute}'".format(cno = df.G3E_CNO[0], attribute = attribute)
    df=pd.read_sql(sql,con=connection)
except Exception as e:
    print("Attribute not found")
    print(e)

# %%
try:
    sql = "select distinct(G3E_PNO) from g3e_tabattribute where g3e_ano = {ano}".format(ano = df.G3E_ANO[0])
    df=pd.read_sql(sql,con=connection)
except Exception as e:
    print("Error with ANO")
    print(e)

# %%
try:
    sql = "select G3E_TABLE from g3e_picklist where g3e_pno = {pno}".format(pno = df.G3E_PNO[0])
    df=pd.read_sql(sql,con=connection)
except Exception as e:
    print("Error with PNO")
    print(e)


# %%
try:
    sql = "select * from {table}".format(table = df.G3E_TABLE[0])
    df=pd.read_sql(sql,con=connection)
except Exception as e:
    print("Error with table")
    print(e)


# %%
filename = feature + "_" + attribute + ".csv"
csvdata = df.to_csv()
with open(filename,'w') as f:
    f.write(csvdata)
print("Printed csv")


# %%
connection.close()
print("Connection closed")


