# %%
import cx_Oracle
import config
import config_2
import os
import pandas as pd
from connection import connection

# %%
# make connection
conn = connection(config)
conn.connect()

# %%
# query VIEW_NAMEs with FULL at the end
sql = "SELECT VIEW_NAME from sys.all_views WHERE owner = '{owner}' and VIEW_NAME like '{key}'".format(owner = config.username, key = '%FULL')
vnames = pd.read_sql(sql,con=conn.connecting)

# %%
# query all views's text and store in dict
views_dict = {}
for _,row in vnames.iterrows():
    sql = "SELECT TEXT FROM USER_VIEWS WHERE VIEW_NAME = '{name}'".format(name = row['VIEW_NAME'])
    conn.cursor.execute(sql)
    text = conn.cursor.fetchall()
    views_dict[row['VIEW_NAME']] = text
    print("{name} is collected".format(name = row['VIEW_NAME']))



# %%
# close connection
conn.close()

# %%
# make new connection
conn2 = connection(config_2)
conn2.connect()

# %%
# create FULL views or replace existing FULL views using text
for name in views_dict:
    sql = "CREATE OR REPLACE VIEW {name} AS {text}".format(name = name, text = views_dict[name][0][0])
    conn2.cursor.execute(sql)
    print("{name} is created".format(name = name))

# %%
conn2.close()


