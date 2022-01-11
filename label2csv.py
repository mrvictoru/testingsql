# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import config
import cx_Oracle
import pandas as pd
from connection import connection


# %%
# connect to Oracle DB
conn = connection(config)
conn.connect()


# %%
feature = input("Feature: ")
sql = "select * from g3e_labelrule a join g3e_label b on a.g3e_lfno = b.g3e_lfno where g3e_rule like '%{feature}%'".format(feature = feature)
df = pd.read_sql(sql,con=conn.connecting)


# %%
filename = feature + "_lable" + ".csv"
csvdata = df.to_csv()
with open(filename,'w') as f:
    f.write(csvdata)


# %%
conn.close()


