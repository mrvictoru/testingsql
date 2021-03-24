# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import config
import cx_Oracle
import pandas as pd


# %%
# connect to Oracle DB
connection = None
try:
    connection = cx_Oracle.connect(
        config.username,
        config.password,
        config.dsn,
        encoding = "UTF-8"
    )
    print("Connected")
    print(connection.version)
except cx_Oracle.Error as error:
    print(error)

cursor = connection.cursor()


# %%
feature = input("Feature: ")
sql = "select * from g3e_labelrule a join g3e_label b on a.g3e_lfno = b.g3e_lfno where g3e_rule like '%{feature}%'".format(feature = feature)
df = pd.read_sql(sql,con=connection)


# %%
df


# %%
filename = feature + "_lable" + ".csv"
csvdata = df.to_csv()
with open(filename,'w') as f:
    f.write(csvdata)


# %%



