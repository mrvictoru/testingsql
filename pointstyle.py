#!/usr/bin/env python
# coding: utf-8

# In[5]:
from connection import connection
import config
import re
import pandas as pd
import csv


# In[6]:


# connect to Oracle DB
conn = connection(config)
conn.connect()


# In[9]:


fcsv = input("csv file: ")
symid = pd.read_csv(fcsv)
df = pd.DataFrame()
for row in symid.G3E_STROKESYMBOL_0_1:
    sqlstyle = "SELECT * FROM {table} WHERE g3e_sno = '{styleid}'".format(table = "G3E_POINTSTYLE", styleid = int(row[1:len(row)-1]))
    ptstyle = pd.read_sql(sqlstyle,con=conn.connecting)
    ptstyle = ptstyle.drop(['G3E_EDITDATE'], axis = 1)
    df = df.append(ptstyle)


# In[18]:


filename = fcsv[:6] + "pointstyle" + ".csv"
csvdata = df.to_csv()
with open(filename,'w') as f:
    f.write(csvdata)
    


# In[21]:

# close connection
conn.close()






