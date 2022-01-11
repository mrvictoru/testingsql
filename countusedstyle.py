import config
#import cx_Oracle
from connection import connection
import pandas as pd

conn = connection(config)
conn.connect()

polygon_count = 0
polyline_count = 0
point_count = 0
compline_count = 0
text_count = 0

sql = "SELECT VIEW_NAME from sys.all_views WHERE owner = '{owner}' and VIEW_NAME like 'V_%'".format(owner = config.username)
vname = pd.read_sql(sql,con=conn.connecting)

views_name = []
for row in vname.VIEW_NAME:
    if row[:2] == "V_":
        views_name.append(row)

for name in views_name:
    try:
        sql = "SELECT distinct(G3E_STYLEID) FROM {view}".format(view = name)
        styleids = pd.read_sql(sql,con=conn)
        for id in styleids.G3E_STYLEID:
            sql = "SELECT G3E_TYPE FROM G3E_STYLE WHERE G3E_SNO = {id}".format(id = id)
            conn.cursor.execute(sql)
            g3etype = conn.cursor.fetchall()

            if g3etype[0][0] == "PointStyle":
                point_count += 1
            elif g3etype[0][0] == "CompositeLineStyle":
                compline_count += 1
            elif g3etype[0][0] == "TextStyle":
                text_count += 1
            elif g3etype[0][0] == "AreaStyle":
                polygon_count += 1
            elif g3etype[0][0] == "LineStyle":
                polyline_count += 1
    except Exception as e:
        print(e)

print(polygon_count)
print(polyline_count)
print(point_count)
print(compline_count)
print(text_count)

conn.close()