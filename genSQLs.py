# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import cx_Oracle
import config
import os
from connection import connection as conn


# %%
# connect to Oracle DB
connection = conn(config)
connection.connect()


# %%
# get FNO list
FNOs = []

connection.cursor.execute("SELECT UNIQUE G3E_FNO FROM G3E_FEATURECOMPONENT")
FNO_list = connection.cursor.fetchall()

uniq_fno = len(FNO_list)


for temp in FNO_list:
    FNOs.append(temp[0])


# %%
# loop through FNOs and obtain a G3E table
features = []
for FNO in FNOs:
    connection.cursor.execute("SELECT G3E_TABLE,G3E_GEOMETRYTYPE FROM G3E_FEATURECOMPONENT a JOIN G3E_COMPONENT b on a.G3E_CNO = b.G3E_CNO WHERE a.G3E_FNO = :G3E_FNO", G3E_FNO = FNO)
    G3E_TABLE = connection.cursor.fetchall()
    tables = []
    for table in G3E_TABLE:
        if (table[1] is None) and ("SR_" not in table[0] and "_SR" not in table[0]):
            tables.append(table[0])
    feature = {
        "FNO": FNO,
        "Table": tables
    }
    features.append(feature)

step_count = 0
print_count = 0
errorlog = []
# %%
# loop through.py
for feature in features:
    step_count += 1

# %%
    # pick out the attribute for the G3E_Table
    g3es = []
    for table in feature["Table"]:
        sql = "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE table_name = '" + table + "'"
        connection.cursor.execute(sql)
        COLUMNS = connection.cursor.fetchall()
        g3e = {
            "G3E_TABLE": table,
            "ATTRIBUTE": []
        }
        for column in COLUMNS:
            g3e["ATTRIBUTE"].append(column[0])
        
        g3es.append(g3e)


    # %%
    # make alphabets for table
    alphabets = []
    for i in range(97, 97+len(g3es)):
        alphabets.append(chr(i))
    
    # %%
    # get main table

    connection.cursor.execute("SELECT G3E_NAME FROM G3E_FEATURE WHERE G3E_FNO = :G3E_FNO", G3E_FNO = feature["FNO"])
    majortable = connection.cursor.fetchall()[0][0]
    STRING_out = ""
    if majortable is not None:

        # write G3E table into SQL scirpt into text file
        filename = majortable + "_SQL.txt"
        with open(filename,"w") as file:
            file.write("CREATE VIEW " + majortable + "_FULL as\n")
            file.write("SELECT\n")
            
            i = 0
            # print main table's attribute first
            for g3e in g3es:
                if g3e["G3E_TABLE"] in majortable:
                    g3e["alphabets"] = alphabets[i]
                    for attribute in g3e["ATTRIBUTE"]:
                        file.write("    " + alphabets[i] + "." + attribute + " as " + g3e["G3E_TABLE"] + "_" + attribute + ",\n")
                    i += 1
                
            # print remaining tables' attribute
            for g3e in g3es:
                if g3e["G3E_TABLE"] not in majortable:
                    g3e["alphabets"] = alphabets[i]
                    for attribute in g3e["ATTRIBUTE"]:
                        # check if attribute already exist, if so add distint name
                        file.write("    " + alphabets[i] + "." + attribute + " as " + g3e["G3E_TABLE"] + "_" + attribute + ",\n")
                    i += 1
            
            # delete the last comma
            for i in range(3):
                file.seek(file.tell() - 1, os.SEEK_SET)
                file.truncate()

            # print the table that need to be join
            file.write("\nFROM\n")
            # main table
            for g3e in g3es:
                if g3e["alphabets"] is alphabets[0]:
                    file.write("    " + g3e["G3E_TABLE"] + " " + g3e["alphabets"] +"\n")
            # joining remaining table
            for g3e in g3es:
                if g3e["alphabets"] is not alphabets[0]:
                    file.write("    LEFT JOIN " + g3e["G3E_TABLE"] + " " + g3e["alphabets"] + " ON " + alphabets[0] + ".G3E_FID" + " = " + g3e["alphabets"] + ".G3E_FID\n")
            
        print("Created SQL scripted in " + filename)

        with open(filename,"r") as readfile:
            sql = readfile.read()
            table = filename.replace("_SQL.txt","")
            try:
                connection.cursor.execute(sql)
                print("Created view for " + table)
                print_count += 1

            except cx_Oracle.DatabaseError as exc:
                error, = exc.args
                if error.code == 955:
                    drop = "DROP VIEW " + table + "_FULL"
                    print(drop)
                    connection.cursor.execute(drop)
                    connection.cursor.execute(sql)
                    print("Created view for " + table)
                    print_count += 1
                else:
                    print("Fail to create view: " + table)
                    print(str(error.code) + " " + str(error.message))
                
print(errorlog)
print("Overall FNO count: " + str(uniq_fno))
print("Step count: " + str(step_count))
print("View created: " + str(print_count))
# %%
# close connection
connection.close()


# %%



