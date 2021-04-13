from connection import connect
import config
#import cx_Oracle
import re
import pandas as pd
import csv

def get_display_condtion(view_name,logichard = False, stroke = True):

    # get the feature name for the desired display condition
    feature_name = view_name[2:]
    print(feature_name)
    sql = "Select g3e_geometrytype from G3E_COMPONENT where G3E_NAME = '{name}'".format(name = feature_name)
    cursor.execute(sql)
    try:
        geometrytype = cursor.fetchall()[0][0]
        print(geometrytype)
    except Exception as e:
        print(e)
        return


    # hardcode condition to know which styletable for the given style view
    if geometrytype == "PolygonGeometry":
        styletable = 'G3E_AREASTYLE'
    elif geometrytype == "PolylineGeometry":
        styletable = 'G3E_COMPOSITELINESTYLE'
    elif geometrytype == "OrientedPointGeometry":
        styletable = 'G3E_POINTSTYLE'
    elif geometrytype == "TextPointGeometry":
        styletable = 'G3E_TEXTSTYLE'
    elif geometrytype == "CompositePolylineGeometry":
        styletable = 'G3E_COMPOSITELINESTYLE'

    print(styletable)


    # extract text that create the given style view
    pattern = re.compile(r'\s+')
    sql = "SELECT TEXT FROM USER_VIEWS WHERE VIEW_NAME = '{view_name}'".format(view_name = view_name)
    cursor.execute(sql)
    fetch = cursor.fetchall()
    txts = fetch[0][0]
    txts = re.sub(pattern,'',txts)
    txts = txts.rpartition("SELECTCASE")[2]
    txts = txts.split("END", 1)[0]
    default = txts.rpartition("ELSE")[2]
    txts = txts.rpartition("ELSE")[0]
    txts = txts.split("WHEN")


    # find in use style id for given table
    dfid = pd.DataFrame()
    try:
        sql = "SELECT DISTINCT (g3e_styleid) FROM {view}".format(view = view_name)
        dfid = pd.read_sql(sql,con=connection)
    except Exception as e:
        print(e)


    # extract cause(logic condition) and effect(styleid) into dict
    logics = {}
    logics.update({default:{}})

    spl_chr = "THEN"
    spl_and = "AND"
    spl_or = "OR"
    spl_eq = "="
    spl_neq = "<>"
    spl_geq = ">="
    spl_seq = "<="
    spl_g = ">"
    spl_s = "<"
    spl_in = "IN("
    spl_notin = "NOTIN"
    for txt in txts:
        if txt != "":
            styleid = txt.rpartition(spl_chr)[2]
            content = txt.rpartition(spl_chr)[0]
            logic = {}
            # whether to break down logic or not, if easy then yes, else no
            if not logichard:
                if spl_and in content:
                    cond_list = content.split(spl_and)
                    for cond in cond_list:
                        if spl_eq in cond:
                            logic.update({cond.rpartition(spl_eq)[0]:"= " + cond.rpartition(spl_eq)[2]})
                        elif spl_neq in cond:
                            logic.update({cond.rpartition(spl_neq)[0]:"<> " + cond.rpartition(spl_neq)[2]})
                        elif spl_geq in cond:
                            logic.update({cond.rpartition(spl_geq)[0]:">= " + cond.rpartition(spl_geq)[2]})
                        elif spl_seq in cond:
                            logic.update({cond.rpartition(spl_seq)[0]:"<= " + cond.rpartition(spl_seq)[2]})
                        elif spl_g in cond:
                            logic.update({cond.rpartition(spl_g)[0]:"> " + cond.rpartition(spl_g)[2]})
                        elif spl_s in cond:
                            logic.update({cond.rpartition(spl_s)[0]:"< " + cond.rpartition(spl_s)[2]})
                        elif spl_notin in cond:
                            logic.update({cond.rpartition(spl_notin)[0]:"NOT " + cond.rpartition(spl_notin)[2]})
                        elif spl_in in cond:
                            logic.update({cond.rpartition(spl_in)[0]:"= " + cond.rpartition(spl_in)[2]})
                        
                else:
                    if spl_eq in content:
                        logic.update({content.rpartition(spl_eq)[0]:"= " + content.rpartition(spl_eq)[2]})
                    elif spl_neq in content:
                        logic.update({content.rpartition(spl_neq)[0]:"<> " + content.rpartition(spl_neq)[2]})
                    elif spl_geq in content:
                        logic.update({content.rpartition(spl_geq)[0]:">= " + content.rpartition(spl_geq)[2]})
                    elif spl_seq in content:
                        logic.update({content.rpartition(spl_seq)[0]:"<= " + content.rpartition(spl_seq)[2]})
                    elif spl_g in content:
                        logic.update({content.rpartition(spl_g)[0]:"> " + content.rpartition(spl_g)[2]})
                    elif spl_s in content:
                        logic.update({content.rpartition(spl_s)[0]:"< " + content.rpartition(spl_s)[2]})
                    elif spl_notin in content:
                        logic.update({content.rpartition(spl_notin)[0]:"NOT " + content.rpartition(spl_notin)[2]})
                    elif spl_in in content:
                        logic.update({content.rpartition(spl_in)[0]:"= " + content.rpartition(spl_in)[2]})
            else:
                logic.update({"logic":content})

            index = len(styleid)
            # remove comment that often tag behind style id
            for i in styleid:
                if not i.isdigit():
                    index = styleid.index(i)
                    break
            styleid = styleid[:index]
            # update logic with corresponding styleid
            logics.update({styleid : logic})

    for row in dfid.iterrows():
        logics[str(int(row[1].values))].update({"In Used" : "YES"})
    #print(logics)


   # get the relevant information for the given style id

    styles = {}
    # check if compositeline, as there is a seperate method of getting style info
    if styletable != 'G3E_COMPOSITELINESTYLE':
        for styleid in logics.keys():
            sqlstyle = "SELECT * FROM {table} WHERE g3e_sno = '{styleid}'".format(table = styletable, styleid = styleid)
            df=pd.read_sql(sqlstyle,con=connection)
            df = df.drop(['G3E_SNO','G3E_USERNAME','G3E_EDITDATE'], axis = 1)
            # get white print color
            try:
                df = df.drop(['G3E_COLOR'], axis = 1)
                sqlcolor = "SELECT b.G3E_COLOR, mod(b.g3e_color,256) r, mod(trunc(b.g3e_color/256),256) g, trunc(b.g3e_color/256/256) b  FROM G3E_STYLEMAPPING a, {styletable} b WHERE G3E_LEGENDSNO = {styleid} and g3e_stno = 301 and a.g3e_sno = b.g3e_sno".format(styletable = styletable, styleid = styleid)
                dfcolor = pd.read_sql(sqlcolor,con=connection)
                df = pd.concat([dfcolor,df], axis = 1)
            except Exception as e:
                print(e)
            df = df.dropna(how='all',axis=1)
            data = df.to_dict(orient = 'list')
            styles.update({styleid:data})

            
    else:
        for styleid in logics.keys():
            # get 1st line style
            sqlstyle = "SELECT b.* FROM G3E_COMPOSITELINESTYLE a JOIN g3e_linestyle b on a.g3e_line1 = b.g3e_sno WHERE a.g3e_sno = '{styleid}'".format(styleid = styleid)
            df1=pd.read_sql(sqlstyle,con=connection)
            
            df1 = df1.drop(df1.columns[[1,3,4,5,6,9,10,11]], axis = 1)
            # get stroke pattern
            if not df1.empty and stroke:
                sqlstyle = "SELECT * FROM G3E_NORMALIZEDSTROKE WHERE G3E_SPNO = {stroke}".format(stroke = df1.G3E_STROKEPATTERN[0])
                df1s = pd.read_sql(sqlstyle,con=connection)
                df1s = df1s.drop(['G3E_SPNO','G3E_USERNAME','G3E_EDITDATE','G3E_DASHPATTERNADJUSTMENT','G3E_MICROSTATIONSTYLENAME','G3E_UDLS'], axis = 1)
                df1 = pd.concat([df1,df1s], axis = 1)
            # get white print color
            try:
                df1 = df1.drop(['G3E_COLOR'], axis = 1)
                sqlcolor = "SELECT b.G3E_COLOR, mod(b.g3e_color,256) r, mod(trunc(b.g3e_color/256),256) g, trunc(b.g3e_color/256/256) b  FROM G3E_STYLEMAPPING a, {styletable} b WHERE G3E_LEGENDSNO = {styleid} and g3e_stno = 301 and a.g3e_sno = b.g3e_sno".format(styletable = 'G3E_LINESTYLE', styleid = df1.G3E_SNO[0])
                dfcolor = pd.read_sql(sqlcolor,con=connection)
                df1 = pd.concat([dfcolor,df1], axis = 1)
                df1 = df1.drop(['G3E_SNO'], axis = 1)
            except Exception as e:
                print(e)
            

            # get 2nd line style
            sqlstyle = "SELECT c.* FROM G3E_COMPOSITELINESTYLE a JOIN g3e_linestyle c on a.g3e_line2 = c.g3e_sno WHERE a.g3e_sno = '{styleid}'".format(styleid = styleid)
            df2=pd.read_sql(sqlstyle,con=connection)
            
            df2 = df2.drop(df2.columns[[1,3,4,5,6,9,10,11]], axis = 1)
            # get stroke pattern
            if not df2.empty and stroke:
                sqlstyle = "SELECT * FROM G3E_NORMALIZEDSTROKE WHERE G3E_SPNO = {stroke}".format(stroke = df2.G3E_STROKEPATTERN[0])
                df2s = pd.read_sql(sqlstyle,con=connection)
                df2s = df2s.drop(['G3E_SPNO','G3E_USERNAME','G3E_EDITDATE','G3E_DASHPATTERNADJUSTMENT','G3E_MICROSTATIONSTYLENAME','G3E_UDLS'], axis = 1)
                df2 = pd.concat([df2,df2s], axis = 1)
            # get white print color
            try:
                df2 = df2.drop(['G3E_COLOR'], axis = 1)
                sqlcolor = "SELECT b.G3E_COLOR, mod(b.g3e_color,256) r, mod(trunc(b.g3e_color/256),256) g, trunc(b.g3e_color/256/256) b  FROM G3E_STYLEMAPPING a, {styletable} b WHERE G3E_LEGENDSNO = {styleid} and g3e_stno = 301 and a.g3e_sno = b.g3e_sno".format(styletable = 'G3E_LINESTYLE', styleid = df2.G3E_SNO[0])
                dfcolor = pd.read_sql(sqlcolor,con=connection)
                df2 = pd.concat([dfcolor,df2], axis = 1)
                df2 = df2.drop(['G3E_SNO'], axis = 1)
            except Exception as e:
                print(e)

            # combine table
            df = pd.concat([df1,df2], axis = 1)
            # rename column that has the same name
            cols = pd.Series(df.columns)            
            for dup in df.columns[df.columns.duplicated(keep=False)]:
                cols[df.columns.get_loc(dup)] = ([dup + '_' + str(d_idx) if d_idx != 0 else dup for d_idx in range(df.columns.get_loc(dup).sum())])
            df.columns = cols

            # if both pattern is empty then get style from single line style table
            if df.empty:
                sqlstyle = "SELECT * FROM G3E_LINESTYLE WHERE g3e_sno = '{styleid}'".format(table = styletable, styleid = styleid)
                df = pd.read_sql(sqlstyle,con=connection)
                # get stroke pattern
                if df.G3E_STROKEPATTERN[0] is not None and stroke:
                    sqlstyle = "SELECT * FROM G3E_NORMALIZEDSTROKE WHERE G3E_SPNO = {stroke}".format(stroke = df.G3E_STROKEPATTERN[0])
                    dfs = pd.read_sql(sqlstyle,con=connection)
                    dfs = dfs.drop(['G3E_SPNO','G3E_USERNAME','G3E_EDITDATE','G3E_DASHPATTERNADJUSTMENT','G3E_MICROSTATIONSTYLENAME','G3E_UDLS'], axis = 1)
                    df = pd.concat([df,dfs], axis = 1)
                df = df.drop(['G3E_USERNAME','G3E_EDITDATE'], axis = 1)
                # get white print color
                try:
                    df = df.drop(['G3E_COLOR'], axis = 1)
                    sqlcolor = "SELECT b.G3E_COLOR, mod(b.g3e_color,256) r, mod(trunc(b.g3e_color/256),256) g, trunc(b.g3e_color/256/256) b  FROM G3E_STYLEMAPPING a, {styletable} b WHERE G3E_LEGENDSNO = {styleid} and g3e_stno = 301 and a.g3e_sno = b.g3e_sno".format(styletable = 'G3E_LINESTYLE', styleid = df.G3E_SNO[0])
                    dfcolor = pd.read_sql(sqlcolor,con=connection)
                    df = pd.concat([dfcolor,df], axis = 1)
                    df = df.drop(['G3E_SNO'], axis = 1)
                except Exception as e:
                    print(e)
            
            data = df.to_dict(orient = 'list')
            styles.update({styleid:data})


    # convert back to dataframe for printing to csv
    df1 = pd.DataFrame.from_dict({(i): styles[i]
                                    for i in styles.keys()},
                                    orient = 'index')
    # drop empty column in df1
    for column in df1:
        isNone = True
        for item in df1[column]:
            if not (item == [None] or type(item) == float):
                isNone = False
                break
        if isNone:
            df1 = df1.drop([column], axis = 1)


    df2 = pd.DataFrame.from_dict({(i): logics[i]
                                    for i in logics.keys()},
                                    orient = 'index')

    df = pd.concat([df1,df2], axis = 1)


    filename = view_name + ".csv"
    csvdata = df.to_csv()
    with open(filename,'w') as f:
        f.write(csvdata)


# connect to Oracle DB
connection = connect()

cursor = connection.cursor()

sql = "SELECT VIEW_NAME from sys.all_views WHERE owner = '{owner}'".format(owner = config.username)
df = pd.read_sql(sql,con=connection)


key = input('Enter keyword for view: ')
hard = int(input('is the logic hard(1 = True/ 0 = False): '))
if hard == 0:
    hardkey = False
elif hard == 1:
    hardkey = True
else:
    print("logic input error")
    exit()
strokekey = int(input('want to include stroke?(1 = True/ 0 = False): '))
if strokekey == 0:
    stroke = False
elif strokekey == 1:
    stroke = True
else:
    print("stroke input error")
    exit()

count = 0
views_name = []
for row in df.VIEW_NAME:
    if row[:2] == "V_" and key in row:
        views_name.append(row)
        count += 1
print("No. of view match:" + str(count))


for view in views_name:
    get_display_condtion(view,hardkey,stroke)


# close connection
if connection:
    cursor.close()
    connection.close()
    print("Connection closed")


