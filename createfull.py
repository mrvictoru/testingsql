import cx_Oracle
import os
import sys
import config
from connection import connection

if len(sys.argv) != 2:
    print("Usage: python createfull.py <filepath>")
    exit()

currentdir = os.getcwd()
filepath = os.path.join(currentdir, sys.argv[1])

conn = connection(config)
conn.connect()
if not conn.connecting:
    print("Connection failed")
    exit()

fullfiles = os.listdir(filepath)

# loop through the text files and create FULL views with SQL using text and filename as view name
for fullfile in fullfiles:
    if fullfile.endswith('SQL.txt'):
        # get the view name from the filename
        viewname = fullfile.replace('SQL.txt', '')
        # get the text from the file
        with open(os.path.join(filepath, fullfile), 'r') as f:
            text = f.read()
        # create the view
        try:
            conn.cursor.execute(text)
            print("Created view " + viewname)
        except cx_Oracle.DatabaseError as error:
            print(error)
            print("Failed to create view " + viewname)






