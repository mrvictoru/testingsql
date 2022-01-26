import os

files = os.listdir(os.getcwd())

for file in files:
    if file.endswith('SQL.txt'):
        # motify text file and replace "CREATE" with "CREATE OR REPLACE"
        with open(file, 'r') as f:
            text = f.read()
        text = text.replace('CREATE', 'CREATE OR REPLACE')
        with open(file, 'w') as f:
            f.write(text)
        print("Modified " + file)
    else:
        print("Skipped " + file)
