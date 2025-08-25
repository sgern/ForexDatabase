import pandas as pd
import requests
import os
import sqlite3

url = 'https://www.federalreserve.gov/datadownload/Output.aspx?rel=H10&series=60f32914ab61dfab590e0e470153e3ae&lastobs=10&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package'
headers = {'User-Agent': 'Mozilla/5.0'}

r = requests.get(url, headers = headers)

csv_file = 'FRB_H10.csv'

f = open(csv_file, 'wb')
f.write(r.content)
f.close()

r.close()

f = open(csv_file)
df = pd.read_csv(f)
f.close()

os.remove(csv_file)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

df.drop([0, 1, 3, 4], inplace=True)
df.reset_index(drop=True, inplace=True)

con = sqlite3.connect('forex.db')
cur = con.cursor()
cur.execute('CREATE TABLE IF NOT EXISTS currency(id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE)')
cur.execute('CREATE TABLE IF NOT EXISTS entry(date TEXT, currency_id TEXT, rate REAL NOT NULL, PRIMARY KEY (date, currency_id), FOREIGN KEY (currency_id) REFERENCES currency (id))')

for column_index in range(1, df.shape[1]):
    column = df.iloc[:, column_index]
    currency_data = (column[0], column.name)
    
    cur.execute('INSERT OR REPLACE INTO currency VALUES(?, ?)', currency_data)
    con.commit()
    
    for row_index in range(1, len(df)):
        entry_data = (df.iloc[row_index, 0], column[0], column[row_index])
        
        cur.execute('INSERT OR REPLACE INTO entry VALUES(?, ?, ?)', entry_data)
        con.commit()