import sqlite3

conn = sqlite3.connect('db.sqlite3')
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%production%'")
tables = cursor.fetchall()
print('Production-related tables:')
for table in tables:
    print(f'  {table[0]}')

cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%calculated%'")
tables = cursor.fetchall()
print('Calculated-related tables:')
for table in tables:
    print(f'  {table[0]}')

conn.close()
