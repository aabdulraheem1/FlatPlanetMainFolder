import sqlite3

conn = sqlite3.connect('db.sqlite3')
cursor = conn.cursor()

print('=== Sep 2026 Production Records for Aug 25 SP ===')

# Check total Sep 2026 records
cursor.execute("SELECT COUNT(*) FROM website_calculatedproductionmodel WHERE version_id = 'Aug 25 SP' AND pouring_date LIKE '2026-09%'")
total = cursor.fetchone()[0]
print(f'Total Sep 2026 records: {total}')

# Check records for product 2037-203-01B
cursor.execute("SELECT COUNT(*) FROM website_calculatedproductionmodel WHERE version_id = 'Aug 25 SP' AND pouring_date LIKE '2026-09%' AND product_id = '2037-203-01B'")
product_count = cursor.fetchone()[0]
print(f'Sep 2026 records for product 2037-203-01B: {product_count}')

if product_count > 0:
    cursor.execute("SELECT pouring_date, site_id, production_quantity, tonnes FROM website_calculatedproductionmodel WHERE version_id = 'Aug 25 SP' AND pouring_date LIKE '2026-09%' AND product_id = '2037-203-01B'")
    records = cursor.fetchall()
    print('Records for 2037-203-01B in Sep 2026:')
    for record in records:
        print(f'  Date: {record[0]}, Site: {record[1]}, Qty: {record[2]}, Tonnes: {record[3]}')

conn.close()
print('=== Test Complete ===')
