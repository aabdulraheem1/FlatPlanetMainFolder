import os
import pandas as pd

folder = r"X:\SPR\Inventory Model Master Data"
all_products = set()

for file in os.listdir(folder):
    if file.endswith('.xlsx') or file.endswith('.xls'):
        path = os.path.join(folder, file)
        try:
            df = pd.read_excel(path)
            if 'Product' in df.columns:
                products = df['Product'].dropna().astype(str).unique()
                all_products.update(products)
        except Exception as e:
            print(f"Error reading {file}: {e}")

# Convert to sorted list and print or save
product_list = sorted(all_products)
print(product_list)  # Print first 10 products for brevity



