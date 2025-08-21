#!/usr/bin/env python
import os
import django
import sys

# Setup Django
sys.path.append('c:/Users/aali/OneDrive - bradken.com/Data/Training/SPR/spr')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'spr.settings')
django.setup()

from website.models import CalculatedProductionModel
import pandas as pd

# Get all production records for BK57592A
records = CalculatedProductionModel.objects.filter(
    product_id='BK57592A', 
    version__version='Aug 25 SPR'
).order_by('pouring_date', 'id')

print('=== ALL 11 PRODUCTION RECORDS FOR BK57592A ===')
print('ID\tSite\tPouringDate\t\tQty\tSource')
print('-' * 60)

for i, record in enumerate(records, 1):
    print(f'{record.id}\t{record.site_id}\t{record.pouring_date}\t{record.production_quantity}')
    
print(f'\nTotal Records: {records.count()}')
