from website.models import *
import time

version = 'Jul 25 SPR'
try:
    scenario = scenarios.objects.get(version=version)
    
    print('=== DATA VOLUME ANALYSIS FOR Jul 25 SPR ===')
    print(f'SMART_Forecast_Model records: {SMART_Forecast_Model.objects.filter(version=scenario).count():,}')
    print(f'MasterDataInventory records: {MasterDataInventory.objects.filter(version=scenario).count():,}')
    print(f'MasterDataProductModel records: {MasterDataProductModel.objects.count():,}')
    print(f'MasterDataPlantModel records: {MasterDataPlantModel.objects.count():,}')
    print(f'CalcualtedReplenishmentModel records: {CalcualtedReplenishmentModel.objects.filter(version=scenario).count():,}')
    print(f'CalculatedProductionModel records: {CalculatedProductionModel.objects.filter(version=scenario).count():,}')
    print(f'AggregatedForecast records: {AggregatedForecast.objects.filter(version=scenario).count():,}')
except Exception as e:
    print(f'Error: {e}')
