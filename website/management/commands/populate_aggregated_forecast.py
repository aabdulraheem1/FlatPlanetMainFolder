from django.core.management.base import BaseCommand
from website.models import SMART_Forecast_Model, MasterDataProductModel, AggregatedForecast

class Command(BaseCommand):
    help = 'Populate the AggregatedForecast model with aggregated data from related models'

    def handle(self, *args, **kwargs):
        self.stdout.write('Starting to populate AggregatedForecast model...')

        # Clear existing data to avoid duplicates
        AggregatedForecast.objects.all().delete()

        # Prepare a list to hold AggregatedForecast instances
        aggregated_forecasts = []

        # Aggregate data and prepare instances for bulk creation
        forecasts = SMART_Forecast_Model.objects.all()
        for forecast in forecasts:
            try:
                product_data = MasterDataProductModel.objects.get(Product=forecast.Product)
                aggregated_forecast = AggregatedForecast(
                    version=forecast.version,
                    tonnes=forecast.Tonnes,
                    forecast_region=forecast.Forecast_Region,
                    customer_code=forecast.Customer_code,
                    period=forecast.Period_AU,
                    product=product_data,
                    product_group_description=product_data.ProductGroupDescription,
                    parent_product_group_description=product_data.ParentProductGroupDescription
                )
                aggregated_forecasts.append(aggregated_forecast)
            except MasterDataProductModel.DoesNotExist:
                self.stdout.write(self.style.ERROR(f'Product {forecast.Product} does not exist in MasterDataProductModel'))

        # Bulk create AggregatedForecast instances
        AggregatedForecast.objects.bulk_create(aggregated_forecasts)

        self.stdout.write(self.style.SUCCESS('Finished populating AggregatedForecast model.'))
