from django.core.management.base import BaseCommand
from website.models import SMART_Forecast_Model, MasterDataProductModel, AggregatedForecast

class Command(BaseCommand):
    help = 'Populate the AggregatedForecast model with aggregated data from related models'

    def add_arguments(self, parser):
        parser.add_argument(
            'version',
            type=str,
            help="The version of the scenario to populate data for.",
        )

    def handle(self, *args, **kwargs):
        version = kwargs['version']

        AggregatedForecast.objects.filter(version=version).delete()

        # Prefetch only needed fields for products
        product_data_map = {
            product['Product']: product
            for product in MasterDataProductModel.objects.values(
                 'Product', 'DressMass', 'ProductGroupDescription', 'ParentProductGroupDescription'
            )
        }
        product_instance_map = {
            product.Product: product
            for product in MasterDataProductModel.objects.all()
        }

        aggregated_forecasts = []
        batch_size = 1000

        for forecast in SMART_Forecast_Model.objects.filter(version=version).iterator(chunk_size=batch_size):
            product_data = product_data_map.get(forecast.Product)
            product_instance = product_instance_map.get(forecast.Product)
            if not product_data or not product_instance:
                continue

            dress_mass = product_data['DressMass']
            if forecast.Qty is not None and dress_mass not in [None, 0]:
                tonnes = forecast.Qty * dress_mass / 1000
            elif forecast.Qty is not None and forecast.PriceAUD is not None:
                tonnes = (forecast.Qty * forecast.PriceAUD * 0.65) / 5000
            else:
                tonnes = 0

            aggregated_forecasts.append(AggregatedForecast(
                version=forecast.version,
                tonnes=tonnes,
                forecast_region=forecast.Forecast_Region,
                customer_code=forecast.Customer_code,
                period=forecast.Period_AU,
                product=product_instance,
                product_group_description=product_data['ProductGroupDescription'],
                parent_product_group_description=product_data['ParentProductGroupDescription']
            ))

            if len(aggregated_forecasts) >= batch_size:
                AggregatedForecast.objects.bulk_create(aggregated_forecasts, batch_size=batch_size)
                aggregated_forecasts = []

        if aggregated_forecasts:
            AggregatedForecast.objects.bulk_create(aggregated_forecasts, batch_size=batch_size)