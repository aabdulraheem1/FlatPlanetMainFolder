# management/commands/calculate_forecast_by_parent_group.py

from django.core.management.base import BaseCommand
from . models import SMART_Forecast_Model, MasterDataProductModel, AggregatedForecastParentGroup
from django.db.models import Sum
from collections import defaultdict

class Command(BaseCommand):
    help = 'Calculate and store forecast by parent product group'

    def handle(self, *args, **kwargs):
        AggregatedForecastParentGroup.objects.all().delete()  # Clear existing records

        # Build a cache for product -> parent product group
        product_to_parent = dict(
            MasterDataProductModel.objects.values_list('Product', 'ParentProductGroup')
        )

        # Aggregate
        raw_data = defaultdict(lambda: defaultdict(float))
        for forecast in SMART_Forecast_Model.objects.all():
            if forecast.Product in product_to_parent and forecast.Period_AU:
                parent_group = product_to_parent[forecast.Product]
                raw_data[forecast.Period_AU][parent_group] += forecast.Tonnes or 0

        # Save results
        for period, groups in raw_data.items():
            for parent_group, tonnes in groups.items():
                AggregatedForecastParentGroup.objects.create(
                    period=period,
                    parent_product_group=parent_group,
                    tonnes=tonnes
                )

        self.stdout.write(self.style.SUCCESS("Aggregated data saved successfully."))
