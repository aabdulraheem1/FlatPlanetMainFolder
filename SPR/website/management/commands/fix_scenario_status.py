from django.core.management.base import BaseCommand
from website.models import scenarios

class Command(BaseCommand):
    help = 'Fix scenario status for Aug 25 SP'

    def handle(self, *args, **options):
        try:
            scenario = scenarios.objects.get(version='Aug 25 SP')
            self.stdout.write(f'Before: status={scenario.calculation_status}, last_calculated={scenario.last_calculated}')
            scenario.calculation_status = 'up_to_date'
            scenario.save()
            self.stdout.write(f'After: status={scenario.calculation_status}, last_calculated={scenario.last_calculated}')
            self.stdout.write(self.style.SUCCESS('✅ Updated Aug 25 SP scenario status to up_to_date'))
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR('❌ Aug 25 SP scenario not found'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error: {e}'))
