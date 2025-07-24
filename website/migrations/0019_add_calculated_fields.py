from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('website', '0018_aggregatedforecastchartdata_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='masterdataplan',
            name='CalendarDays',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='masterdataplan',
            name='AvailableDays',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='masterdataplan',
            name='PlanDressMass',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
