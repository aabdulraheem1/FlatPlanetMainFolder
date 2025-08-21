from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('website', '0043_alter_masterdatamanuallyassignproductionrequirement_unique_together'),
    ]

    operations = [
        migrations.AddField(
            model_name='masterdataproductmodel',
            name='product_type',
            field=models.CharField(blank=True, choices=[('new', 'New'), ('repeat', 'Repeat')], help_text='New product (never invoiced) or Repeat (previously invoiced)', max_length=10, null=True),
        ),
    ]
