from django.db import models

class AnaSummary(models.Model):
    equip_id = models.BigIntegerField(blank=True, null=True)
    run_start_date = models.DateTimeField(blank=True, null=True)
    result = models.CharField(max_length=100)
    comment = models.CharField(max_length=100, blank=True, null=True)
    measurement_date = models.DateTimeField(blank=True, null=True)
    block_no = models.BigIntegerField(blank=True, null=True)
    evaluation_place = models.CharField(max_length=100,blank=True, null=True)
    category = models.CharField(max_length=100, blank=True, null=True)
    off_point = models.BigIntegerField(blank=True, null=True)
    sub_category = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ana_summary'
