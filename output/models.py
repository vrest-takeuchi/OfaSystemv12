from django.db import models


class AnaSummary(models.Model):
    equip_id = models.BigIntegerField(blank=True, null=True)
    run_start_date = models.DateTimeField(blank=True, null=True)
    result = models.BigIntegerField( blank=True, null=True)
    total_offpoint = models.BigIntegerField( blank=True, null=True)
    offpoint_detail = models.BigIntegerField( blank=True,unique=True, null=True)
    comment = models.BigIntegerField( blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ana_summary'

class CategoryDetail(models.Model):
    id = models.BigIntegerField(primary_key=True)
    name = models.TextField(blank=True, null=True)
    detail = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'category_detail'
    #
    # def __str__(self):
    #     return str(self.name)


class OffpointDetail(models.Model):
    offpoint_detail= models.ForeignKey(AnaSummary,to_field='offpoint_detail',on_delete=models.CASCADE, related_name="detail")
    measurement_date = models.DateTimeField(blank=True, null=True)
    offpoint = models.BigIntegerField( blank=True, null=True)
    offpoint_category = models.BigIntegerField( blank=True, null=True)
    category = models.ForeignKey(CategoryDetail,to_field='id',on_delete=models.DO_NOTHING,related_name='category_name')
    evaluation_place = models.BigIntegerField( blank=True, null=True)
    block_no = models.BigIntegerField( blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'offpoint_detail'




