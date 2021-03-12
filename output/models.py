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
    offpoint = models.BigIntegerField(blank=True, null=True)
    evaluation_place = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'category_detail'

    #
    # def __str__(self):
    #     return str(self.name)
class DrivingEvaluationItemTbl(models.Model):
    evaluation_cd = models.TextField(db_column='evaluation_cd',unique=True,  blank=True, null=True)  # Field renamed to remove unsuitable characters.
    evaluation_cd1 = models.TextField(db_column='evaluation-cd1', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    evaluation_cd2 = models.TextField(db_column='evaluation-cd2', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    evaluation_cd3 = models.TextField(db_column='evaluation-cd3', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    evaluation_item_name = models.TextField(db_column='evaluation-item-name', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    evaluation_message = models.TextField(db_column='evaluation-message', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    score = models.BigIntegerField(blank=True, null=True)
    applied_place_inside = models.BigIntegerField(db_column='applied-place-inside', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    applied_place_street = models.BigIntegerField(db_column='applied-place-street', blank=True, null=True)  # Field renamed to remove unsuitable characters.

    class Meta:
        managed = False
        db_table = 'driving_evaluation_item_tbl'

class DrivingEvaluationByBlockTbl(models.Model):
    block_cd = models.TextField(db_column='block_cd', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    block_name = models.TextField(db_column='block-name', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    evaluation_cd = models.TextField(db_column='evaluation_cd', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    evaluation_item_name = models.TextField(db_column='evaluation-item-name', blank=True, null=True)  # Field renamed to remove unsuitable characters.

    class Meta:
        managed = False
        db_table = 'driving_evaluation_by_block_tbl'


class OffpointDetail(models.Model):
    offpoint_detail= models.ForeignKey(AnaSummary,to_field='offpoint_detail',on_delete=models.CASCADE, related_name="detail")
    measurement_date = models.DateTimeField(blank=True, null=True)
    offpoint = models.BigIntegerField( blank=True, null=True)
    offpoint_category = models.BigIntegerField( blank=True, null=True)
    category = models.ForeignKey(DrivingEvaluationItemTbl,to_field='evaluation_cd',on_delete=models.DO_NOTHING,related_name='category_name')
    evaluation_place = models.CharField(max_length=255,blank=True, null=True)
    block_no = models.CharField(max_length=255,blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'offpoint_detail'

class DjangoMigrations(models.Model):
    app = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    applied = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_migrations'

class BlockInfoTbl(models.Model):
    block_cd = models.TextField(db_column='block-cd', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    block_cd_1 = models.TextField(db_column='block-cd-1', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    block_cd_2 = models.TextField(db_column='block-cd-2', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    block_name = models.TextField(db_column='block-name', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    block_caption = models.TextField(db_column='block-caption', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    applied_place_inside = models.BigIntegerField(db_column='applied-place-inside', blank=True, null=True)  # Field renamed to remove unsuitable characters.
    applied_place_street = models.BigIntegerField(db_column='applied-place-street', blank=True, null=True)  # Field renamed to remove unsuitable characters.

    class Meta:
        managed = False
        db_table = 'block_info_tbl'




class EvaluationList(models.Model):
    car_id = models.FloatField(blank=True, null=True)
    driving_mode = models.TextField(blank=True, null=True)
    ms_course_id = models.FloatField(blank=True, null=True)
    equip_id = models.FloatField(blank=True, null=True)
    driving_course_name = models.TextField(blank=True, null=True)
    run_start_date = models.DateTimeField(blank=True, null=True)
    result = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'evaluation_list'


class Threshold(models.Model):
    evaluation_cd = models.TextField(primary_key=True)
    val1 = models.DecimalField(max_digits=100, decimal_places=0, blank=True, null=True)
    val2 = models.DecimalField(max_digits=100, decimal_places=0, blank=True, null=True)
    val3 = models.DecimalField(max_digits=100, decimal_places=0, blank=True, null=True)
    val4 = models.DecimalField(max_digits=100, decimal_places=0, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'threshold'



