from django.db import models

# Create your models here.
class AccelerationTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    nine_axis_acceleration_x = models.CharField(max_length=20)
    nine_axis_acceleration_y = models.CharField(max_length=20)
    nine_axis_acceleration_z = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'acceleration_tbl'


class AngularvelocityTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    nine_axis_angular_velocity_x = models.CharField(max_length=20)
    nine_axis_angular_velocity_y = models.CharField(max_length=20)
    nine_axis_angular_velocity_z = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'angularvelocity_tbl'


class AuthGroup(models.Model):
    name = models.CharField(unique=True, max_length=150)

    class Meta:
        managed = False
        db_table = 'auth_group'


class AuthGroupPermissions(models.Model):
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)
    permission = models.ForeignKey('AuthPermission', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_group_permissions'
        unique_together = (('group', 'permission'),)


class AuthPermission(models.Model):
    name = models.CharField(max_length=255)
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING)
    codename = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'auth_permission'
        unique_together = (('content_type', 'codename'),)


class AuthUser(models.Model):
    password = models.CharField(max_length=128)
    last_login = models.DateTimeField(blank=True, null=True)
    is_superuser = models.IntegerField()
    username = models.CharField(unique=True, max_length=150)
    first_name = models.CharField(max_length=150)
    last_name = models.CharField(max_length=150)
    email = models.CharField(max_length=254)
    is_staff = models.IntegerField()
    is_active = models.IntegerField()
    date_joined = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'auth_user'


class AuthUserGroups(models.Model):
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_groups'
        unique_together = (('user', 'group'),)


class AuthUserUserPermissions(models.Model):
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_user_permissions'
        unique_together = (('user', 'permission'),)


class CanAccelTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    can_accel = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'can_accel_tbl'


class CanBrakeTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    can_brake = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'can_brake_tbl'


class CanPositionTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    can_turn_lever_position = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'can_position_tbl'


class CanSpeedTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    can_speed = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'can_speed_tbl'


class CanSteeringTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    can_steering = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'can_steering_tbl'


class CeleryTaskmeta(models.Model):
    task_id = models.CharField(unique=True, max_length=255)
    status = models.CharField(max_length=50)
    result = models.TextField(blank=True, null=True)
    date_done = models.DateTimeField()
    traceback = models.TextField(blank=True, null=True)
    hidden = models.IntegerField()
    meta = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'celery_taskmeta'


class CeleryTasksetmeta(models.Model):
    taskset_id = models.CharField(unique=True, max_length=255)
    result = models.TextField()
    date_done = models.DateTimeField()
    hidden = models.IntegerField()

    class Meta:
        managed = False
        db_table = "celery_tasksetmeta"


class DjangoAdminLog(models.Model):
    action_time = models.DateTimeField()
    object_id = models.TextField(blank=True, null=True)
    object_repr = models.CharField(max_length=200)
    action_flag = models.PositiveSmallIntegerField()
    change_message = models.TextField()
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING, blank
=True, null=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'django_admin_log'


class DjangoContentType(models.Model):
    app_label = models.CharField(max_length=100)
    model = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'django_content_type'
        unique_together = (('app_label', 'model'),)


class DjangoMigrations(models.Model):
    app = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    applied = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_migrations'


class DjangoSession(models.Model):
    session_key = models.CharField(primary_key=True, max_length=40)
    session_data = models.TextField()
    expire_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_session'


class DjceleryCrontabschedule(models.Model):
    minute = models.CharField(max_length=64)
    hour = models.CharField(max_length=64)
    day_of_week = models.CharField(max_length=64)
    day_of_month = models.CharField(max_length=64)
    month_of_year = models.CharField(max_length=64)

    class Meta:
        managed = False
        db_table = 'djcelery_crontabschedule'


class DjceleryIntervalschedule(models.Model):
    every = models.IntegerField()
    period = models.CharField(max_length=24)

    class Meta:
        managed = False
        db_table = 'djcelery_intervalschedule'


class DjceleryPeriodictask(models.Model):
    name = models.CharField(unique=True, max_length=200)
    task = models.CharField(max_length=200)
    args = models.TextField()
    kwargs = models.TextField()
    queue = models.CharField(max_length=200, blank=True, null=True)
    exchange = models.CharField(max_length=200, blank=True, null=True)
    routing_key = models.CharField(max_length=200, blank=True, null=True)
    expires = models.DateTimeField(blank=True, null=True)
    enabled = models.IntegerField()
    last_run_at = models.DateTimeField(blank=True, null=True)
    total_run_count = models.PositiveIntegerField()
    date_changed = models.DateTimeField()
    description = models.TextField()
    crontab = models.ForeignKey(DjceleryCrontabschedule, models.DO_NOTHING, blank=True, null=True)
    interval = models.ForeignKey(DjceleryIntervalschedule, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'djcelery_periodictask'


class DjceleryPeriodictasks(models.Model):
    ident = models.SmallIntegerField(primary_key=True)
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'djcelery_periodictasks'


class DjceleryTaskstate(models.Model):
    state = models.CharField(max_length=64)
    task_id = models.CharField(unique=True, max_length=36)
    name = models.CharField(max_length=200, blank=True, null=True)
    tstamp = models.DateTimeField()
    args = models.TextField(blank=True, null=True)
    kwargs = models.TextField(blank=True, null=True)
    eta = models.DateTimeField(blank=True, null=True)
    expires = models.DateTimeField(blank=True, null=True)
    result = models.TextField(blank=True, null=True)
    traceback = models.TextField(blank=True, null=True)
    runtime = models.FloatField(blank=True, null=True)
    retries = models.IntegerField()
    hidden = models.IntegerField()
    worker = models.ForeignKey('DjceleryWorkerstate', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'djcelery_taskstate'


class DjceleryWorkerstate(models.Model):
    hostname = models.CharField(unique=True, max_length=255)
    last_heartbeat = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'djcelery_workerstate'


class DjkombuMessage(models.Model):
    visible = models.IntegerField()
    sent_at = models.DateTimeField(blank=True, null=True)
    payload = models.TextField()
    queue = models.ForeignKey('DjkombuQueue', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'djkombu_message'


class DjkombuQueue(models.Model):
    name = models.CharField(unique=True, max_length=200)

    class Meta:
        managed = False
        db_table = 'djkombu_queue'


class EquipStatusTbl(models.Model):
    equip_id = models.BigIntegerField(primary_key=True)
    startup_st = models.TextField()  # This field type is a guess.
    operation_st = models.TextField()  # This field type is a guess.
    mqtt_st = models.TextField()  # This field type is a guess.
    device_st = models.TextField()  # This field type is a guess.

    class Meta:
        managed = False
        db_table = 'equip_status_tbl'


class GeomagnetismTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    nine_axis_geomagnetism_x = models.CharField(max_length=20)
    nine_axis_geomagnetism_y = models.CharField(max_length=20)
    nine_axis_geomagnetism_z = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'geomagnetism_tbl'


class LocationTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    gps_date = models.CharField(max_length=20)
    latitude = models.CharField(max_length=20)
    latitude_direction = models.CharField(max_length=20)
    longitude = models.CharField(max_length=20)
    longitude_direction = models.CharField(max_length=20)
    velocity = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'location_tbl'


class SatelliteTbl(models.Model):
    id = models.BigAutoField(primary_key=True)
    equip_id = models.BigIntegerField()
    device_id = models.BigIntegerField()
    seqno = models.BigIntegerField()
    measurement_date = models.DateTimeField()
    run_start_date = models.DateTimeField()
    run_end_date = models.DateTimeField()
    type_id = models.BigIntegerField()
    positioning_quality = models.CharField(max_length=20)
    used_satellites = models.CharField(max_length=20)
    reg_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'satellite_tbl'






