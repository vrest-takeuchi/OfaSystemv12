from django_pandas.io import *
from .models import *
import datetime
import pytz
from pytz import timezone
from django.http import HttpResponse
from sqlalchemy import create_engine
import math
import pandas as pd
import numpy as np
from djangoProject2.param_setting import *
from output.models import AnaSummary,OffpointDetail,CategoryDetail
from rest_framework import viewsets
from .serializers import AnaSummarySerializer,OffpointDetailSerializer
from datetime import datetime as dt
import requests
import math
arr = []


def AccelerationDfn(a, b):
    acceleration_df = read_frame(AccelerationTbl.objects.filter(run_start_date=a).filter(equip_id=b))

    return acceleration_df.loc[:, ['measurement_date',
                                   'nine_axis_acceleration_x',
                                   'nine_axis_acceleration_y',
                                   'nine_axis_acceleration_z']]


def AngularvelocityDfn(a, b):
    angularvelocityDf = read_frame(AngularvelocityTbl.objects.filter(run_start_date=a).filter(equip_id=b))
    return angularvelocityDf.loc[:, ['measurement_date',
                                     'nine_axis_angular_velocity_x',
                                     'nine_axis_angular_velocity_y',
                                     'nine_axis_angular_velocity_z']]


def CanBrakeDfn(a, b):
    canBrakeDf = read_frame(CanBrakeTbl.objects.filter(run_start_date=a).filter(equip_id=b))
    return canBrakeDf.loc[:, ['measurement_date', 'can_brake']]


def CanPositionDfn(a, b):
    canPositionDf = read_frame(CanPositionTbl.objects.filter(run_start_date=a).filter(equip_id=b))
    return canPositionDf.loc[:, ['measurement_date', 'can_turn_lever_position']]


def CanSpeedDfn(a, b):
    canSpeedDf = read_frame(CanSpeedTbl.objects.filter(run_start_date=a).filter(equip_id=b))
    return canSpeedDf.loc[:, ['measurement_date', 'can_speed']]


def CanSteeringDfn(a, b):
    canSteeringDf = read_frame(CanSteeringTbl.objects.filter(run_start_date=a).filter(equip_id=b))
    return canSteeringDf.loc[:, ['measurement_date', 'can_steering']]


def CanAccelDfn(a, b):
    canAccelDf = read_frame(CanAccelTbl.objects.filter(run_start_date=a).filter(equip_id=b))
    return canAccelDf.loc[:, ['measurement_date', 'can_accel']]


def SatelliteDfn(a, b):
    satelliteDf = read_frame(SatelliteTbl.objects.filter(run_start_date=a).filter(equip_id=b))
    return satelliteDf.loc[:, ['measurement_date', 'positioning_quality', 'used_satellites']]


def LocationDfn(a, b):
    locationDf = read_frame(LocationTbl.objects.filter(run_start_date=a).filter(equip_id=b))
    return locationDf.loc[:, ['measurement_date', 'latitude', 'longitude', 'velocity']]


def location_df(a, b):
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
    location_df_val = pd.read_sql('gps_block', con=engine)
    df = location_df_val[(location_df_val["run_start_date"] == a) & (location_df_val["equip_id"] == b)]
    data_location_a= df.sort_values(['id'], ascending=[True])
    data_location = data_location_a.reset_index(drop=True)
    if len(data_location) > 0:
        for y in range(0, len(data_location) - 1):  # ベクトルAB（vecX/vecY)

            a1 = float(data_location['latitude'].values[y])
            a2 = float(data_location['longitude'].values[y])
            b1 = float(data_location['latitude'].values[y + 1])
            b2 = float(data_location['longitude'].values[y + 1])
            vecX = round(b1 - a1, significant_digits_vec)
            vecY = round(b2 - a2, significant_digits_vec)
            vec = round(math.sqrt(vecX ** 2 + vecY ** 2), significant_digits_vec)
            data_location.loc[y, 'vecX'] = vecX
            data_location.loc[y, 'vecY'] = vecY
            data_location.loc[y, 'VEC'] = vec
        data=[]
        for i, g in data_location.groupby([(data_location['block_no'] != data_location['block_no'].shift()).cumsum()]):

            # print(model_data_location['block_no'].tolist())
            g['time'] = i
            data.append(g)
        data_a = pd.concat(data, ignore_index=True)

        return data_a
    else:
        data_location['vecX'] = 'NaN'
        data_location['vecY'] = 'NaN'
        data_location['VEC'] = 'NaN'
        data_location['time'] = 'NaN'
        return data_location


"""status取得"""


def status_df(a):
    try:
        with conn.cursor() as cursor:
            sql = "SELECT equip_id, hex(operation_st), hex(mqtt_st) FROM equip_status_tbl WHERE equip_id = %s"
            cursor.execute(sql, (a,))
            a = pd.DataFrame(cursor.fetchall()).reindex(axis='index')
            if len(a)==0:
                status=1
            else:
                status = a.at[0, 'hex(operation_st)']

    finally:
        print('')
    return status

def reverse_check(a):
    if (len(a)>0):
        a1 = a.reset_index()
        for s in range(0, len(a)-1):
            ax = float(a1.at[s, 'longitude'])
            ay = float(a1.at[s, 'latitude'])
            bx = float(a1.at[s+1, 'longitude'])#次の点のベクトル
            by = float(a1.at[s+1, 'latitude'])#次の点のベクトル
            a1.at[s,'X']=bx-ax
            a1.at[s,'Y']=by-ay
            #
            # a1.at[s, 'VEC_atoa+1'] = round(math.sqrt((bx - ax) ** 2 + (by - ay) ** 2), 6)#a点から次の点のベクトル
            # a1.at[s, 'inp_measurement_model'] = (ax * bx + ay * by)#a点でのベクトル
        for s in range(0, len(a)-1):
            aX = float(a1.at[s, 'X'])
            aY = float(a1.at[s, 'Y'])
            bX = float(a1.at[s+1, 'X'])#次の点のベクトル
            bY = float(a1.at[s+1, 'Y'])#次の点のベクトル
            a1.at[s, 'VEC_AtoA+1'] = math.sqrt((bX - aX) ** 2 + (bY - aY) ** 2)#A点から次の点のベクトル
            a1.at[s, 'inpAtoA+1'] = (aX * bX + aY * bY)#A点から次のベクトルでの内積
            a1.loc[(a1['inpAtoA+1']< 0) , 'inpminus'] = 'minus'

        return a1

    else:
        a1 = a
        a1['VEC_AtoA+1'] = 'NaN'
        a1['inpAtoA+1'] = 'NaN'
        a1['inpminus'] = 'NaN'
        return a1

def continue_count(df):
    # NaNはグルーピング時に無視されるので適当に入れ替えておく
    df['new_value'] = df['inpminus'].fillna('-')
    # 1個シフトした行が違う値なら +1cumsum, 同じ値なら no cumsum
    df['changepoint_cumsum'] = (df['new_value'] != df['new_value'].shift()).cumsum()
    # new_valueとcontinue_cumsumでグルーピング
    df_group = df.groupby(['new_value', 'changepoint_cumsum'])
    mi_df = df.set_index(['new_value', 'changepoint_cumsum'])
    # 各グループに入っている値をカウント & 元データとガッチャンコ
    mi_df['continue_count'] = df_group['new_value'].count()
    # インデックスをもどして上げる
    df = mi_df.reset_index()
    return df.drop(['new_value', 'changepoint_cumsum'], axis=1)

def function_neer_point(a, b):
    if (len(b) > 0)&(len(a)>0):
        a1 = a.reset_index()
        b1 = b.reset_index()
        min_num = min(len(a)-1, len(b)-1);

        for s in range(0, min_num):
            ax = float(a1.at[s, 'longitude'])
            ay = float(a1.at[s, 'latitude'])
            bx = float(b1.at[s, 'longitude'])
            by = float(b1.at[s, 'latitude'])
            spa = float(a1.at[s, 'velocity'])
            spb = float(b1.at[s, 'velocity'])
            a1.at[s, 'VEC_measurement_model'] = round(math.sqrt((bx - ax) ** 2 + (by - ay) ** 2), 6)
            a1.at[s, 'inp_measurement_model'] = (ax * bx + ay * by)
            a1.at[s, 'speed_dif_measurement_model'] = spb - spa

        return a1

    else:
        a1 = a
        a1['VEC_measurement_model'] = 'NaN'
        a1['inp_measurement_model'] = 'NaN'
        a1['speed_dif_measurement_model'] = 'NaN'

        return a1


def gps_location_data(a, b):

    for c1, sdf in a.groupby(['block_no','time']):


        for c2, sdf2 in b.groupby(['block_no','time']):
            if c1 == c2:
                data = function_neer_point(sdf, sdf2)
                x = pd.DataFrame(data)
                arr.append(x)

    if len(arr) > 0:
        gps_data__ = pd.concat(arr, ignore_index=True)
        gps_data__.sort_values(by=['measurement_date'], inplace=True)

        return gps_data__
    else:
        gps_data__ = function_neer_point(a, b)
        gps_data__.sort_values(by=['measurement_date'], inplace=True)

        return gps_data__


model = location_df(model_run_start_date, model_equip_id)
global result

from background_task import background


@background(queue='queue_name1', schedule=1)
def some_long_duration_process(param_equip_id,rsd_tex):
    param_run_start_date_wotz = dt.strptime(rsd_tex, '%Y-%m-%d %H:%M:%S')
    param_run_start_date_text = rsd_tex+'+00:00'
    param_run_start_date=pd.to_datetime(param_run_start_date_wotz, utc=True)
    Satelite_data = SatelliteDfn(param_run_start_date, param_equip_id)#GPS測位数ロード
    location_measurement_data = location_df(param_run_start_date_text, param_equip_id)#GPS経度緯度ロード
    location_model_data = location_df(model_run_start_date, model_equip_id)
    location_data_meas =continue_count(reverse_check(location_measurement_data))
    location_data_mode = continue_count(reverse_check(location_model_data))
    location_data=gps_location_data(location_data_meas,location_data_mode,)
    status = status_df(param_equip_id)
    status = int(status)
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
    Result=0
    percent=0
    if status != 0:  # status error⇒detail無しまたは車載IDエラー
        print('not_send_completely...')
        Result = Result + 1
        df_result = pd.DataFrame(
            {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date], 'result': [Result],
             'total_offpoint': '1', 'offpoint_detail': [id],
             'comment': '100'})
        """既存データを削除"""
        with conn2.cursor() as cur:
            # テーブルを削除する SQL を準備
            sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
            cur.execute(sql, (param_run_start_date,))
        conn2.commit()
        df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)
        api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"
        url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
        r = requests.get(url)
        print('not_send_completely...')
        return HttpResponse(r.text)

    if (len(Satelite_data) == 0) | (len(location_data) == 0):#GPSデータが存在しない時、Sateliteかlocationどちらか
        gps_data = pd.concat([Satelite_data, location_data])
        print('no_gps_data')
    else:
        gps_data = pd.merge(Satelite_data, location_data, on='measurement_date', how='outer')
    Acceleration_data = AccelerationDfn(param_run_start_date, param_equip_id)
    Angularvelocity_data = AngularvelocityDfn(param_run_start_date, param_equip_id)
    CanBrake_data = CanBrakeDfn(param_run_start_date, param_equip_id)
    CanPosition_data = CanPositionDfn(param_run_start_date, param_equip_id)
    CanSpeed_data = CanSpeedDfn(param_run_start_date, param_equip_id)
    CanSteering_data = CanSteeringDfn(param_run_start_date, param_equip_id)
    CanAccel_data = CanAccelDfn(param_run_start_date, param_equip_id)
    axis_data = pd.merge(Acceleration_data, Angularvelocity_data, on='measurement_date', how='outer')
    can1_data = pd.merge(CanBrake_data, CanPosition_data, on='measurement_date', how='outer')
    can2_data = pd.merge(CanSpeed_data, CanSteering_data, on='measurement_date', how='outer')
    can3_data = pd.merge(can1_data, can2_data, on='measurement_date', how='outer')
    can_data = pd.merge(can3_data, CanAccel_data, on='measurement_date', how='outer')
    num_gps = len(gps_data)
    num_can = len(can_data)
    num_axis = len(axis_data)
    comment = 0
    Result=0
    id = pd.read_sql('ana_summary', con=engine)['offpoint_detail'].max() + 1
    if len(gps_data['block_no']) == 0 & num_gps != 0:
        percent = 1
    elif len(gps_data['block_no']) != 0 & num_gps != 0:
        percent = (gps_data['block_no'] == '100').sum() / num_gps
    elif len(gps_data['block_no']) == 0 & num_gps == 0:
        percent = 1
    elif len(gps_data['block_no']) != 0 & num_gps == 0:
        percent = 1
    if num_axis < num_axis_min:
        comment = comment + 1
        print('not a lot of data..9-axis data')
    if num_can < num_can_min:
        comment = comment + 2
        print('not a lot of data..can data')
    if percent > block_e_percent:
        comment = comment + 10
        print('not a lot of block_data...')
    if num_gps < num_gps_min:
        Result = Result + 10
        df_result = pd.DataFrame(
            {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date], 'result': [Result],
             'total_offpoint': '1', 'offpoint_detail': [id],
             'comment': [comment]})
        with conn2.cursor() as cur:
            # テーブルを削除する SQL を準備
            sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
            cur.execute(sql, (param_run_start_date,))
        conn2.commit()
        df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)
        print('not a lot of...gps data...')
    else:
        print('start_analysis...')
        gps_axis_data = pd.merge(gps_data, axis_data, on='measurement_date', how='outer')
        gps_axis_can_data = pd.merge(gps_axis_data, can_data, on='measurement_date', how='outer')
        gps_axis_can_data['measurement_date'] = pd.to_datetime(gps_axis_can_data['measurement_date'])
        gps_axis_can_data.sort_values(by=['measurement_date'], inplace=True)
        Ana_data = gps_axis_can_data.reset_index(drop=True).fillna(method='ffill').drop('index', axis=1)
        """解析用測定値"""
        inp = Ana_data['inpAtoA+1'].astype(float)  # 測定値のベクトルの内積
        VEC = Ana_data['VEC_AtoA+1'].astype(float)  # 測定値のベクトルの差の大きさ
        vel_dif = Ana_data['speed_dif_measurement_model'].astype(float)  # 測定値とモデル値の速度差
        model_vel = model['velocity']  # モデル値の速度
        velocity = Ana_data['velocity']
        block_no = Ana_data['block_no']  # 測定値のブロックナンバー
        can_speed = Ana_data['can_speed'].astype(float)  # 測定値のCANスピード
        axis_y = Ana_data['nine_axis_acceleration_y'].astype(float)
        axis_x = Ana_data['nine_axis_acceleration_x'].astype(float)
        blake_sw = Ana_data['can_brake'].astype(float)#ブレーキスイッチ
        accel = Ana_data['can_accel'].astype(float) #アクセル開度
        steering_level = Ana_data['can_steering'].astype(float).fillna(0)
        minus=Ana_data['continue_count']
        comment=0
        if Ana_data['used_satellites'].astype(float).min()<10:
            comment = comment + 1000
            print('not a lot of used satellites...')
        if Ana_data['positioning_quality'].astype(float).min()<4:
            comment = comment + 2000
            print('law positioning_quality...')
        """逆行/逆行/小"""
        Ana_data.loc[(inp < 0) & (minus > reverse_s_val), '050101'] = reverse_s_point  # 逆行小
        """逆行/逆行/中"""
        Ana_data.loc[(inp < 0) & (minus > reverse_m_val), '050102'] = reverse_m_point  # 逆行中
        """逆行/逆行/大"""
        Ana_data.loc[(inp < 0) & (minus > reverse_l_val), '050103'] = reverse_l_point  # 逆行大

        """'徐行違反/徐行違反/右左折"""
        Ana_data.loc[(vel_dif > slowly_speed_dif) & (model_vel < slowly_speed) & (block_no == 'F4'), '320101'] = slow_down_cross_point
        """'徐行違反/徐行違反/頂上"""
        Ana_data.loc[(vel_dif > slowly_speed_dif) & (model_vel < slowly_speed) & (block_no == 'L1'), '320103'] = slow_down_top_point
        """'徐行違反/徐行違反/坂"""
        Ana_data.loc[(vel_dif > slowly_speed_dif) & (model_vel < slowly_speed) & (block_no == 'L2'), '320104'] = slow_down_slope_point

        """'速度超過/速度超過/小"""
        Ana_data.loc[(can_speed > speedover_s_val) | (velocity > speedover_s_val)& (block_no == 'C0'), '440101'] = speedover_s_point  # 速度超過小
        """'速度超過/速度超過/中"""
        Ana_data.loc[(can_speed > speedover_m_val) | (velocity > speedover_m_val)& (block_no == 'C0'), '440102'] = speedover_m_point  # 速度超過中
        """'速度超過/速度超過/大"""
        Ana_data.loc[(can_speed > speedover_l_val) | (velocity > speedover_l_val)& (block_no == 'C0'), '440103'] = speedover_l_point  # 速度超過大

        """速度早すぎ/速度（小）/速い"""
        Ana_data.loc[(vel_dif > speed_fast_s_val)& (block_no == 'C0'), '140101'] = speed_fast_s_point
        """速度早すぎ/速度（大）/速い"""
        Ana_data.loc[(vel_dif >= speed_fast_m_val)& (block_no == 'C0'), '140201'] = speed_fast_m_point
        """速度速すぎ(カーブ）"""
        Ana_data.loc[(axis_y.abs() > speedover_c_s_val) & (block_no == 'D0'), '140102'] = speedover_c_s_point
        Ana_data.loc[(axis_y.abs() > speedover_c_m_val) & (block_no == 'D0'), '140202'] = speedover_c_m_point
        """制動操作不良/ブレーキ/不円滑"""
        Ana_data.loc[(axis_x.abs() > blake_uneven_val) & (blake_sw == blake_sw_on_val) & (block_no == 'C0'), '130102'] = blake_uneven_point
        """アクセルむら/アクセルむら/急発進"""
        Ana_data.loc[(axis_x.abs() > accel_uneven_val) & (accel > accel_on_val), '030101'] = accel_uneven_point
        """急ハンドル/急ハンドル"""
        Ana_data.loc[(axis_y.abs() >= sudden_handle_val) & ((steering_level.astype(int) > level_right) | (steering_level.astype(int) > level_left)), '180101'] = sudden_handle_point
        """'急ブレーキ禁止違反/急ブレーキ"""
        Ana_data.loc[(axis_x.abs() > sudden_brake_val) & (blake_sw == blake_sw_on_val) & (
                can_speed < slowly_speed), '400101'] = sudden_brake_point
        """合図不履行"""
        ana = []

        for c1, sdf in Ana_data.groupby(['block_no','time']):

            num_right = (sdf['can_turn_lever_position'] == position_right).sum()
            num_left = (sdf['can_turn_lever_position'] == position_left).sum()
            steering_level_right = (sdf['can_steering'].fillna(0).astype(float).astype(int) > level_right).sum()
            steering_level_left = (sdf['can_steering'].fillna(0).astype(float).astype(int).fillna(0) > level_left).sum()
            if c1[0] == 'B0':  # 発着所
                Ana_data.loc[
                    (num_right < 1) and (num_left < 1), '100101'] = dep_fail_to_signal_not_point
                Ana_data.loc[(num_right < dep_fail_to_signal_time_val) or (
                        num_left < dep_fail_to_signal_time_val), '100102'] = dep_fail_to_signal_time_point
                Ana_data.loc[(num_right > dep_fail_to_signal_return_val) or (
                        num_left > dep_fail_to_signal_return_val), '100103'] = dep_fail_to_signal_return_point
            else:
                Ana_data['100101'] = np.nan
                Ana_data['100102'] = np.nan
                Ana_data['100103'] = np.nan

            if c1[0] == 'E4':  # 交差点
                if (steering_level_right > 1) or (steering_level_left > 1):
                    Ana_data.loc[(num_right < 1) and (num_left < 1), '100301'] = cross_fail_to_signal_not_point
                    Ana_data.loc[(num_right < cross_fail_to_signal_time_val) or (
                            num_left < cross_fail_to_signal_time_val), '100302'] = cross_fail_to_signal_time_point
                    Ana_data.loc[(num_right > cross_fail_to_signal_return_val) or (
                            num_left > cross_fail_to_signal_return_val), '100303'] = cross_fail_to_signal_return_point
            else:
                Ana_data['100301'] = np.nan
                Ana_data['100302'] = np.nan
                Ana_data['100303'] = np.nan
            ana.append(Ana_data)
        Ana_data = pd.concat(ana, ignore_index=True)
        anaaa = Ana_data.loc[:, ['050101',
                                 '050102',
                                 '050103',
                                 '140101',
                                 '140201',
                                 '320101',
                                 '320103',
                                 '320104',
                                 '440101',
                                 '440102',
                                 '440103',
                                 '140102',
                                 '140202',
                                 '130102',
                                 '030101',
                                 '180101',
                                 '400101',
                                 '100101',
                                 '100102',
                                 '100103',
                                 '100301',
                                 '100302',
                                 '100303',
                                 ]].dropna(how="all").index

        anaana = Ana_data.loc[anaaa].sort_values('measurement_date', ascending=[True])

        Ana_data = anaana.reset_index(drop=True)
        def ana_cal(x):
            category = x[
                ['050101',
                 '050102',
                 '050103',
                 '140101',
                 '140201',
                 '320101',
                 '320103',
                 '320104',
                 '440101',
                 '440102',
                 '440103',
                 '140102',
                 '140202',
                 '130102',
                 '030101',
                 '180101',
                 '400101',
                 '100101',
                 '100102',
                 '100103',
                 '100301',
                 '100302',
                 '100303',]]
            x['category'] = category.astype(float).idxmax(axis=1)
            # t=x.drop(['id','positioning_quality','speed_dif_measurement_model','can_accel','nine_axis_angular_velocity_z','can_brake','can_speed','can_steering','can_turn_lever_position','nine_axis_acceleration_z','nine_axis_angular_velocity_y','nine_axis_angular_velocity_x','nine_axis_acceleration_y','nine_axis_acceleration_x','level_0','vecX','vecY','velocity','inp_measurement_model','update_time','inpAtoA+1','inpminus','VEC_measurement_model','continue_count','VEC','X','Y','VEC_AtoA+1','used_satellites' ,'latitude','longitude','driving_course_id',], axis=1)
            x['off_point']=category.astype(float).max(axis=1)
            x['evaluation_place']=x['block_no']
            return x

        if len(Ana_data) == 0:
            print('not off point...')
            Result=0
            Result = Result
            comment = comment + 10000

            df_result = pd.DataFrame(
                {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date], 'result': [Result],
                 'total_offpoint': '0', 'offpoint_detail': [id],
                 'comment': [comment]})
            with conn2.cursor() as cur:
                # テーブルを削除する SQL を準備
                sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
                cur.execute(sql, (param_run_start_date,))
            conn2.commit()
            engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
            df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)
            api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"
            url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
            r = requests.get(url)
            print('not off point analysis done...')
            return HttpResponse(r.text)
        else :
            Result=0
            ana_cal_data = ana_cal(Ana_data)

            a = ana_cal_data.loc[:, ['equip_id',
                                     'run_start_date',
                                     'measurement_date',
                                     'block_no',
                                     'evaluation_place',
                                     'category',
                                     'off_point',
                                     'time']]


            a['result'] = Result
            a['comment'] = comment
            a['sub_category'] = 100
            df_result = a
            evaluation = []
            for c1, sdf in df_result.groupby('time'):
                df = sdf.drop_duplicates(subset='category')
                var_reverse = df[(df['category'] == '050103') | (df['category'] == '050102') | (df['category'] == '050101')]
                var_speed_fast = df[(df['category']== '140101') | (df['category'] == '140201')]
                var_speed_fast_c = df[(df['category'] == '140102') | (df['category'] == '140202')]
                var_slow_cross = df[(df['category'] == '320101')]
                var_slow_top = df[(df['category'] == '320103')]
                var_slow_slope = df[(df['category'] == '320104')]
                var_speedover = df[(df['category'] == '440101') | (df['category'] == '440102')| (df['category']== '440103')]
                var_blake = df[(df['category'] == '400101')]
                var_accel = df[(df['category'] =='030101')]
                var_handle = df[(df['category'] == '180101')]
                var_brake_s = df[(df['category']== '130102')]
                var_dep_sig = df[(df['category'] == '100101') | (df['category']== '100102')| (df['category']== '100103')]
                var_cross_sig = df[(df['category'] == '100301') | (df['category']== '100302')| (df['category']== '100303')]
                if len(var_reverse) > 0:
                    A_1_2 = var_reverse.loc[[var_reverse['off_point'].astype(int).idxmax()]]
                else:
                    A_1_2 = pd.DataFrame()
                if len(var_speed_fast) > 0:
                    A_3_4 = var_speed_fast.loc[[var_speed_fast['off_point'].astype(int).idxmax()]]
                else:
                    A_3_4 = pd.DataFrame()
                if len(var_speedover)>0:
                    A_8_9_10 = var_speedover.loc[[var_speedover['off_point'].astype(int).idxmax()]]
                else:
                    A_8_9_10 = pd.DataFrame()
                if len(var_speed_fast_c)>0:
                    A_11_12 = var_speed_fast_c.loc[[var_speed_fast_c['off_point'].astype(int).idxmax()]]
                else:
                    A_11_12 = pd.DataFrame()
                if len(var_dep_sig)>0:
                    A_17_19 = var_dep_sig.loc[[var_dep_sig['off_point'].astype(int).idxmax()]]
                else:
                    A_17_19 = pd.DataFrame()
                if len(var_cross_sig)>0:
                    A_20_22 = var_cross_sig.loc[[var_cross_sig['off_point'].astype(int).idxmax()]]
                else:
                    A_20_22 = pd.DataFrame()
                sss = pd.concat(
                    [A_1_2, A_3_4, var_slow_cross, var_slow_top, var_slow_slope, var_blake, var_accel, var_handle,
                     var_brake_s, A_8_9_10, A_11_12, A_17_19, A_20_22])
                evaluation.append(sss)
            Ana_data = pd.concat(evaluation, ignore_index=True)

            """既存データを削除"""
            with conn2.cursor() as cur:
                # テーブルを削除する SQL を準備
                sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')

                cur.execute(sql, (param_run_start_date,))
            conn2.commit()
            engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
            total_point=Ana_data['off_point'].sum()
            measurement_date_val=Ana_data.loc[ :,'measurement_date']
            offpoint_val=Ana_data.loc[ :,'off_point']
            category_val=Ana_data.loc[ :,'category']
            evaluation_place_val=Ana_data.loc[ :,'evaluation_place']
            block_no_val=Ana_data.loc[ :,'block_no']
            id =  read_frame(AnaSummary.objects.all())['id'].max() + 1
            Summary_data=pd.DataFrame(
                {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date], 'result': [Result],
                 'total_offpoint': [total_point], 'offpoint_detail': [id],
                 'comment': [comment]})
            detail_data=pd.DataFrame(
                {'offpoint_detail_id': id, 'measurement_date': measurement_date_val, 'offpoint': offpoint_val,
                 'offpoint_category': offpoint_val*10, 'category_id': category_val,'evaluation_place':evaluation_place_val,'block_no':block_no_val,
                 })


            Summary_data.to_sql('ana_summary', con=engine, if_exists='append', index=False)
            detail_data.to_sql('offpoint_detail', con=engine, if_exists='append', index=False)
            print('analysis_done...')
            print('analysis_output_completely...')

def ana_data(request):

    print('start_processing......')
    param_text = request.GET.get('equip_id')
    param_equip_id=int(param_text)
    rsd_tex = request.GET.get('run_start_date')
    param_run_start_date_wotz = dt.strptime(rsd_tex, '%Y-%m-%d %H:%M:%S')
    param_run_start_date=pd.to_datetime(param_run_start_date_wotz, utc=True)
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))

    t = pd.read_sql('ana_summary', con=engine).drop_duplicates(subset='run_start_date')
    data=t[t['run_start_date'] == param_run_start_date]
    q = len(data[data['result'].astype(int) > 0])
    s = len(data[data['comment'].astype(int) > 0])
    exist = len(data)

    if (exist != 0) & (q == 0) & (s == 0):#detail⇒すべて表示
        api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"

        url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
        r = requests.get(url)
        print('exist_data...')

        return HttpResponse(r.text)
    else:
        api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date={run_start_date}"
        url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
        r = requests.get(url)
        with conn2.cursor() as cur:
            # テーブルを削除する SQL を準備
            sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
            cur.execute(sql, (param_run_start_date,))
        conn2.commit()

        print('make_data...')

        some_long_duration_process(param_equip_id,rsd_tex)
        if r.text=='[]':
            r=[{"id":np.nan,"equip_id":param_equip_id,"run_start_date":rsd_tex,"result":11,"total_offpoint":np.nan,"comment":np.nan,"detail":[]}]
            return HttpResponse(r)
        else:
            return HttpResponse(r.text)

class AnaSummaryViewSet(viewsets.ModelViewSet):
    queryset = AnaSummary.objects.all()  # 全てのデータを取得
    serializer_class = AnaSummarySerializer
    filter_fields = ('equip_id', 'run_start_date')


