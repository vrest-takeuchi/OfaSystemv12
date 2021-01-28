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
from output.models import AnaSummary,OffpointDetail
from rest_framework import viewsets
from .serializers import AnaSummarySerializer,OffpointDetailSerializer
from datetime import datetime as dt
import requests

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
    model_data_location_a= df.sort_values(['id'], ascending=[True])
    model_data_location = model_data_location_a.reset_index(drop=True)
    if len(model_data_location) > 0:
        for y in range(0, len(model_data_location) - 1):  # ベクトルAB（vecX/vecY)

            a1 = float(model_data_location['latitude'].values[y])
            a2 = float(model_data_location['longitude'].values[y])
            b1 = float(model_data_location['latitude'].values[y + 1])
            b2 = float(model_data_location['longitude'].values[y + 1])
            vecX = round(b1 - a1, significant_digits_vec)
            vecY = round(b2 - a2, significant_digits_vec)
            vec = round(math.sqrt(vecX ** 2 + vecY ** 2), significant_digits_vec)
            model_data_location.loc[y, 'vecX'] = vecX
            model_data_location.loc[y, 'vecY'] = vecY
            model_data_location.loc[y, 'VEC'] = vec
        data=[]
        for i, g in model_data_location.groupby([(model_data_location['block_no'] != model_data_location['block_no'].shift()).cumsum()]):

            # print(model_data_location['block_no'].tolist())
            g['time'] = i
            data.append(g)
        data_a = pd.concat(data, ignore_index=True)

        return data_a
    else:
        model_data_location['vecX'] = 'NaN'
        model_data_location['vecY'] = 'NaN'
        model_data_location['VEC'] = 'NaN'
        model_data_location['time'] = 'NaN'
        return model_data_location


"""status取得"""


def status_df(a):
    try:
        with conn.cursor() as cursor:
            sql = "SELECT equip_id, hex(operation_st), hex(mqtt_st) FROM equip_status_tbl WHERE equip_id = %s"
            cursor.execute(sql, (a,))
            a = pd.DataFrame(cursor.fetchall()).reindex(axis='index')
            status = a.at[0, 'hex(operation_st)']
    finally:
        print('')
    return status


def function_neer_point(a, b):
    if len(b) > 0:
        a1 = a.reset_index()
        b1 = b.reset_index()
        for s in range(0, len(a)):
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


def ana_data(request):

    print('start_processing......')
    param_text = request.GET.get('equip_id')
    param_equip_id=int(param_text)
    rsd_tex = request.GET.get('run_start_date')
    param_run_start_date_wotz = dt.strptime(rsd_tex, '%Y-%m-%d %H:%M:%S')
    param_run_start_date_text = rsd_tex+'+00:00'
    param_run_start_date=pd.to_datetime(param_run_start_date_wotz, utc=True)

    status = status_df(param_equip_id)
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
    id = 0
    id = pd.read_sql('ana_summary', con=engine)['offpoint_detail'].max() + 1

    Result = 0
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
        if status == 0:#status error⇒detail無し
            print('not_send_completely...')

            Result = Result + 1
            df_result = pd.DataFrame(
                {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date], 'result': [Result],'total_offpoint':'1','offpoint_detail':[id],
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

        else:

            Satelite_data = SatelliteDfn(param_run_start_date, param_equip_id)
            measurement = location_df(param_run_start_date_text, param_equip_id)
            model = location_df(model_run_start_date, model_equip_id)

            location_data = gps_location_data(measurement, model)

            if (len(Satelite_data)==0)|(len(location_data)==0):
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
            if len(gps_data['block_no'])==0&num_gps!=0:
                percent=1
            elif len(gps_data['block_no'])!=0&num_gps!=0:
                percent=(gps_data['block_no'] == '100').sum() / num_gps
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

                api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date={run_start_date}"

                url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
                r = requests.get(url)

                print('not a lot of...gps data...')

                return HttpResponse(r.text)

            else:

                print('start_analysis...')
                gps_axis_data = pd.merge(gps_data, axis_data, on='measurement_date', how='outer')
                gps_axis_can_data = pd.merge(gps_axis_data, can_data, on='measurement_date', how='outer')
                gps_axis_can_data['measurement_date'] = pd.to_datetime(gps_axis_can_data['measurement_date'])
                gps_axis_can_data.sort_values(by=['measurement_date'], inplace=True)
                Ana_data = gps_axis_can_data.reset_index(drop=True).fillna(method='ffill').drop('index', axis=1)


                """解析用測定値"""
                inp = Ana_data['inp_measurement_model'].astype(float)  # 測定値のベクトルとモデル値ベクトルの内積
                VEC = Ana_data['VEC_measurement_model'].astype(float)  # 測定値のベクトルとモデル値のベクトルの差の大きさ
                vel_dif = Ana_data['speed_dif_measurement_model'].astype(float)  # 測定値とモデル値の速度差
                model_vel = model['velocity']  # モデル値の速度
                velocity = Ana_data['velocity']
                block_no = Ana_data['block_no']  # 測定値のブロックナンバー
                can_speed = Ana_data['can_speed'].astype(float)  # 測定値のCANスピード
                axis_y = Ana_data['nine_axis_acceleration_y'].astype(float)
                axis_x = Ana_data['nine_axis_acceleration_x'].astype(float)
                blake_sw = Ana_data['can_brake'].astype(float)
                accel = Ana_data['can_accel'].astype(float)
                steering_level = Ana_data['can_steering'].astype(float)

                if Ana_data['used_satellites'].astype(float).min()<10:
                    comment = comment + 1000
                    print('not a lot of used satellites...')
                if Ana_data['positioning_quality'].astype(float).min()<4:
                    comment = comment + 2000
                    print('law positioning_quality...')
                """逆行"""
                Ana_data.loc[(inp < 0) & (VEC > reverse_s_val), '1'] = reverse_s_point  # 逆行小
                Ana_data.loc[(inp < 0) & (VEC > reverse_m_val), '2'] = reverse_m_point  # 逆行大
                """速度早すぎ"""
                Ana_data.loc[vel_dif > speed_fast_s_val, '3'] = speed_fast_s_point
                Ana_data.loc[vel_dif >= speed_fast_m_val, '4'] = speed_fast_m_point
                """徐行違反"""
                Ana_data.loc[(vel_dif > slowly_speed) & (model_vel < slowly_speed) & (
                        block_no == A_block_no_cross), '5'] = slow_down_cross_point
                Ana_data.loc[(vel_dif > slowly_speed) & (model_vel < slowly_speed) & (
                        block_no == A_block_no_top), '6'] = slow_down_top_point
                Ana_data.loc[(vel_dif > slowly_speed) & (model_vel < slowly_speed) & (
                        block_no == A_block_no_slope), '7'] = slow_down_slope_point
                """速度超過"""
                Ana_data.loc[
                    (can_speed > speedover_s_val) | (velocity > speedover_s_val), '8'] = speedover_s_point  # 速度超過小
                Ana_data.loc[
                    (can_speed > speedover_m_val) | (velocity > speedover_m_val), '9'] = speedover_m_point  # 速度超過中
                Ana_data.loc[
                    (can_speed > speedover_l_val) | (velocity > speedover_l_val), '10'] = speedover_l_point  # 速度超過大
                """速度速超過(カーブ）"""
                Ana_data.loc[
                    (axis_y.abs() > speedover_c_s_val) & (
                            block_no == A_block_no_curve), '11'] = speedover_c_s_point
                Ana_data.loc[(axis_y.abs() > speedover_c_m_val) & (
                        block_no == A_block_no_curve), '12'] = speedover_c_m_point
                """制動操作不良"""
                Ana_data.loc[
                    (axis_x.abs() > blake_uneven_val) & (blake_sw == blake_sw_on_val), '13'] = blake_uneven_point
                """アクセルむら"""
                Ana_data.loc[
                    (axis_x.abs() > accel_uneven_val) & (accel > accel_on_val), '14'] = accel_uneven_point
                """急ハンドル"""
                Ana_data.loc[(axis_y.abs() >= sudden_handle_val) & (
                        (steering_level > level_right) | (steering_level > level_left)), '15'] = sudden_handle_point
                """急ブレーキ禁止違反"""
                Ana_data.loc[(axis_x.abs() > sudden_brake_val) & (blake_sw == blake_sw_on_val) & (
                        can_speed < slowly_speed), '16'] = sudden_brake_point
                """合図不履行"""
                ana = []

                for c1, sdf in Ana_data.groupby(['block_no','time']):

                    num_right = (sdf['can_turn_lever_position'] == position_right).sum()
                    num_left = (sdf['can_turn_lever_position'] == position_left).sum()
                    steering_level_right = (sdf['can_steering'] > level_right).sum()
                    steering_level_left = (sdf['can_steering'] > level_left).sum()
                    if c1[0] == A_block_no_dep:  # 発着所
                        Ana_data.loc[
                            (num_right < 1) and (num_left < 1), '17'] = dep_fail_to_signal_not_point
                        Ana_data.loc[(num_right < dep_fail_to_signal_time_val) or (
                                num_left < dep_fail_to_signal_time_val), '18'] = dep_fail_to_signal_time_point
                        Ana_data.loc[(num_right > dep_fail_to_signal_return_val) or (
                                num_left > dep_fail_to_signal_return_val), '19'] = dep_fail_to_signal_return_point
                    else:
                        Ana_data['17'] = np.nan
                        Ana_data['18'] = np.nan
                        Ana_data['19'] = np.nan

                    if c1[0] == A_block_no_cross:  # 交差点
                        if (steering_level_right > 1) or (steering_level_left > 1):
                            Ana_data.loc[(num_right < 1) and (
                                    num_left < 1), '20'] = cross_fail_to_signal_not_point
                            Ana_data.loc[(num_right < cross_fail_to_signal_time_val) or (
                                    num_left < cross_fail_to_signal_time_val), '21'] = cross_fail_to_signal_time_point
                            Ana_data.loc[(num_right > cross_fail_to_signal_return_val) or (
                                    num_left > cross_fail_to_signal_return_val), '22'] = cross_fail_to_signal_return_point

                    else:
                        Ana_data['20'] = np.nan
                        Ana_data['21'] = np.nan
                        Ana_data['22'] = np.nan
                    ana.append(Ana_data)
                Ana_data = pd.concat(ana, ignore_index=True)

                anaaa = Ana_data.loc[:, ['1',
                                         '2',
                                         '3',
                                         '4',
                                         '5',
                                         '6',
                                         '7',
                                         '8',
                                         '9',
                                         '10',
                                         '11',
                                         '12',
                                         '13',
                                         '14',
                                         '15',
                                         '16',
                                         '17',
                                         '18',
                                         '19',
                                         '20',
                                         '21',
                                         '22']].dropna(how="all").index

                anaana = Ana_data.loc[anaaa].sort_values('measurement_date', ascending=[True])

                Ana_data = anaana.reset_index(drop=True)
                def ana_cal(x):
                    category = x[
                        ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17',
                         '18', '19', '20', '21', '22']]
                    x['category'] = category.astype(float).idxmax(axis=1)

                    fill0_point = x['category'].fillna(0)
                    for i in range(0, len(x)):

                        A = fill0_point[i]
                        if A != 0:
                            x.loc[i, 'off_point'] = x.loc[i, A]
                            cat = x.at[i, 'category']
                            if cat == '1' or cat == '2' or cat == '3' or cat == '4' or cat == '8' or cat == '9' or cat == '10' or cat == '13' or cat == '14' or cat == '15' or cat == '16':
                                x.loc[i, 'evaluation_place'] = 0
                            else:
                                x.loc[i, 'evaluation_place'] = x.at[i, 'block_no']

                        else:
                            x.loc[i, 'off_point'] = 0

                            x.loc[i, 'evaluation_place'] = 100

                    return x

                if len(Ana_data) == 0:
                    print('not off point...')
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
                    df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)

                    api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"

                    url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
                    r = requests.get(url)

                    print('not off point analysis done...')

                    return HttpResponse(r.text)
                else :
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
                        var_reverse = df[(df['category'].astype(int) <= 2) & (df['category'].astype(int) >= 1)]
                        var_speed_fast = df[(df['category'].astype(int) <= 4) & (df['category'].astype(int) >= 3)]
                        var_slow_cross = df[(df['category'].astype(int) == 5)]
                        var_slow_top = df[(df['category'].astype(int) == 6)]
                        var_slow_slope = df[(df['category'].astype(int) == 7)]
                        var_speedover = df[(df['category'].astype(int) <= 10) & (df['category'].astype(int) >= 8)]
                        var_speedover_c = df[(df['category'].astype(int) <= 12) & (df['category'].astype(int) >= 11)]
                        var_blake = df[(df['category'].astype(int) == 13)]
                        var_accel = df[(df['category'].astype(int) == 14)]
                        var_handle = df[(df['category'].astype(int) == 15)]
                        var_brake = df[(df['category'].astype(int) == 16)]
                        var_dep_sig = df[(df['category'].astype(int) <= 19) & (df['category'].astype(int) >= 17)]
                        var_cross_sig = df[(df['category'].astype(int) <= 22) & (df['category'].astype(int) >= 20)]
                        if len(var_reverse) > 0:
                            A_1_2 = var_reverse.loc[[var_reverse['category'].astype(int).idxmax()]]
                        else:
                            A_1_2 = pd.DataFrame()
                        if len(var_speed_fast) > 0:
                            A_3_4 = var_speed_fast.loc[[var_speed_fast['category'].astype(int).idxmax()]]
                        else:
                            A_3_4 = pd.DataFrame()
                        if len(var_speedover):
                            A_8_9_10 = var_speedover.loc[[var_speedover['category'].astype(int).idxmax()]]
                        else:
                            A_8_9_10 = pd.DataFrame()
                        if len(var_speedover_c):
                            A_11_12 = var_speedover_c.loc[[var_speedover_c['category'].astype(int).idxmax()]]
                        else:
                            A_11_12 = pd.DataFrame()
                        if len(var_dep_sig):
                            A_17_19 = var_dep_sig.loc[[var_dep_sig['category'].astype(int).idxmax()]]
                        else:
                            A_17_19 = pd.DataFrame()
                        if len(var_cross_sig):
                            A_20_22 = var_cross_sig.loc[[var_cross_sig['category'].astype(int).idxmax()]]
                        else:
                            A_20_22 = pd.DataFrame()
                        sss = pd.concat(
                            [A_1_2, A_3_4, var_slow_cross, var_slow_top, var_slow_slope, var_blake, var_accel, var_handle,
                             var_brake, A_8_9_10, A_11_12, A_17_19, A_20_22])
                        evaluation.append(sss)
                    Ana_data = pd.concat(evaluation, ignore_index=True)

                    """既存データを削除"""
                    with conn2.cursor() as cur:
                        # テーブルを削除する SQL を準備
                        sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')

                        cur.execute(sql, (param_run_start_date,))
                    conn2.commit()
                    id=0
                    id=pd.read_sql('ana_summary', con=engine)['offpoint_detail'].max()+1
                    total_point=Ana_data['off_point'].sum()
                    measurement_date_val=Ana_data.loc[ :,'measurement_date']
                    offpoint_val=Ana_data.loc[ :,'off_point']
                    category_val=Ana_data.loc[ :,'category']
                    evaluation_place_val=Ana_data.loc[ :,'evaluation_place']
                    block_no_val=Ana_data.loc[ :,'block_no']
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
                    api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"
                    url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
                    r = requests.get(url)
                    print(r)
                    print('analysis_output_completely...')
                    return HttpResponse(r.text)


class AnaSummaryViewSet(viewsets.ModelViewSet):
    queryset = AnaSummary.objects.all()  # 全てのデータを取得
    serializer_class = AnaSummarySerializer
    filter_fields = ('equip_id', 'run_start_date')
