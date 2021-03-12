from django_pandas.io import *
from .models import *
from django.http import HttpResponse
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from djangoProject2.param_setting import *
from output.models import AnaSummary, Threshold
from rest_framework import viewsets
from .serializers import AnaSummarySerializer
from datetime import datetime as dt
import requests
import math
from background_task import background
global result
arr = []
engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))


class InportDbClass:
    def AccelerationDfn(self, a, b):
        acceleration_df = read_frame(AccelerationTbl.objects.filter(run_start_date=a).filter(equip_id=b))

        return acceleration_df.loc[:, ['measurement_date',
                                       'nine_axis_acceleration_x',
                                       'nine_axis_acceleration_y',
                                       'nine_axis_acceleration_z']]

    def AngularvelocityDfn(self, a, b):
        angularvelocityDf = read_frame(AngularvelocityTbl.objects.filter(run_start_date=a).filter(equip_id=b))
        return angularvelocityDf.loc[:, ['measurement_date',
                                         'nine_axis_angular_velocity_x',
                                         'nine_axis_angular_velocity_y',
                                         'nine_axis_angular_velocity_z']]

    def CanBrakeDfn(self, a, b):
        canBrakeDf = read_frame(CanBrakeTbl.objects.filter(run_start_date=a).filter(equip_id=b))
        return canBrakeDf.loc[:, ['measurement_date', 'can_brake']]

    def CanPositionDfn(self, a, b):
        canPositionDf = read_frame(CanPositionTbl.objects.filter(run_start_date=a).filter(equip_id=b))
        return canPositionDf.loc[:, ['measurement_date', 'can_turn_lever_position']]

    def CanSpeedDfn(self, a, b):
        canSpeedDf = read_frame(CanSpeedTbl.objects.filter(run_start_date=a).filter(equip_id=b))
        return canSpeedDf.loc[:, ['measurement_date', 'can_speed']]

    def CanSteeringDfn(self, a, b):
        canSteeringDf = read_frame(CanSteeringTbl.objects.filter(run_start_date=a).filter(equip_id=b))
        return canSteeringDf.loc[:, ['measurement_date', 'can_steering']]

    def CanAccelDfn(self, a, b):
        canAccelDf = read_frame(CanAccelTbl.objects.filter(run_start_date=a).filter(equip_id=b))
        return canAccelDf.loc[:, ['measurement_date', 'can_accel']]

    def SatelliteDfn(self, a, b):
        satelliteDf = read_frame(SatelliteTbl.objects.filter(run_start_date=a).filter(equip_id=b))
        return satelliteDf.loc[:, ['measurement_date', 'positioning_quality', 'used_satellites']]

    def LocationDfn(self, a, b):
        locationDf = read_frame(LocationTbl.objects.filter(run_start_date=a).filter(equip_id=b))
        return locationDf.loc[:, ['measurement_date', 'latitude', 'longitude', 'velocity']]


i = InportDbClass()


class GpsProcessingClass:

    def location_df(self, a, b):
        engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
        location_df_val = pd.read_sql('gps_block', con=engine)
        df = location_df_val[(location_df_val["run_start_date"] == a) & (location_df_val["equip_id"] == b)]
        data_location_a = df.sort_values(['id'], ascending=[True])
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
            data = []
            for i, g in data_location.groupby(
                    [(data_location['block_no'] != data_location['block_no'].shift()).cumsum()]):
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

    def continue_count(self, df):
        df['new_value'] = df['inpminus'].fillna('-')
        # 1個シフトした行が違う値なら +1cumsum, 同じ値なら no cumsum
        df['changepoint_cumsum'] = (df['new_value'] != df['new_value'].shift()).cumsum()
        # new_valueとcontinue_cumsumでグルーピング
        df_group = df.groupby(['new_value', 'changepoint_cumsum'])
        mi_df = df.set_index(['new_value', 'changepoint_cumsum'])
        # 各グループに入っている値をカウント & 元データ
        mi_df['continue_count'] = df_group['new_value'].count()
        # インデックスをもどして上げる
        df = mi_df.reset_index()
        return df.drop(['new_value', 'changepoint_cumsum'], axis=1)

    def reverse_check(self, a):
        if len(a) > 0:
            a1 = a.reset_index()
            for s in range(0, len(a) - 1):
                ax = float(a1.at[s, 'longitude'])
                ay = float(a1.at[s, 'latitude'])
                bx = float(a1.at[s + 1, 'longitude'])  # 次の点のベクトル
                by = float(a1.at[s + 1, 'latitude'])  # 次の点のベクトル
                a1.at[s, 'X'] = bx - ax
                a1.at[s, 'Y'] = by - ay
                #
                # a1.at[s, 'VEC_atoa+1'] = round(math.sqrt((bx - ax) ** 2 + (by - ay) ** 2), 6)#a点から次の点のベクトル
                # a1.at[s, 'inp_measurement_model'] = (ax * bx + ay * by)#a点でのベクトル
            for s in range(0, len(a) - 1):
                aX = float(a1.at[s, 'X'])
                aY = float(a1.at[s, 'Y'])
                bX = float(a1.at[s + 1, 'X'])  # 次の点のベクトル
                bY = float(a1.at[s + 1, 'Y'])  # 次の点のベクトル
                a1.at[s, 'VEC_AtoA+1'] = math.sqrt((bX - aX) ** 2 + (bY - aY) ** 2)  # A点から次の点のベクトル
                a1.at[s, 'inpAtoA+1'] = (aX * bX + aY * bY)  # A点から次のベクトルでの内積
                a1.loc[(a1['inpAtoA+1'] < 0), 'inpminus'] = 'minus'
                a1.loc[(a1['inpAtoA+1'] > 0), 'inpminus'] = 'plus'

            return a1

        else:
            a1 = a
            a1['VEC_AtoA+1'] = 'NaN'
            a1['inpAtoA+1'] = 'NaN'
            a1['inpminus'] = 'NaN'
            return a1

    def function_neer_point(self, a, b):
        if (len(b) > 0) & (len(a) > 0):
            a1 = a.reset_index()
            b1 = b.reset_index()
            min_num = min(len(a) - 1, len(b) - 1)

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

    def gps_location_data(self, a, b):

        for c1, sdf in a.groupby(['block_no', 'time']):

            for c2, sdf2 in b.groupby(['block_no', 'time']):
                if c1 == c2:
                    data = self.function_neer_point(sdf, sdf2)
                    x = pd.DataFrame(data)
                    arr.append(x)

        if len(arr) > 0:
            gps_data__ = pd.concat(arr, ignore_index=True)
            gps_data__.sort_values(by=['measurement_date'], inplace=True)

            return gps_data__
        else:
            gps_data__ = self.function_neer_point(a, b)
            gps_data__.sort_values(by=['measurement_date'], inplace=True)

            return gps_data__

    def check(self, a, b):
        location_measurement_data = self.location_df(a, b)  # GPS経度緯度ロード
        location_model_data = self.location_df(model_run_start_date, model_equip_id)
        location_data_meas = self.continue_count(self.reverse_check(location_measurement_data))
        location_data_mode = self.continue_count(self.reverse_check(location_model_data))
        location_data = self.gps_location_data(location_data_meas, location_data_mode, )
        return location_data


gp = GpsProcessingClass()


class Status:
    """status取得"""

    def status_df(self, a):
        try:
            with conn.cursor() as cursor:
                sql = "SELECT equip_id, hex(operation_st), hex(mqtt_st) FROM equip_status_tbl WHERE equip_id = %s"
                cursor.execute(sql, (a,))
                a = pd.DataFrame(cursor.fetchall()).reindex(axis='index')
                if len(a) == 0:
                    status = 1
                else:
                    status = a.at[0, 'hex(operation_st)']

        finally:
            print('')
        return status

    def status_rsd(self, a):
        status_rsd = read_frame(EquipStatusTbl.objects.filter(equip_id=a))

        return status_rsd.at[0, 'run_start_date']


st = Status()


class BuildGpsData:
    def gps_data(self, s, l):
        gps_data = pd.merge(s, l, on='measurement_date', how='outer')
        return gps_data


bg = BuildGpsData()


class Build9axisData:
    def axis_data(self, pr, pe):
        Acceleration_data = i.AccelerationDfn(pr, pe)
        Angularvelocity_data = i.AngularvelocityDfn(pr, pe)
        axis_data = pd.merge(Acceleration_data, Angularvelocity_data, on='measurement_date', how='outer')
        return axis_data


ba = Build9axisData()


class BuildCanData:
    def can_data(self, pr, pe):
        CanBrake_data = i.CanBrakeDfn(pr, pe)
        CanPosition_data = i.CanPositionDfn(pr, pe)
        CanSpeed_data = i.CanSpeedDfn(pr, pe)
        CanSteering_data = i.CanSteeringDfn(pr, pe)
        CanAccel_data = i.CanAccelDfn(pr, pe)
        can1_data = pd.merge(CanBrake_data, CanPosition_data, on='measurement_date', how='outer')
        can2_data = pd.merge(CanSpeed_data, CanSteering_data, on='measurement_date', how='outer')
        can3_data = pd.merge(can1_data, can2_data, on='measurement_date', how='outer')
        can_data = pd.merge(can3_data, CanAccel_data, on='measurement_date', how='outer')
        return can_data


bc = BuildCanData()


class check_gps_block_num:
    def check_block_percent(self, gd, ng):
        if len(gd['block_no']) == 0 & ng != 0:
            percent = 1
            return percent
        elif len(gd['block_no']) != 0 & ng != 0:
            percent = (gd['block_no'] == '100').sum() / ng
            return percent
        elif len(gd['block_no']) == 0 & ng == 0:
            percent = 1
            return percent
        elif len(gd['block_no']) != 0 & ng == 0:
            percent = 1
            return percent


cgb = check_gps_block_num()


class build_Analysis_data:
    def ana_data(self, gd, ad, ca):
        gps_axis_data = pd.merge(gd, ad, on='measurement_date', how='outer')
        gps_axis_can_data = pd.merge(gps_axis_data, ca, on='measurement_date', how='outer')
        gps_axis_can_data['measurement_date'] = pd.to_datetime(gps_axis_can_data['measurement_date'])
        gps_axis_can_data.sort_values(by=['measurement_date'], inplace=True)
        Ana_data = gps_axis_can_data.reset_index(drop=True).fillna(method='ffill').fillna(method='bfill').drop('index',
                                                                                                               axis=1)
        return Ana_data


bald = build_Analysis_data()
val = read_frame(Threshold.objects.all())


class ev_fun(object):
    def __init__(self, data):
        self.data = data

    """安全装置不適"""

    def fn010001(self):
        a = self.data
        a.loc['010001'] = np.nan

    def fn010002(self):
        a = self.data
        a.loc['010002'] = np.nan

    def fn010003(self):
        a = self.data
        a.loc['010003'] = np.nan

    def fn010004(self):
        a = self.data
        a.loc['010003'] = np.nan

    def fn010005(self):
        print('not Implementation')

    def fn010006(self):
        print('not Implementation')

    def fn010007(self):
        print('not Implementation')

    """運転姿勢不良"""

    def fn020001(self):
        print('not Implementation')

    def fn020002(self):
        print('not Implementation')

    def fn020003(self):
        print('not Implementation')

    def fn020004(self):
        print('not Implementation')

    def fn020005(self):
        print('not Implementation')

    def fn020006(self):
        print('not Implementation')

    """アクセルむら"""

    def fn030001(self):
        a = self.data
        axis_x = a['nine_axis_acceleration_x'].astype(float)
        accel = a['can_accel'].astype(float)  # アクセル開度
        block_no = a['block_no']
        ev_cd = '030001'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = float(val2.at[0, 'val2'])
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        a.loc[(axis_x.abs() > value1) & (accel > value2) & (block_no == 'A0'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']

    def fn030003(self):
        print('not Implementation')

    """逆行"""

    def fn050001(self):
        a = self.data
        inp = a['inpAtoA+1'].astype(float)  # 測定値のベクトルの内積
        minus = a['continue_count']
        block_no = a['block_no']
        ev_cd = '050001'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()

        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        """逆行/逆行/小"""
        a.loc[(inp < value1) & (minus > value2) & (block_no == 'A0'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']  # 逆行小
        # a.to_csv("050001.csv")
        # print((inp <value1) & (minus > value2))

    def fn050002(self):
        a = self.data
        inp = a['inpAtoA+1'].astype(float)  # 測定値のベクトルの内積
        minus = a['continue_count']
        block_no = a['block_no']
        ev_cd = '050002'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        """逆行/逆行/中"""
        a.loc[(inp < value1) & (minus > value2) & (block_no == 'A0'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']  # 逆行中

    def fn050003(self):
        a = self.data
        inp = a['inpAtoA+1'].astype(float)  # 測定値のベクトルの内積
        minus = a['continue_count']
        block_no = a['block_no']
        ev_cd = '050003'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        """逆行/逆行/大"""
        a.loc[(inp < value1) & (minus > value2) & (block_no == 'A0'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']  # 逆行大

    """発進手間どり"""

    def fn060001(self):
        print('not Implementation')

    def fn060002(self):
        print('not Implementation')

    """発進不能"""

    def fn070001(self):
        print('not Implementation')

    def fn070002(self):
        print('not Implementation')

    def fn070003(self):
        a = self.data
        ev_cd = '070003'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        sp_av = a['can_speed'].head(value2).astype(float).mean()
        if sp_av < float(value1):
            a.at[value2, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    """速度維持"""

    def fn080101(self):
        print('not Implementation')

    def fn080102(self):
        print('not Implementation')

    def fn080201(self):
        print('not Implementation')

    def fn080202(self):
        print('not Implementation')

    def fn080203(self):
        print('not Implementation')

    """合図不履行"""

    def fn100101(self):  # 発着所/路端/しない

        a = self.data

        block_no = a['block_no']
        ev_cd = '100101'
        d = a.reset_index()

        maxtime = d['time'].max()

        for c1, sdf in d.groupby('time'):

            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            # a.loc[sdf['can_turn_lever_position']>0, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            sdf_reindex = sdf[(sdf['block_no'] == 'B0') | (sdf['block_no'] == 'U0')]
            last_nam = sdf_reindex['index'].max()
            if ((len(sdf_reindex[sdf_reindex['can_turn_lever_position'] == 0]) == 0) & (c1 != maxtime)):
                a.loc[last_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn100102(self):  # 発着所/路端/継続
        a = self.data
        block_no = a['block_no']
        ev_cd = '100102'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = int(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        d = a.reset_index()

        maxtime = d['time'].max()

        for c1, sdf in d.groupby('time'):
            sig_last_av = sdf['can_turn_lever_position'].tail(value1).astype(float).mean()
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            sdf_reindex = sdf[(sdf['block_no'] == 'B0') | (sdf['block_no'] == 'U0')]
            last_nam = sdf_reindex['index'].max()
            if ((len(sdf_reindex[sdf_reindex['can_turn_lever_position'] == 0]) != 0) & (c1 != maxtime) & (
                    sig_last_av == 0)):
                a.loc[last_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn100103(self):  # 発着所/路端/戻し
        a = self.data
        ev_cd = '100103'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = int(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        d = a.reset_index()

        mintime = d['time'].min()

        for c1, sdf in d.groupby('time'):
            sig_fst_av = sdf['can_turn_lever_position'].head(value1).astype(float).mean()
            sigfst = sdf['can_turn_lever_position'].head(1).astype(float).max()
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()

            sdf_reindex = sdf[(sdf['block_no'] == 'B0') | (sdf['block_no'] == 'U0')]
            first_nam = sdf_reindex['index'].min()
            if ((len(sdf_reindex[sdf_reindex['can_turn_lever_position'] == 0]) != 0) & (c1 != mintime) & (
                    sig_fst_av > 0) & (sigfst > 0)):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn100201(self):
        print('not Implementation')

    def fn100202(self):
        print('not Implementation')

    def fn100203(self):
        print('not Implementation')

    def fn100204(self):
        print('not Implementation')

    def fn100301(self):  # 交差点しない
        a = self.data
        ev_cd = '100301'
        d = a.reset_index()
        maxtime = d['time'].max()
        for c1, sdf in d.groupby('time'):
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            # a.loc[sdf['can_turn_lever_position']>0, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            sdf_reindex = sdf[(sdf['block_no'] == 'E0') | (sdf['block_no'] == 'E2') | (sdf['block_no'] == 'E3') | (
                        sdf['block_no'] == 'E4') | (sdf['block_no'] == 'F0') | (sdf['block_no'] == 'F2') | (
                                          sdf['block_no'] == 'F3') | (sdf['block_no'] == 'F4')]
            last_nam = sdf_reindex['index'].max()
            if ((len(sdf_reindex[sdf_reindex['can_turn_lever_position'] == 0]) == 0) & (c1 != maxtime)):
                a.loc[last_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn100302(self):
        a = self.data
        block_no = a['block_no']
        ev_cd = '100302'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = int(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        d = a.reset_index()

        maxtime = d['time'].max()

        for c1, sdf in d.groupby('time'):
            sig_last_av = sdf['can_turn_lever_position'].tail(value1).astype(float).mean()
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            sdf_reindex = sdf[(sdf['block_no'] == 'E0') | (sdf['block_no'] == 'E2') | (sdf['block_no'] == 'E3') | (
                        sdf['block_no'] == 'E4') | (sdf['block_no'] == 'F0') | (sdf['block_no'] == 'F2') | (
                                          sdf['block_no'] == 'F3') | (sdf['block_no'] == 'F4')]
            last_nam = sdf_reindex['index'].max()
            if ((len(sdf_reindex[sdf_reindex['can_turn_lever_position'] == 0]) != 0) & (c1 != maxtime) & (
                    sig_last_av == 0)):
                a.loc[last_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn100303(self):
        a = self.data
        ev_cd = '100303'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = int(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        d = a.reset_index()
        mintime = d['time'].min()
        for c1, sdf in d.groupby('time'):
            sig_fst_av = sdf['can_turn_lever_position'].head(value1).astype(float).mean()
            sigfst = sdf['can_turn_lever_position'].head(1).astype(float).max()
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            sdf_reindex = sdf[(sdf['block_no'] == 'E0') | (sdf['block_no'] == 'E2') | (sdf['block_no'] == 'E3') | (
                        sdf['block_no'] == 'E4') | (sdf['block_no'] == 'F0') | (sdf['block_no'] == 'F2') | (
                                          sdf['block_no'] == 'F3') | (sdf['block_no'] == 'F4')]
            first_nam = sdf_reindex['index'].min()
            if ((len(sdf_reindex[sdf_reindex['can_turn_lever_position'] == 0]) != 0) & (c1 != mintime) & (
                    sig_fst_av > 0) & (sigfst > 0)):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn100304(self):
        a = self.data
        ev_cd = '100304'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = int(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        d = a.reset_index()
        for c1, sdf in d.groupby('time'):
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            sdf_reindex = sdf[(sdf['block_no'] != 'A0') & (sdf['block_no'] != 'B0') & (sdf['block_no'] != 'U0') & (
                        sdf['block_no'] != 'E0') & (
                                      sdf['block_no'] != 'E2') & (sdf['block_no'] != 'E3') & (
                                          sdf['block_no'] != 'E4') & (
                                      sdf['block_no'] != 'F0') & (sdf['block_no'] != 'F2') & (
                                      sdf['block_no'] != 'F3') & (sdf['block_no'] != 'F4') & (
                                          sdf['block_no'] != 'N0')  & (sdf['block_no'] != 'N1') & (sdf['block_no'] != 'N2') & (sdf['block_no'] != 'J0') & (
                                          sdf['block_no'] != 'C0') & (sdf['block_no'] != 'I0') & (sdf['block_no'] != 'D0') & (sdf['block_no'] != 'L0')& (sdf['block_no'] != 'K0')]
            last_nam = sdf_reindex['index'].max()
            first_nam = sdf_reindex['index'].min()

            if ((len(sdf_reindex[sdf_reindex['can_turn_lever_position'] == 0]) != 0)):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
        for c1, sdf in d.groupby('block_no'):

            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[
                driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            time_val = sdf['time'].astype(int).max() + 1
            valueval1 = a[a['time'] == time_val]
            sdf_reindex = sdf[(sdf['block_no'] == 'N0')]
            first_nam = sdf_reindex['index'].max()
            p_val = valueval1.reset_index()
            c2 = 0

            if len(p_val) > 1:
                c2 = p_val.at[0, 'block_no']

            sth_av = sdf['can_steering'].head(value1).mean()
            stl_av = sdf['can_steering'].tail(value2).mean()

            if (c1 == 'C0') & (c2 == 'E0') & (sth_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'E2') & (sth_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'E3') & (sth_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'E4') & (sth_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F0') & (sth_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F2') & (sth_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F3') & (sth_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F4') & (sth_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

            if (c1 == 'C0') & (c2 == 'E0') & (stl_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'E2') & (stl_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'E3') & (stl_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'E4') & (stl_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F0') & (stl_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F2') & (stl_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F3') & (stl_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F4') & (stl_av == 0):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn100401(self):
        print('not Implementation')

    def fn100402(self):
        print('not Implementation')

    def fn100403(self):
        print('not Implementation')

    def fn100404(self):
        print('not Implementation')

    """安全不確認"""

    def fn110001(self):
        print('not Implementation')

    def fn110002(self):
        print('not Implementation')

    def fn110003(self):
        print('not Implementation')

    def fn110004(self):
        print('not Implementation')

    def fn110005(self):
        print('not Implementation')

    def fn110006(self):
        print('not Implementation')

    def fn110007(self):
        print('not Implementation')

    def fn110008(self):
        print('not Implementation')

    def fn110009(self):
        print('not Implementation')

    def fn110010(self):
        print('not Implementation')

    """ブレーキ制動操作不良"""

    def fn130001(self):
        print('not Implementation')

    def fn130002(self):
        a = self.data
        ev_cd = '130002'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        value3 = float(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        can_speed = a['can_speed'].astype(float)
        block_no = a['block_no']
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        axis_x = a['nine_axis_acceleration_x'].astype(float)
        blake_sw = a['can_brake'].astype(float)  # ブレーキスイッチ
        """制動操作不良/ブレーキ/断"""
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'E0'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'E1'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'E3'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'E4'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'F0'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'F1'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'F2'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'F3'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'F4'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2) & (can_speed == value3) & (block_no == 'L0'), ev_cd] = \
        driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn130003(self):
        print('not Implementation')

    def fn130004(self):
        print('not Implementation')

    def fn130005(self):
        a = self.data
        ev_cd = '130005'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        axis_x = a['nine_axis_acceleration_x'].astype(float)
        blake_sw = a['can_brake'].astype(float)  # ブレーキスイッチ
        """制動操作不良/ブレーキ/不円滑"""
        a.loc[(axis_x.abs() > value1) & (blake_sw == value2), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']

    def fn130006(self):
        print('not Implementation')

    """速度速すぎ"""

    def fn140101(self):
        a = self.data
        ev_cd = '140101'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        vel_dif = a['speed_dif_measurement_model'].astype(float)
        block_no = a['block_no']
        """速度早すぎ/速度（小）/速い"""
        a.loc[(vel_dif > value1) & (block_no == 'A0'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn140102(self):
        a = self.data
        ev_cd = '140102'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        axis_y = a['nine_axis_acceleration_y'].astype(float)
        block_no = a['block_no']
        """速度速すぎ(カーブ）"""
        a.loc[(axis_y.abs() > value1) & (block_no == 'D0'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn140201(self):
        a = self.data
        ev_cd = '140201'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        vel_dif = a['speed_dif_measurement_model'].astype(float)
        block_no = a['block_no']
        """速度早すぎ/速度（大）/速い"""
        a.loc[(vel_dif >= value1) & (block_no == 'A0'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn140202(self):
        a = self.data
        ev_cd = '140202'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].max()
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        axis_y = a['nine_axis_acceleration_y'].astype(float)
        block_no = a['block_no']
        """速度速すぎ(カーブ）"""
        a.loc[(axis_y.abs() > value1) & (block_no == 'D0'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn140203(self):
        print('not Implementation')

    """急停止区間"""

    def fn150000(self):
        print('not Implementation')

    def fn170001(self):
        print('not Implementation')

    def fn170002(self):
        print('not Implementation')

    """急ハンドル"""

    def fn180000(self):
        a = self.data
        ev_cd = '180000'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        axis_y = a['nine_axis_acceleration_y'].astype(float)
        steering_level = a['can_steering'].astype(float).fillna(0)
        block_no = a['block_no']
        """急ハンドル/急ハンドル"""
        a.loc[(steering_level.astype(int) > 0) & (axis_y.abs() > value1) & (steering_level.astype(int) > value2) & (
                    block_no != 'D0'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(steering_level.astype(int) < 0) & (axis_y.abs() > value1) & (steering_level.astype(int) < value3) & (
                    block_no != 'D0'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    """ふらつき"""

    def fn190101(self):
        print('not Implementation')

    def fn190102(self):
        print('not Implementation')

    def fn190103(self):
        print('not Implementation')

    def fn190201(self):
        print('not Implementation')

    def fn190202(self):
        print('not Implementation')

    def fn190203(self):
        print('not Implementation')

    """通過不能"""

    def fn200001(self):
        print('not Implementation')

    def fn200002(self):
        print('not Implementation')

    def fn200003(self):
        print('not Implementation')

    """停止位置不適"""

    def fn210001(self):
        print('not Implementation')

    def fn210002(self):
        print('not Implementation')

    def fn210003(self):
        print('not Implementation')

    def fn210004(self):
        print('not Implementation')

    """巻き込み防止不適"""

    def fn220001(self):
        print('not Implementation')

    def fn220002(self):
        print('not Implementation')

    """側方間隔不保持"""

    def fn230001(self):
        print('not Implementation')

    def fn230002(self):
        print('not Implementation')

    def fn230003(self):
        print('not Implementation')

    """脱輪"""

    def fn240001(self):
        a = self.data
        ev_cd = '240001'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        # value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        # value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        axis_z = a['nine_axis_acceleration_z'].astype(float)
        block_no = a['block_no']
        """脱輪/小/脱輪（小）"""
        a.loc[(axis_z.abs() > value1) & (block_no == 'N0'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_z.abs() > value1) & (block_no == 'N1'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
        a.loc[(axis_z.abs() > value1) & (block_no == 'N2'), ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn240002(self):
        print('not Implementation')

    def fn240003(self):
        print('not Implementation')

    """接触"""

    def fn250001(self):
        print('not Implementation')

    def fn250002(self):
        print('not Implementation')

    def fn250004(self):
        print('not Implementation')

    """後方間隔不良"""

    def fn260000(self):
        print('not Implementation')

    """路側帯進入"""

    def fn270001(self):
        print('not Implementation')

    def fn270002(self):
        print('not Implementation')

    """通行帯違反"""

    def fn280001(self):
        print('not Implementation')

    def fn280002(self):
        print('not Implementation')

    def fn280003(self):
        print('not Implementation')

    def fn280004(self):
        print('not Implementation')

    def fn280005(self):
        print('not Implementation')

    """追いつかれ義務違反"""

    def fn290001(self):
        print('not Implementation')

    def fn290002(self):
        print('not Implementation')

    """右側通行"""

    def fn320001(self):
        print('not Implementation')

    def fn320002(self):
        print('not Implementation')

    def fn320003(self):
        print('not Implementation')

    def fn320004(self):
        print('not Implementation')

    """安全地帯等進入"""

    def fn330000(self):
        print('not Implementation')

    """進路変更違反"""

    def fn340101(self):
        print('not Implementation')

    def fn340102(self):
        print('not Implementation')

    def fn340103(self):
        print('not Implementation')

    def fn340104(self):  # 右振り

        a = self.data
        ev_cd = '340104'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = val1.at[0, 'val1']


        d = a.reset_index()

        for c1, sdf in d.groupby('block_no'):

            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            time_val = sdf['time'].astype(int).max() + 1
            valueval1 = a[a['time'] == time_val]
            sdf_reindex = sdf[(sdf['block_no'] == 'N0')]
            first_nam = sdf_reindex['index'].max()
            p_val = valueval1.reset_index()
            c2 = 0
            if len(p_val) > 1:
                c2 = p_val.at[0, 'block_no']

            st_av = sdf['can_steering'].head(500).mean()
            print(type(st_av))
            print(type(value1))


            if (c1 == 'C0') & (c2 == 'N1') & (st_av == value1):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn340201(self):
        print('not Implementation')

    def fn340202(self):
        print('not Implementation')

    def fn340203(self):
        print('not Implementation')

    def fn340204(self):
        print('not Implementation')

    def fn340205(self):
        print('not Implementation')

    def fn340206(self):
        a = self.data
        ev_cd = '340206'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        # value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()

        d = a.reset_index()

        for c1, sdf in d.groupby('block_no'):

            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            time_val = sdf['time'].astype(int).max() + 1
            valueval1 = a[a['time'] == time_val]
            sdf_reindex = sdf[(sdf['block_no'] == 'N0')]
            first_nam = sdf_reindex['index'].max()
            p_val = valueval1.reset_index()
            c2 = 0
            if len(p_val) > 1:
                c2 = p_val.at[0, 'block_no']

            st_av = sdf['can_steering'].head(value2).mean()

            if (c1 == 'C0') & (c2 == 'E3') & (st_av < value1):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']
            if (c1 == 'C0') & (c2 == 'F3') & (st_av < value1):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn340207(self):
        print('not Implementation')

    """進路変更禁止違反"""

    def fn350001(self):
        print('not Implementation')

    def fn350002(self):
        print('not Implementation')

    """右左折方法違反"""

    def fn370001(self):
        print('not Implementation')

    def fn370002(self):
        print('not Implementation')

    def fn370003(self):
        print('not Implementation')

    def fn370004(self):
        print('not Implementation')

    """安全進行違反"""

    def fn380001(self):
        print('not Implementation')

    def fn380002(self):
        print('not Implementation')

    """課題不履行"""

    def fn390001(self):
        print('not Implementation')

    def fn390002(self):
        print('not Implementation')

    def fn390003(self):
        print('not Implementation')

    """徐行違反"""

    def fn400001(self):
        print('not Implementation')

    def fn400002(self):
        print('not Implementation')

    def fn400003(self):
        a = self.data
        ev_cd = '400003'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        # value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        vel_dif = a['speed_dif_measurement_model'].astype(float)  # 測定値とモデル値の速度差
        model_vel = model['velocity']  # モデル値の速度
        block_no = a['block_no']
        """'徐行違反/徐行違反/右左折"""
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'E0'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'E2'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'E3'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'E4'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'F0'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'F2'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'F3'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'F4'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']

    def fn400004(self):
        print('not Implementation')

    def fn400005(self):
        print('not Implementation')

    def fn400006(self):
        print('not Implementation')

    def fn400007(self):
        print('not Implementation')

    def fn400008(self):
        print('not Implementation')

    def fn400009(self):
        print('not Implementation')

    def fn400010(self):
        a = self.data
        ev_cd = '400010'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = float(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        # value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        vel_dif = a['speed_dif_measurement_model'].astype(float)  # 測定値とモデル値の速度差
        model_vel = model['velocity']  # モデル値の速度
        block_no = a['block_no']
        """'徐行違反/徐行違反/頂上"""
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'R1'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']

    def fn400011(self):
        a = self.data
        ev_cd = '400011'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        # value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        vel_dif = a['speed_dif_measurement_model'].astype(float)  # 測定値とモデル値の速度差
        model_vel = model['velocity']  # モデル値の速度
        block_no = a['block_no']
        """'徐行違反/徐行違反/坂"""
        a.loc[(vel_dif > value1) & (model_vel < value2) & (block_no == 'R2'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']

    """進行方向別通行違反"""

    def fn410000(self):
        print('not Implementation')

    """交差点進入禁止違反"""

    def fn420003(self):
        print('not Implementation')

    def fn420004(self):
        print('not Implementation')

    """交差点進入禁止違反"""

    def fn430001(self):
        print('not Implementation')

    def fn430002(self):
        print('not Implementation')

    """優先判断不良"""

    def fn440001(self):
        print('not Implementation')

    def fn440002(self):
        print('not Implementation')

    def fn440003(self):
        print('not Implementation')

    """進行妨害"""

    def fn450004(self):
        print('not Implementation')

    def fn450006(self):
        print('not Implementation')

    """横断等禁止違反"""

    def fn460002(self):
        print('not Implementation')

    """一時不停止"""

    def fn470001(self):
        a = self.data
        ev_cd = '470001'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        # value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        block_no = a['block_no']
        d = a.reset_index()
        for c1, sdf in d.groupby('block_no'):
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            can_speed_0 = sdf['can_speed']==value1
            blake_on_0 = sdf['can_brake'] == value2
            sdf_reindex = sdf[(sdf['block_no'] == 'L0')]
            first_nam = sdf_reindex['index'].max()

            """'一時不停止"""
            if (len(can_speed_0) > 0) & (len(blake_on_0) > 0) & (c1 == 'L0'):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn470002(self):
        print('not Implementation')

    """横断者保護違反"""

    def fn490001(self):
        print('not Implementation')

    def fn490002(self):
        print('not Implementation')

    def fn490003(self):
        print('not Implementation')

    """歩行者保護不停止等"""

    def fn500002(self):
        print('not Implementation')

    def fn500004(self):
        print('not Implementation')

    def fn500005(self):
        print('not Implementation')

    def fn500006(self):
        print('not Implementation')

    """安全間隔不保持"""

    def fn510001(self):
        print('not Implementation')

    def fn510002(self):
        print('not Implementation')

    def fn510003(self):
        print('not Implementation')

    """急ブレーキ禁止違反"""

    def fn550000(self):
        print('not Implementation')

    """車間距離不保持"""

    def fn560000(self):
        print('not Implementation')

    """駐停車方法違反"""

    def fn570001(self):
        print('not Implementation')

    def fn570002(self):
        print('not Implementation')

    def fn570003(self):
        print('not Implementation')

    """速度超過"""

    def fn600001(self):
        a = self.data
        ev_cd = '600001'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        # value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
        reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
        can_speed = a['can_speed'].astype(float)  # 測定値のCANスピード
        velocity = a['velocity']
        block_no = a['block_no']
        """'速度超過/速度超過"""
        a.loc[(can_speed > value1) & (block_no == 'C0'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']  # 速度超過
        a.loc[(velocity > value2) & (block_no == 'C0'), ev_cd] = driving_evaluation_item_tbl.at[
            reqd_Index, 'score']  # 速度超過

    def fn600002(self):
        print('not Implementation')

    def fn600003(self):
        print('not Implementation')

    """踏切不停止等"""

    def fn610001(self):
        a = self.data
        ev_cd = '470001'
        val1 = val[val['evaluation_cd'] == ev_cd]['val1'].reset_index()
        value1 = float(val1.at[0, 'val1'])
        val2 = val[val['evaluation_cd'] == ev_cd]['val2'].reset_index()
        value2 = int(val2.at[0, 'val2'])
        val3 = val[val['evaluation_cd'] == ev_cd]['val3'].reset_index()
        # value3 = int(val3.at[0, 'val3'])
        val4 = val[val['evaluation_cd'] == ev_cd]['val4'].max()
        block_no = a['block_no']
        d = a.reset_index()
        for c1, sdf in d.groupby('block_no'):
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            reqd_Index = driving_evaluation_item_tbl[driving_evaluation_item_tbl['evaluation_cd'] == ev_cd].index.max()
            can_speed_0 = sdf['can_speed']==value1
            blake_on_0 = sdf['can_brake'] == value2
            sdf_reindex = sdf[(sdf['block_no'] == 'L0')]
            first_nam = sdf_reindex['index'].max()

            """'一時不停止/踏切"""
            if (len(can_speed_0) > 0) & (len(blake_on_0) > 0) & (c1 == 'K0'):
                a.loc[first_nam, ev_cd] = driving_evaluation_item_tbl.at[reqd_Index, 'score']

    def fn610003(self):
        print('not Implementation')

    """追越し違反"""

    def fn620001(self):
        print('not Implementation')

    def fn620002(self):
        print('not Implementation')

    def fn620003(self):
        print('not Implementation')

    def fn620006(self):
        print('not Implementation')

    def fn620007(self):
        print('not Implementation')

    def fn620008(self):
        print('not Implementation')

    def fn620009(self):
        print('not Implementation')

    def fn620010(self):
        print('not Implementation')

    def fn620011(self):
        print('not Implementation')

    def fn620012(self):
        print('not Implementation')

    """割り込み"""

    def fn630000(self):
        print('not Implementation')

    """安全運転意識"""

    def fn650001(self):
        print('not Implementation')

    def fn650002(self):
        print('not Implementation')

    """駐停車違反"""

    def fn660001(self):
        print('not Implementation')

    def fn660002(self):
        print('not Implementation')

    def fn660003(self):
        print('not Implementation')

    def fn660004(self):
        print('not Implementation')

    def fn660005(self):
        print('not Implementation')

    def fn660006(self):
        print('not Implementation')

    def fn660007(self):
        print('not Implementation')

    def fn660008(self):
        print('not Implementation')

    """駐車違反"""

    def fn670001(self):
        print('not Implementation')

    def fn670005(self):
        print('not Implementation')

    def fn670007(self):
        print('not Implementation')

    """駐車違反"""

    def fn680000(self):
        print('not Implementation')


model = gp.location_df(model_run_start_date, model_equip_id)


@background(queue='queue_name1', schedule=1)
def some_long_duration_process(param_equip_id, rsd_tex):
    param_run_start_date_wotz = dt.strptime(rsd_tex, '%Y-%m-%d %H:%M:%S')
    param_run_start_date = pd.to_datetime(param_run_start_date_wotz, utc=True)
    Satelite_data = i.SatelliteDfn(param_run_start_date, param_equip_id)  # GPS測位数ロード
    location_data = gp.check(param_run_start_date, param_equip_id)
    status_val = st.status_df(param_equip_id)
    status = int(status_val)
    status_rsd_check = st.status_rsd(param_equip_id)
    a = status_rsd_check
    b = param_run_start_date

    global comment, Result, percent
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
    Result = 0
    percent = 0
    send_state_val = val[val['evaluation_cd'] == 'not_send_comp_R'].reset_index()
    send_state = send_state_val.at[0, 'val2']
    id = pd.read_sql('ana_summary', con=engine)['offpoint_detail'].max() + 1
    if (status != 0) & (a == b):  # status error⇒detail無しまたは車載IDエラー
        print('not_send_completely...')
        Result = int(send_state)
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
    gps_num_val = val[val['evaluation_cd'] == 'gps_data_num_err_R'].reset_index()
    gps_num = gps_num_val.at[0, 'val2']
    if (len(Satelite_data) == 0) | (len(location_data) == 0):  # GPSデータが存在しない時、Sateliteかlocationどちらか
        Result = int(gps_num)
        df_result = pd.DataFrame(
            {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date], 'result': [Result],
             'total_offpoint': '0', 'offpoint_detail': [id],
             'comment': 0})
        with conn2.cursor() as cur:
            # テーブルを削除する SQL を準備
            sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
            cur.execute(sql, (param_run_start_date,))
        conn2.commit()
        df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)
        print('not_a_lot_of_gps_data')
    else:
        gps_data = bg.gps_data(Satelite_data, location_data)
        gps_data.to_csv("gpsdata.csv")
        axis_data = ba.axis_data(param_run_start_date, param_equip_id)
        axis_data.to_csv("axisdata.csv")
        can_data = bc.can_data(param_run_start_date, param_equip_id)
        can_data.to_csv("candata.csv")
        num_gps = len(gps_data)
        num_can = len(can_data)
        num_axis = len(axis_data)
        comment = 0
        Result = 0
        percent = cgb.check_block_percent(gps_data, num_gps)
        data_num_err_val = val[val['evaluation_cd'] == 'data_num_err_c'].reset_index()
        data_num_err = data_num_err_val.at[0, 'val2']
        num_axis_min = data_num_err_val.at[0, 'val1']
        num_can_min = data_num_err_val.at[0, 'val3']
        if (num_axis < int(num_axis_min)) | (num_can < int(num_can_min)):
            comment = comment + int(data_num_err)
            print('not a lot of data..9-axis data..can data')

        else:
            print('start_analysis...')
            Ana_data = bald.ana_data(gps_data, axis_data, can_data)
            sate_val = val[val['evaluation_cd'] == 'satelite_num_c'].reset_index()
            satelite_num = sate_val.at[0, 'val1']
            satelite_code = sate_val.at[0, 'val2']

            pq_val = val[val['evaluation_cd'] == 'position_quality_c'].reset_index()
            pq_num = pq_val.at[0, 'val1']
            pq_code = pq_val.at[0, 'val2']

            comment = 0
            if Ana_data['used_satellites'].astype(float).min() < float(satelite_num):
                comment = comment + int(satelite_code)
                print('not a lot of used satellites...')
            if Ana_data['positioning_quality'].astype(float).min() < float(pq_num):
                comment = comment + float(pq_code)
                print('law positioning_quality...')

            mscid = Ana_data.drop_duplicates(subset='block_no').reset_index()
            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            elist = driving_evaluation_item_tbl['evaluation_cd'].tolist()
            Ana_data.to_csv("Ana_data.csv")
            Ana_data = pd.read_csv("Ana_data.csv")
            Ana_data[elist] = np.nan

            dfa = pd.DataFrame([])

            for s in range(0, len(mscid)):
                dfb = pd.DataFrame([])
                No = mscid.at[s, 'block_no']

                engine = create_engine(
                    'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
                evaluation_by_block = pd.read_sql('driving_evaluation_by_block_tbl', con=engine)
                a = evaluation_by_block[evaluation_by_block['block_cd'] == No].reset_index()

                for m in range(0, len(a)):
                    p = ev_fun
                    ll = a.at[m, 'evaluation_cd']
                    fn_name = 'fn' + ll
                    q = fn_name
                    cb = p(Ana_data)
                    result2 = getattr(cb, q)()
                    dfa = dfb.append(Ana_data)

            dfc = dfa.append(Ana_data)

            driving_evaluation_item_tbl = pd.read_sql('driving_evaluation_item_tbl', con=engine)
            elist = driving_evaluation_item_tbl['evaluation_cd'].tolist()
            anaaa = dfc.loc[:, elist].dropna(how="all").index

            anaana = Ana_data.sort_values('measurement_date', ascending=[True])
            # anaana = Ana_data.loc[anaaa]
            Ana_data = anaana.reset_index(drop=True)

            Ana_data.to_csv("test.csv")

            # def dep_sig(self, a):
            #     """合図不履行"""
            #     ana = []
            #
            #     for c1, sdf in a.groupby(['block_no', 'time']):
            #
            #         num_right = (sdf['can_turn_lever_position'] == position_right).sum()
            #         num_left = (sdf['can_turn_lever_position'] == position_left).sum()
            #         # steering_level_right = (sdf['can_steering'].fillna(0).astype(float).astype(int) > level_right).sum()
            #         # steering_level_left = (sdf['can_steering'].fillna(0).astype(float).astype(int).fillna(0) > level_left).sum()
            #         if c1[0] == 'B0':  # 発着所
            #             a.loc[
            #                 (num_right < 1) and (num_left < 1), '100101'] = dep_fail_to_signal_not_point
            #             a.loc[(num_right < dep_fail_to_signal_time_val) or (
            #                     num_left < dep_fail_to_signal_time_val), '100102'] = dep_fail_to_signal_time_point
            #             a.loc[(num_right > dep_fail_to_signal_return_val) or (
            #                     num_left > dep_fail_to_signal_return_val), '100103'] = dep_fail_to_signal_return_point
            #         else:
            #             a['100101'] = np.nan
            #             a['100102'] = np.nan
            #             a['100103'] = np.nan
            #
            #         ana.append(a)
            #     Ana_data = pd.concat(ana, ignore_index=True)
            #     return Ana_data
            # if c1[0] == 'E4':  # 交差点
            #     if (steering_level_right > 1) or (steering_level_left > 1):
            #         Ana_data.loc[(num_right < 1) and (num_left < 1), '100301'] = cross_fail_to_signal_not_point
            #         Ana_data.loc[(num_right < cross_fail_to_signal_time_val) or (
            #                 num_left < cross_fail_to_signal_time_val), '100302'] = cross_fail_to_signal_time_point
            #         Ana_data.loc[(num_right > cross_fail_to_signal_return_val) or (
            #                 num_left > cross_fail_to_signal_return_val), '100303'] = cross_fail_to_signal_return_point
            # else:
            #     Ana_data['100301'] = np.nan
            #     Ana_data['100302'] = np.nan
            #     Ana_data['100303'] = np.nan
            # anaana = Ana_data.loc[anaaa].sort_values('measurement_date', ascending=[True])
            # Ana_data = anaana.reset_index(drop=True)

            def ana_cal(x):
                category = x[elist]
                x['category'] = category.astype(float).idxmax(axis=1)
                # print(category.astype(float).idxmax(axis=1))
                # t=x.drop(['id','positioning_quality','speed_dif_measurement_model','can_accel','nine_axis_angular_velocity_z','can_brake','can_speed','can_steering','can_turn_lever_position','nine_axis_acceleration_z','nine_axis_angular_velocity_y','nine_axis_angular_velocity_x','nine_axis_acceleration_y','nine_axis_acceleration_x','level_0','vecX','vecY','velocity','inp_measurement_model','update_time','inpAtoA+1','inpminus','VEC_measurement_model','continue_count','VEC','X','Y','VEC_AtoA+1','used_satellites' ,'latitude','longitude','driving_course_id',], axis=1)
                x['off_point'] = category.astype(float).max(axis=1)
                x['evaluation_place'] = x['block_no']
                return x

            if len(Ana_data) == 0:
                print('not off point...')
                Result = 0
                Result = Result
                comment = comment

                df_result = pd.DataFrame(
                    {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date], 'result': [Result],
                     'total_offpoint': '0', 'offpoint_detail': [id],
                     'comment': [comment]})
                with conn2.cursor() as cur:
                    # テーブルを削除する SQL を準備
                    sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
                    cur.execute(sql, (param_run_start_date,))
                conn2.commit()
                engine = create_engine(
                    'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
                df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)
                api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"
                url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
                r = requests.get(url)
                print('not off point analysis done...')
                return HttpResponse(r.text)
            else:
                Result = 0

                ana_cal_data = ana_cal(Ana_data)
                ana_cal_data.to_csv("test_2.csv")

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
                    """アクセルむら"""
                    accel_uneven = df[(df['category'] == '030001')]

                    """逆走小中大"""
                    var_reverse = df[
                        (df['category'] == '050001') | (df['category'] == '050002') | (df['category'] == '050003')]

                    """発進不能"""
                    start_miss = df[(df['category'] == '070003')]

                    """制動操作不良/ブレーキ/不円滑"""
                    blake_uneven = df[(df['category'] == '130005')]

                    """速度速すぎ小大"""
                    var_speed_fast = df[(df['category'] == '140101') | (df['category'] == '140201')]

                    """速度速すぎ(カーブ）小大"""
                    var_speed_fast_c = df[(df['category'] == '140102') | (df['category'] == '140202')]

                    """制動操作不良/ブレーキ/不円滑"""
                    sudden_handle = df[(df['category'] == '180000')]
                    """脱輪(小）"""
                    derailing = df[(df['category'] == '240001')]
                    """徐行違反右左折"""
                    slow_cross = df[(df['category'] == '400003')]
                    """徐行違反上り"""
                    slow_up = df[(df['category'] == '400010')]
                    """徐行違反下り"""
                    slow_down = df[(df['category'] == '400011')]
                    """速度超過"""
                    speedover = df[(df['category'] == '600001')]

                    dep_sig_1 = df[(df['category'] == '100101')]
                    dep_sig_2 = df[(df['category'] == '100102')]
                    dep_sig_3 = df[(df['category'] == '100103')]

                    cross_sig_1 = df[(df['category'] == '100301')]
                    cross_sig_2 = df[(df['category'] == '100302')]
                    cross_sig_3 = df[(df['category'] == '100303')]
                    cross_sig_4 = df[(df['category'] == '100304')]

                    right_miss = df[(df['category'] == '340104')]
                    right_miss_2 = df[(df['category'] == '340206')]

                    stop = df[(df['category'] == '470001')]

                    stop_2 = df[(df['category'] == '610001')]

                    sss = pd.concat(
                        [accel_uneven, start_miss, blake_uneven, sudden_handle, derailing, slow_cross, slow_up,
                         slow_down, speedover, dep_sig_1, dep_sig_2, dep_sig_3, cross_sig_1, cross_sig_2, cross_sig_3,
                         cross_sig_4, right_miss, right_miss_2, stop])
                    evaluation.append(sss)
                Ana_data = pd.concat(evaluation, ignore_index=True)
                Ana_data.to_csv("cal_log.csv")
                """既存データを削除"""
                with conn2.cursor() as cur:
                    # テーブルを削除する SQL を準備
                    sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')

                    cur.execute(sql, (param_run_start_date,))
                conn2.commit()
                engine = create_engine(
                    'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
                total_point = Ana_data['off_point'].sum()
                measurement_date_val = Ana_data.loc[:, 'measurement_date']
                offpoint_val = Ana_data.loc[:, 'off_point']
                category_val = Ana_data.loc[:, 'category']
                evaluation_place_val = Ana_data.loc[:, 'evaluation_place']
                block_no_val = Ana_data.loc[:, 'block_no']
                id = 0
                if len(read_frame(AnaSummary.objects.all())) == 0:
                    id = 1
                else:
                    id = read_frame(AnaSummary.objects.all())['id'].max() + 1

                Summary_data = pd.DataFrame(
                    {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date], 'result': [Result],
                     'total_offpoint': [total_point], 'offpoint_detail': [id],
                     'comment': [comment]})
                detail_data = pd.DataFrame(
                    {'offpoint_detail_id': id, 'measurement_date': measurement_date_val, 'offpoint': offpoint_val,
                     'offpoint_category': offpoint_val * 10, 'category_id': category_val,
                     'evaluation_place': evaluation_place_val, 'block_no': block_no_val,
                     })
                detail_data = detail_data.sort_values('measurement_date', ascending=[True])

                print(detail_data)
                Summary_data.to_sql('ana_summary', con=engine, if_exists='append', index=False)
                detail_data.to_sql('offpoint_detail', con=engine, if_exists='append', index=False)
                print('analysis_done...')
                print('analysis_output_completely...')


def ana_data(request):
    print('start_processing......')
    param_text = request.GET.get('equip_id')
    param_equip_id = int(param_text)
    rsd_tex = request.GET.get('run_start_date')
    param_run_start_date_wotz = dt.strptime(rsd_tex, '%Y-%m-%d %H:%M:%S')
    param_run_start_date = pd.to_datetime(param_run_start_date_wotz, utc=True)
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
    print(param_run_start_date)
    t = pd.read_sql('ana_summary', con=engine).drop_duplicates(subset='run_start_date')
    data = t[t['run_start_date'] == param_run_start_date]
    result_check = len(data[data['result'].astype(int) > 0])
    comment_check = len(data[data['comment'].astype(int) > 0])
    exist = len(data)

    t_trainings = pd.read_sql("SELECT * FROM t_trainings", connection_config2)
    driving_check_a = t_trainings[t_trainings['driving_mode'] == "1"].reset_index()  # 実車走行データ
    driving_check_b = driving_check_a.loc[:, ['id', 'car_id', 'driving_datetime_start', 'driving_mode', 'ms_course_id']]
    el_time = pd.to_datetime(driving_check_b['driving_datetime_start'], utc=True)
    dc_cal = driving_check_b.drop('driving_datetime_start', axis=1)

    driving_check_c = pd.concat([dc_cal, el_time], axis=1)
    driving_check = driving_check_c.rename(columns={"driving_datetime_start": "run_start_date"})
    driving_check_list = driving_check.drop_duplicates(subset='run_start_date').sort_values(by=['run_start_date']).drop(
        'id', axis=1).reset_index()
    ms_car = pd.read_sql("SELECT * FROM ms_car", connection_config2)
    car_id_equip_id_a = ms_car.loc[:, ['car_id', 'equip_id']]

    for s in range(0, len(driving_check_list)):
        driving_check_list.loc[s, 'equip_id'] = \
        car_id_equip_id_a[car_id_equip_id_a['car_id'] == driving_check_list.at[s, 'car_id']]['equip_id'].max()

    el_check = driving_check_list[driving_check_list['run_start_date'] == param_run_start_date]
    print(el_check)
    if len(el_check) == 0:
        api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date={run_start_date}"
        url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
        r = requests.get(url)
        # with conn2.cursor() as cur:
        #     # テーブルを削除する SQL を準備
        #     sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
        #     cur.execute(sql, (param_run_start_date,))
        # conn2.commit()

        print('not_need_evaluation...')

        some_long_duration_process(param_equip_id, rsd_tex)
        if r.text == '[]':
            r = [{"id": np.nan, "equip_id": param_equip_id, "run_start_date": rsd_tex, "result": 2,
                  "total_offpoint": np.nan, "comment": np.nan, "detail": []}]
            return HttpResponse(r)
        else:
            return HttpResponse(r.text)

    if (exist != 0) & (result_check == 0) & (comment_check == 0):  # detail⇒すべて表示
        api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"

        url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
        r = requests.get(url)
        print('exist_data...')

        return HttpResponse(r.text)
    else:
        api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date={run_start_date}"
        url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date_wotz)
        r = requests.get(url)

        print('make_data...')

        some_long_duration_process(param_equip_id, rsd_tex)
        if r.text == '[]':
            r = [{"id": np.nan, "equip_id": param_equip_id, "run_start_date": rsd_tex, "result": 11,
                  "total_offpoint": np.nan, "comment": np.nan, "detail": []}]
            return HttpResponse(r)
        else:
            return HttpResponse(r.text)


class AnaSummaryViewSet(viewsets.ModelViewSet):
    queryset = AnaSummary.objects.all()  # 全てのデータを取得
    serializer_class = AnaSummarySerializer
    filter_fields = ('equip_id', 'run_start_date')
