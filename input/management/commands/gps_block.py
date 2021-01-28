from django.core.management.base import BaseCommand
from input.views import *
import datetime
import numpy
from djangoProject2.param_setting import *


def in_rect(rect, target):
    a = (rect[0][0], rect[0][1])
    b = (rect[1][0], rect[1][1])
    c = (rect[2][0], rect[2][1])
    d = (rect[3][0], rect[3][1])
    e = (target[0], target[1])

    # 原点から点へのベクトルを求める
    vector_a = numpy.array(a)
    vector_b = numpy.array(b)
    vector_c = numpy.array(c)
    vector_d = numpy.array(d)
    vector_e = numpy.array(e)

    # 点から点へのベクトルを求める
    vector_ab = vector_b - vector_a
    vector_ae = vector_e - vector_a
    vector_bc = vector_c - vector_b
    vector_be = vector_e - vector_b
    vector_cd = vector_d - vector_c
    vector_ce = vector_e - vector_c
    vector_da = vector_a - vector_d
    vector_de = vector_e - vector_d

    # 外積を求める
    vector_cross_ab_ae = numpy.cross(vector_ab, vector_ae)
    vector_cross_bc_be = numpy.cross(vector_bc, vector_be)
    vector_cross_cd_ce = numpy.cross(vector_cd, vector_ce)
    vector_cross_da_de = numpy.cross(vector_da, vector_de)

    return vector_cross_ab_ae < 0 and vector_cross_bc_be < 0 and vector_cross_cd_ce < 0 and vector_cross_da_de < 0


class Command(BaseCommand):

    def handle(self, *args, **options):

        global df_result2,result, df_result1, conn2, Df_result1
        engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
        LocationDf = read_frame(LocationTbl.objects.filter(equip_id=A_equip_id))
        gps_RSD = LocationDf.drop_duplicates(subset='run_start_date')
        gps_cal = pd.merge(gps_RSD, pd.read_sql('gps_block', con=engine).drop_duplicates(subset='run_start_date'),
                           on='run_start_date', how='outer', indicator=True)
        gps_time = gps_cal[gps_cal['_merge'] == 'left_only']['run_start_date']
        if(len(gps_time)==0):
            print('未振り分けデータなし')

        for run_start_date in gps_time:

            print(run_start_date)
            gps_data = read_frame(LocationTbl.objects.filter(run_start_date=run_start_date).filter(equip_id=A_equip_id))

            """course情報取得"""
            ms_driving_course_evaluationDf = pd.read_sql("SELECT * FROM ms_driving_course_evaluation",connection_config2)
            course_a = ms_driving_course_evaluationDf[
                ms_driving_course_evaluationDf['driving_course_id'] == A_driving_course_id].reset_index()
            course = course_a.loc[:, ['evaluation_block_code',
                                      'leftup_longitude',
                                      'rightup_latitude',
                                      'rightup_longitude',
                                      'leftdown_latitude',
                                      'leftup_latitude',
                                      'leftdown_longitude',
                                      'rightdown_latitude',
                                      'rightdown_longitude']]
            print('解析中...')
            if len(gps_data) == 0:
                print('not_gps_data')
                print('block振り分け失敗')
            else:
                for s in range(0, len(gps_data)):
                    gps_data.loc[s, 'driving_course_id'] = 6

                    for n in range(0, len(course) - 1):

                        p1 = float(gps_data.at[s, 'latitude'])
                        p2 = float(gps_data.at[s, 'longitude'])
                        nwla = float(course.at[n, 'leftup_latitude'])
                        nwlo = float(course.at[n, 'leftup_longitude'])
                        nela = float(course.at[n, 'rightup_latitude'])
                        nelo = float(course.at[n, 'rightup_longitude'])
                        swla = float(course.at[n, 'leftdown_latitude'])
                        swlo = float(course.at[n, 'leftdown_longitude'])
                        sela = float(course.at[n, 'rightdown_latitude'])
                        selo = float(course.at[n, 'rightdown_longitude'])
                        rect = [[nwla, nwlo], [nela, nelo], [swla, swlo], [sela, selo]]
                        point = [p1, p2]
                        A = in_rect(rect, point)
                        if A == True:
                            gps_data.loc[s, 'block_no'] = course.at[n, 'evaluation_block_code']
                            gps_data.loc[s, 'update_time'] = datetime.date.today()
                        else:
                            gps_data.loc[s, 'block_no'] = 100
                            gps_data.loc[s, 'update_time'] = datetime.date.today()
                    print(gps_data)
                    df_result2 = gps_data.loc[:, ['equip_id',
                                                  'measurement_date',
                                                  'run_start_date',
                                                  'latitude',
                                                  'velocity',
                                                  'block_no',
                                                  'longitude',
                                                  'driving_course_id',
                                                  'update_time']]
                df_result2.to_sql('gps_block', con=engine, if_exists='append', index=False)
                print('block振り分け終了,走行データ評価開始')

            print('start_processing......')
            Model = location_df(model_run_start_date, model_equip_id)
            param_run_start_date = run_start_date
            param_equip_id = A_equip_id

            status = status_df(param_equip_id)
            engine = create_engine(
                'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
            id = 0
            id = pd.read_sql('ana_summary', con=engine)['offpoint_detail'].max() + 1

            Result = 0
            t = pd.read_sql('ana_summary', con=engine).drop_duplicates(subset='run_start_date')
            data = t[t['run_start_date'] == param_run_start_date]
            q = len(data[data['result'].astype(int) > 0])
            s = len(data[data['comment'].astype(int) > 0])
            exist = len(data)

            if (exist != 0) & (q == 0) & (s == 0):  # detail⇒すべて表示
                api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"

                url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date)
                r = requests.get(url)
                print('exist_data...')

            else:
                if status == 0:  # status error⇒detail無し
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

                    url = api.format(host=apihost, equip_id=param_equip_id, run_start_date=param_run_start_date)
                    r = requests.get(url)

                    print('not_send_completely...')


                else:

                    Satelite_data = SatelliteDfn(param_run_start_date, param_equip_id)
                    measurement = location_df(param_run_start_date, param_equip_id)
                    model = location_df(model_run_start_date, model_equip_id)

                    location_data = gps_location_data(measurement, model)

                    if (len(Satelite_data) == 0) | (len(location_data) == 0):
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

                        api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date={run_start_date}"

                        url = api.format(host=apihost, equip_id=param_equip_id,
                                         run_start_date=param_run_start_date)
                        r = requests.get(url)

                        print('not a lot of...gps data...')


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

                        if Ana_data['used_satellites'].astype(float).min() < 10:
                            comment = comment + 1000
                            print('not a lot of used satellites...')
                        if Ana_data['positioning_quality'].astype(float).min() < 4:
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
                            (can_speed > speedover_s_val) | (
                                        velocity > speedover_s_val), '8'] = speedover_s_point  # 速度超過小
                        Ana_data.loc[
                            (can_speed > speedover_m_val) | (
                                        velocity > speedover_m_val), '9'] = speedover_m_point  # 速度超過中
                        Ana_data.loc[
                            (can_speed > speedover_l_val) | (
                                        velocity > speedover_l_val), '10'] = speedover_l_point  # 速度超過大
                        """速度速超過(カーブ）"""
                        Ana_data.loc[
                            (axis_y.abs() > speedover_c_s_val) & (
                                    block_no == A_block_no_curve), '11'] = speedover_c_s_point
                        Ana_data.loc[(axis_y.abs() > speedover_c_m_val) & (
                                block_no == A_block_no_curve), '12'] = speedover_c_m_point
                        """制動操作不良"""
                        Ana_data.loc[
                            (axis_x.abs() > blake_uneven_val) & (
                                        blake_sw == blake_sw_on_val), '13'] = blake_uneven_point
                        """アクセルむら"""
                        Ana_data.loc[
                            (axis_x.abs() > accel_uneven_val) & (accel > accel_on_val), '14'] = accel_uneven_point
                        """急ハンドル"""
                        Ana_data.loc[(axis_y.abs() >= sudden_handle_val) & (
                                (steering_level > level_right) | (
                                    steering_level > level_left)), '15'] = sudden_handle_point
                        """急ブレーキ禁止違反"""
                        Ana_data.loc[(axis_x.abs() > sudden_brake_val) & (blake_sw == blake_sw_on_val) & (
                                can_speed < slowly_speed), '16'] = sudden_brake_point
                        """合図不履行"""
                        ana = []

                        for c1, sdf in Ana_data.groupby(['block_no', 'time']):

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
                                ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',
                                 '17',
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
                                {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date],
                                 'result': [Result],
                                 'total_offpoint': '0', 'offpoint_detail': [id],
                                 'comment': [comment]})

                            with conn2.cursor() as cur:
                                # テーブルを削除する SQL を準備
                                sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
                                cur.execute(sql, (param_run_start_date,))
                            conn2.commit()
                            df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)

                            api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"

                            url = api.format(host=apihost, equip_id=param_equip_id,
                                             run_start_date=param_run_start_date)
                            r = requests.get(url)

                            print('not off point analysis done...')

                        else:
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
                                var_speed_fast = df[
                                    (df['category'].astype(int) <= 4) & (df['category'].astype(int) >= 3)]
                                var_slow_cross = df[(df['category'].astype(int) == 5)]
                                var_slow_top = df[(df['category'].astype(int) == 6)]
                                var_slow_slope = df[(df['category'].astype(int) == 7)]
                                var_speedover = df[
                                    (df['category'].astype(int) <= 10) & (df['category'].astype(int) >= 8)]
                                var_speedover_c = df[
                                    (df['category'].astype(int) <= 12) & (df['category'].astype(int) >= 11)]
                                var_blake = df[(df['category'].astype(int) == 13)]
                                var_accel = df[(df['category'].astype(int) == 14)]
                                var_handle = df[(df['category'].astype(int) == 15)]
                                var_brake = df[(df['category'].astype(int) == 16)]
                                var_dep_sig = df[
                                    (df['category'].astype(int) <= 19) & (df['category'].astype(int) >= 17)]
                                var_cross_sig = df[
                                    (df['category'].astype(int) <= 22) & (df['category'].astype(int) >= 20)]
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
                                    [A_1_2, A_3_4, var_slow_cross, var_slow_top, var_slow_slope, var_blake, var_accel,
                                     var_handle,
                                     var_brake, A_8_9_10, A_11_12, A_17_19, A_20_22])
                                evaluation.append(sss)
                            Ana_data = pd.concat(evaluation, ignore_index=True)

                            """既存データを削除"""
                            with conn2.cursor() as cur:
                                # テーブルを削除する SQL を準備
                                sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')

                                cur.execute(sql, (param_run_start_date,))
                            conn2.commit()
                            id = 0
                            id = pd.read_sql('ana_summary', con=engine)['offpoint_detail'].max() + 1
                            total_point = Ana_data['off_point'].sum()
                            measurement_date_val = Ana_data.loc[:, 'measurement_date']
                            offpoint_val = Ana_data.loc[:, 'off_point']
                            category_val = Ana_data.loc[:, 'category']
                            evaluation_place_val = Ana_data.loc[:, 'evaluation_place']
                            block_no_val = Ana_data.loc[:, 'block_no']
                            Summary_data = pd.DataFrame(
                                {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date],
                                 'result': [Result],
                                 'total_offpoint': [total_point], 'offpoint_detail': [id],
                                 'comment': [comment]})
                            detail_data = pd.DataFrame(
                                {'offpoint_detail_id': id, 'measurement_date': measurement_date_val,
                                 'offpoint': offpoint_val,
                                 'offpoint_category': offpoint_val * 10, 'category_id': category_val,
                                 'evaluation_place': evaluation_place_val, 'block_no': block_no_val,
                                 })
                            Summary_data.to_sql('ana_summary', con=engine, if_exists='append', index=False)
                            detail_data.to_sql('offpoint_detail', con=engine, if_exists='append', index=False)
                            print('analysis_done...')
                            api = "http://{host}:8000/api/anasummary/?equip_id={equip_id}&run_start_date= {run_start_date}"
                            url = api.format(host=apihost, equip_id=param_equip_id,
                                             run_start_date=param_run_start_date)
                            r = requests.get(url)
                            print(r)
                            print('analysis_output_completely...')

            # status = status_df(param_equip_id)
            # engine = create_engine(
            #     'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
            # Result = 0
            # t = pd.read_sql('ana_summary', con=engine).drop_duplicates(subset='run_start_date')
            # q = len(t[t['result'].astype(int) > 0])
            # s = len(t[t['comment'].astype(int) > 0])
            # exist = len(t[t['run_start_date'] == param_run_start_date])
            #
            # if (exist != 0) & (q == 0) & (s == 0):
            #     print('exist_data...')
            # else:
            #
            #     if status == 0:
            #         print('not_send_completely...')
            #
            #         Result = Result + 1
            #         df_result = pd.DataFrame(
            #             {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date],
            #              'result': [Result], 'comment': '100'})
            #         """既存データを削除"""
            #         with conn2.cursor() as cur:
            #             # テーブルを削除する SQL を準備
            #             sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
            #             cur.execute(sql, (param_run_start_date,))
            #         conn2.commit()
            #         df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)
            #
            #     else:
            #         Satelite_data = SatelliteDfn(param_run_start_date, param_equip_id)
            #         measurement = location_df(param_run_start_date, param_equip_id)
            #         location_data = gps_location_data(measurement, Model)
            #         Acceleration_data = AccelerationDfn(param_run_start_date, param_equip_id)
            #         Angularvelocity_data = AngularvelocityDfn(param_run_start_date, param_equip_id)
            #         CanBrake_data = CanBrakeDfn(param_run_start_date, param_equip_id)
            #         CanPosition_data = CanPositionDfn(param_run_start_date, param_equip_id)
            #         CanSpeed_data = CanSpeedDfn(param_run_start_date, param_equip_id)
            #         CanSteering_data = CanSteeringDfn(param_run_start_date, param_equip_id)
            #         CanAccel_data = CanAccelDfn(param_run_start_date, param_equip_id)
            #         if (len(Satelite_data) == 0) | (len(location_data) == 0) | (len(Acceleration_data)) == 0 | (
            #                 len(Angularvelocity_data) == 0) | (len(CanBrake_data) == 0) | (
            #                 len(CanPosition_data) == 0) | (len(CanSpeed_data) == 0) | (len(CanSteering_data) == 0):
            #             print('no_applicable_data.....')
            #         else:
            #             gps_data = pd.merge(Satelite_data, location_data, on='measurement_date', how='outer')
            #             axis_data = pd.merge(Acceleration_data, Angularvelocity_data, on='measurement_date',
            #                                  how='outer')
            #             can1_data = pd.merge(CanBrake_data, CanPosition_data, on='measurement_date', how='outer')
            #             can2_data = pd.merge(CanSpeed_data, CanSteering_data, on='measurement_date', how='outer')
            #             can3_data = pd.merge(can1_data, can2_data, on='measurement_date', how='outer')
            #             can_data = pd.merge(can3_data, CanAccel_data, on='measurement_date', how='outer')
            #             num_gps = len(gps_data)
            #             num_can = len(can_data)
            #             num_axis = len(axis_data)
            #             comment = 0
            #             if num_axis < num_axis_min or num_can < num_can_min:
            #                 comment = comment + 1
            #                 print('not a lot of data..9-axis data or can data')
            #             if ((gps_data['block_no'] == '100').sum()) / len(gps_data) > block_e_percent:
            #                 comment = comment + 10
            #                 print('not a lot of block_data...')
            #             if num_gps < num_gps_min:
            #                 print('not a lot of...gps data...')
            #                 Result = Result + 10
            #                 df_result = pd.DataFrame(
            #                     {'equip_id': [param_equip_id], 'run_start_date': [param_run_start_date],
            #                      'result': [Result],
            #                      'comment': [comment]})
            #                 with conn2.cursor() as cur:
            #                     # テーブルを削除する SQL を準備
            #                     sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
            #                     cur.execute(sql, (param_run_start_date,))
            #                 conn2.commit()
            #                 df_result.to_sql('ana_summary', con=engine, if_exists='append', index=False)
            #
            #             else:
            #                 print('start_analysis...')
            #                 gps_axis_data = pd.merge(gps_data, axis_data, on='measurement_date', how='outer')
            #                 gps_axis_can_data = pd.merge(gps_axis_data, can_data, on='measurement_date',
            #                                              how='outer')
            #                 gps_axis_can_data['measurement_date'] = pd.to_datetime(
            #                     gps_axis_can_data['measurement_date'])
            #                 gps_axis_can_data.sort_values(by=['measurement_date'], inplace=True)
            #                 ana_data_val = gps_axis_can_data.reset_index(drop=True).fillna(method='ffill').drop('index',
            #                                                                                                     axis=1)
            #                 """解析用測定値"""
            #                 inp = ana_data_val['inp_measurement_model'].astype(float)  # 測定値のベクトルとモデル値ベクトルの内積
            #                 VEC = ana_data_val['VEC_measurement_model'].astype(float)  # 測定値のベクトルとモデル値のベクトルの差の大きさ
            #                 vel_dif = ana_data_val['speed_dif_measurement_model'].astype(float)  # 測定値とモデル値の速度差
            #                 model_vel = Model['velocity']  # モデル値の速度
            #                 velocity = ana_data_val['velocity']
            #                 block_no = ana_data_val['block_no']  # 測定値のブロックナンバー
            #                 can_speed = ana_data_val['can_speed'].astype(float)  # 測定値のCANスピード
            #                 axis_y = ana_data_val['nine_axis_acceleration_y'].astype(float)
            #                 axis_x = ana_data_val['nine_axis_acceleration_x'].astype(float)
            #                 blake_sw = ana_data_val['can_brake'].astype(float)
            #                 accel = ana_data_val['can_accel'].astype(float)
            #                 steering_level = ana_data_val['can_steering'].astype(float)
            #
            #                 """逆行"""
            #                 ana_data_val.loc[(inp < 0) & (VEC > reverse_s_val), '1'] = reverse_s_point  # 逆行小
            #                 ana_data_val.loc[(inp < 0) & (VEC > reverse_m_val), '2'] = reverse_m_point  # 逆行大
            #                 """速度早すぎ"""
            #                 ana_data_val.loc[vel_dif > speed_fast_s_val, '3'] = speed_fast_s_point
            #                 ana_data_val.loc[vel_dif >= speed_fast_m_val, '4'] = speed_fast_m_point
            #                 """徐行違反"""
            #                 ana_data_val.loc[(vel_dif > slowly_speed) & (model_vel < slowly_speed) & (
            #                         block_no == A_block_no_cross), '5'] = slow_down_cross_point
            #                 ana_data_val.loc[(vel_dif > slowly_speed) & (model_vel < slowly_speed) & (
            #                         block_no == A_block_no_top), '6'] = slow_down_top_point
            #                 ana_data_val.loc[(vel_dif > slowly_speed) & (model_vel < slowly_speed) & (
            #                         block_no == A_block_no_slope), '7'] = slow_down_slope_point
            #                 """速度超過"""
            #                 ana_data_val.loc[(can_speed > speedover_s_val) | (
            #                         velocity > speedover_s_val), '8'] = speedover_s_point  # 速度超過小
            #                 ana_data_val.loc[(can_speed > speedover_m_val) | (
            #                         velocity > speedover_m_val), '9'] = speedover_m_point  # 速度超過中
            #                 ana_data_val.loc[(can_speed > speedover_l_val) | (
            #                         velocity > speedover_l_val), '10'] = speedover_l_point  # 速度超過大
            #                 """速度速過ぎ(カーブ）"""
            #                 ana_data_val.loc[
            #                     (axis_y.abs() > speedover_c_s_val) & (
            #                             block_no == A_block_no_curve), '11'] = speedover_c_s_point
            #                 ana_data_val.loc[(axis_y.abs() > speedover_c_m_val) & (
            #                         block_no == A_block_no_curve), '12'] = speedover_c_m_point
            #                 """制動操作不良"""
            #                 ana_data_val.loc[
            #                     (axis_x.abs() > blake_uneven_val) & (
            #                             blake_sw == blake_sw_on_val), '13'] = blake_uneven_point
            #                 """アクセルむら"""
            #                 ana_data_val.loc[
            #                     (axis_x.abs() > accel_uneven_val) & (
            #                             accel > accel_on_val), '14'] = accel_uneven_point
            #                 """急ハンドル"""
            #                 ana_data_val.loc[(axis_y.abs() >= sudden_handle_val) & ((steering_level > level_right) | (
            #                         steering_level > level_left)), '15'] = sudden_handle_point
            #                 """急ブレーキ禁止違反"""
            #                 ana_data_val.loc[(axis_x.abs() > sudden_brake_val) & (blake_sw == blake_sw_on_val) & (
            #                         can_speed < slowly_speed), '16'] = sudden_brake_point
            #                 """合図不履行"""
            #                 ana = []
            #                 for c1, sdf in ana_data_val.groupby('block_no'):
            #
            #                     num_right = (sdf['can_turn_lever_position'] == position_right).sum()
            #                     num_left = (sdf['can_turn_lever_position'] == position_left).sum()
            #                     steering_level_right = (sdf['can_steering'] > level_right).sum()
            #                     steering_level_left = (sdf['can_steering'] > level_left).sum()
            #                     if c1 == A_block_no_dep:  # 発着所
            #                         ana_data_val.loc[
            #                             (num_right < 1) and (num_left < 1), '17'] = dep_fail_to_signal_not_point
            #                         ana_data_val.loc[(num_right < dep_fail_to_signal_time_val) or (
            #                                 num_left < dep_fail_to_signal_time_val), '18'] = dep_fail_to_signal_time_point
            #                         ana_data_val.loc[(num_right > dep_fail_to_signal_return_val) or (
            #                                 num_left > dep_fail_to_signal_return_val), '19'] = dep_fail_to_signal_return_point
            #                     else:
            #                         ana_data_val['17'] = np.nan
            #                         ana_data_val['18'] = np.nan
            #                         ana_data_val['19'] = np.nan
            #
            #                     if c1 == A_block_no_cross:  # 交差点
            #                         if (steering_level_right > 1) or (steering_level_left > 1):
            #                             ana_data_val.loc[(num_right < 1) and (
            #                                     num_left < 1), '20'] = cross_fail_to_signal_not_point
            #                             ana_data_val.loc[(num_right < cross_fail_to_signal_time_val) or (
            #                                     num_left < cross_fail_to_signal_time_val), '21'] = cross_fail_to_signal_time_point
            #                             ana_data_val.loc[(num_right > cross_fail_to_signal_return_val) or (
            #                                     num_left > cross_fail_to_signal_return_val), '22'] = cross_fail_to_signal_return_point
            #                     else:
            #                         ana_data_val['20'] = np.nan
            #                         ana_data_val['21'] = np.nan
            #                         ana_data_val['22'] = np.nan
            #                     ana.append(ana_data_val)
            #                 ana_data_val = pd.concat(ana, ignore_index=True)
            #
            #                 anaaa = ana_data_val.loc[:, ['1',
            #                                              '2',
            #                                              '3',
            #                                              '4',
            #                                              '5',
            #                                              '6',
            #                                              '7',
            #                                              '8',
            #                                              '9',
            #                                              '10',
            #                                              '11',
            #                                              '12',
            #                                              '13',
            #                                              '14',
            #                                              '15',
            #                                              '16',
            #                                              '17',
            #                                              '18',
            #                                              '19',
            #                                              '20',
            #                                              '21',
            #                                              '22']].dropna(how="all").index
            #                 anaana = ana_data_val.loc[anaaa]
            #                 ana_data_val = anaana.reset_index()
            #
            #                 def ana_cal(x):
            #                     category = x[
            #                         ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14',
            #                          '15', '16', '17', '18', '19', '20', '21', '22']]
            #                     x['category'] = category.astype(float).idxmax(axis=1)
            #
            #                     fill0_point = x['category'].fillna(0)
            #
            #                     for i in range(0, len(x)):
            #
            #                         val = fill0_point[i]
            #                         if val != 0:
            #                             x.loc[i, 'off_point'] = x.loc[i, val]
            #                             cat = x.at[i, 'category']
            #                             if cat == '1' or cat == '2' or cat == '3' or cat == '4' or cat == '8' or cat == '9' or cat == '10' or cat == '13' or cat == '14' or cat == '15' or cat == '16':
            #                                 x.loc[i, 'evaluation_place'] = 0
            #                             else:
            #                                 x.loc[i, 'evaluation_place'] = x.at[i, 'block_no']
            #
            #                         else:
            #                             x.loc[i, 'off_point'] = 0
            #
            #                             x.loc[i, 'evaluation_place'] = 100
            #
            #                     return x
            #
            #                 ana_cal_data = ana_cal(ana_data_val)
            #                 a = ana_cal_data.loc[:, ['equip_id',
            #                                          'run_start_date',
            #                                          'measurement_date',
            #                                          'block_no',
            #                                          'evaluation_place',
            #                                          'category',
            #                                          'off_point']]
            #                 a['result'] = Result
            #                 a['comment'] = comment
            #                 a['sub_category'] = 100
            #                 df_result = a
            #                 evaluation = []
            #                 for c1, sdf in df_result.groupby('block_no'):
            #                     df = sdf.drop_duplicates(subset='category')
            #                     var_reverse = df[
            #                         (df['category'].astype(int) <= 2) & (df['category'].astype(int) >= 1)]
            #                     var_speed_fast = df[
            #                         (df['category'].astype(int) <= 4) & (df['category'].astype(int) >= 3)]
            #                     var_slow_cross = df[(df['category'].astype(int) == 5)]
            #                     var_slow_top = df[(df['category'].astype(int) == 6)]
            #                     var_slow_slope = df[(df['category'].astype(int) == 7)]
            #                     var_speedover = df[
            #                         (df['category'].astype(int) <= 10) & (df['category'].astype(int) >= 8)]
            #                     var_speedover_c = df[
            #                         (df['category'].astype(int) <= 12) & (df['category'].astype(int) >= 11)]
            #                     var_blake = df[(df['category'].astype(int) == 13)]
            #                     var_accel = df[(df['category'].astype(int) == 14)]
            #                     var_handle = df[(df['category'].astype(int) == 15)]
            #                     var_brake = df[(df['category'].astype(int) == 16)]
            #                     var_dep_sig = df[
            #                         (df['category'].astype(int) <= 19) & (df['category'].astype(int) >= 17)]
            #                     var_cross_sig = df[
            #                         (df['category'].astype(int) <= 22) & (df['category'].astype(int) >= 20)]
            #                     if len(var_reverse) > 0:
            #                         A_1_2 = var_reverse.loc[[var_reverse['category'].astype(int).idxmax()]]
            #                     else:
            #                         A_1_2 = pd.DataFrame()
            #                     if len(var_speed_fast) > 0:
            #                         A_3_4 = var_speed_fast.loc[[var_speed_fast['category'].astype(int).idxmax()]]
            #                     else:
            #                         A_3_4 = pd.DataFrame()
            #                     if len(var_speedover):
            #                         A_8_9_10 = var_speedover.loc[[var_speedover['category'].astype(int).idxmax()]]
            #                     else:
            #                         A_8_9_10 = pd.DataFrame()
            #                     if len(var_speedover_c):
            #                         A_11_12 = var_speedover_c.loc[
            #                             [var_speedover_c['category'].astype(int).idxmax()]]
            #                     else:
            #                         A_11_12 = pd.DataFrame()
            #                     if len(var_dep_sig):
            #                         A_17_19 = var_dep_sig.loc[[var_dep_sig['category'].astype(int).idxmax()]]
            #                     else:
            #                         A_17_19 = pd.DataFrame()
            #                     if len(var_cross_sig):
            #                         A_20_22 = var_cross_sig.loc[[var_cross_sig['category'].astype(int).idxmax()]]
            #                     else:
            #                         A_20_22 = pd.DataFrame()
            #                     sss = pd.concat(
            #                         [A_1_2, A_3_4, var_slow_cross, var_slow_top, var_slow_slope, var_blake,
            #                          var_accel, var_handle, var_brake, A_8_9_10, A_11_12, A_17_19, A_20_22])
            #                     evaluation.append(sss)
            #                 ana_data_val = pd.concat(evaluation, ignore_index=True)
            #
            #                 """既存データを削除"""
            #                 with conn2.cursor() as cur:
            #                     # テーブルを削除する SQL を準備
            #                     sql = ('DELETE FROM ana_summary WHERE run_start_date = %s ')
            #
            #                     cur.execute(sql, (param_run_start_date,))
            #                 conn2.commit()
            #                 ana_data_val.to_sql('ana_summary', con=engine, if_exists='append', index=False)
            #                 print('analysis_done...')
