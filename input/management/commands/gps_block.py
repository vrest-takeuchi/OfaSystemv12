from django.core.management.base import BaseCommand
from input.views import *
import datetime
import numpy
from djangoProject2.param_setting import *
import time
import matplotlib.pyplot as plt
import numpy as np

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


def main(a, b, c, d, e, f, g, h, i, j):
    n = 1
    x = np.ndarray(n, dtype=float)
    y = np.ndarray(n, dtype=float)
    inside = np.ndarray(n, dtype=bool)

    x = [a]
    y = [b]
    inside[:] = False
    x1, y1 = [], []

    x1.append(c)
    y1.append(d)
    x1.append(e)
    y1.append(f)
    x1.append(g)
    y1.append(h)
    x1.append(i)
    y1.append(j)
    # check the points inside the polygon
    for i, (sx, sy) in enumerate(zip(x, y)):
        inside[i] = inpolygon(sx, sy, x1, y1)
        # True or False

    return inside[i]


def inpolygon(sx, sy, x, y):
    '''
    x[:], y[:]: polygon
    sx, sy: point
    '''
    np = len(x)
    inside = False
    for i1 in range(np):
        i2 = (i1 + 1) % np
        if min(x[i1], x[i2]) < sx < max(x[i1], x[i2]):
            # a = (y[i2]-y[i1])/(x[i2]-x[i1])
            # b = y[i1] - a*x[i1]
            # dy = a*sx+b - sy
            # if dy >= 0:
            if (y[i1] + (y[i2] - y[i1]) / (x[i2] - x[i1]) * (sx - x[i1]) - sy) > 0:
                inside = not inside

    return inside



class Command(BaseCommand):

    def handle(self, *args, **options):
        try:
            while True:
                time.sleep(1)
                print('processing...')

                engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config1))
                list_read = pd.read_sql('evaluation_list', con=engine).drop_duplicates(subset='run_start_date')
                t_trainings = pd.read_sql("SELECT * FROM t_trainings", connection_config2)
                driving_check_a = t_trainings[t_trainings['driving_mode'] == "1"].reset_index()#実車走行データ
                driving_check_b = driving_check_a.loc[:, ['id','car_id','driving_datetime_start','driving_mode','ms_course_id']]

                el_time = pd.to_datetime(driving_check_b['driving_datetime_start'], utc=True)
                dc_cal=driving_check_b.drop('driving_datetime_start', axis=1)
                driving_check_c=pd.concat([dc_cal,el_time],axis=1)
                driving_check=driving_check_c.rename(columns={"driving_datetime_start": "run_start_date"})
                driving_check_list=driving_check.sort_values(by=['run_start_date']).drop_duplicates(subset='run_start_date').drop('id', axis=1)

                list_cal=pd.merge(driving_check_list,list_read, on='run_start_date', how='outer', indicator=True)
                ev_list_a = list_cal[list_cal['_merge'] == 'left_only']
                ev_list_b=ev_list_a.drop(['car_id_y','driving_mode_y','ms_course_id_y','equip_id','driving_course_name','id','_merge'], axis=1)
                ev_list=ev_list_b.rename(columns={"car_id_x": "car_id","driving_mode_x": "driving_mode","ms_course_id_x": "ms_course_id"}).reset_index()
                ms_car = pd.read_sql("SELECT * FROM ms_car", connection_config2)
                car_id_equip_id_a = ms_car.loc[:, ['car_id','equip_id']]
                ms_driving_course = pd.read_sql("SELECT * FROM ms_driving_course", connection_config2)
                ms_driving_course_a = ms_driving_course.loc[:, ['driving_course_id','driving_course_name']]



                for s in range(0, len(ev_list)):
                    ev_list.loc[s, 'equip_id'] = car_id_equip_id_a[car_id_equip_id_a['car_id']==ev_list.at[s,'car_id']]['equip_id'].max()
                    ev_list.loc[s, 'driving_course_name'] = ms_driving_course_a[ms_driving_course_a['driving_course_id'] == ev_list.at[s, 'ms_course_id']]['driving_course_name'].max()



                # print(driving_check)

                # evaluation_list_cai.to_sql('evaluation_list', con=engine, if_exists='append', index=False)




                block_cal=pd.read_sql('gps_block', con=engine).drop_duplicates(subset='run_start_date')
                global df_result2,result, df_result1, conn2, Df_result1
                gps_cal = pd.merge(ev_list, block_cal,on='run_start_date', how='outer', indicator=True)
                evaluation_list = gps_cal[gps_cal['_merge'] == 'left_only']


                if(len(evaluation_list)==0):
                    print('未振り分けデータなし')
                # if pd.isnull(evaluation_list['run_start_date']) or pd.isnull(evaluation_list['ms_course_id']) or pd.isnull(evaluation_list['car_id']):

                for s in range(0, len(evaluation_list)):
                    equip_id=evaluation_list.at[s,'equip_id_x'].max()
                    run_start_date = evaluation_list.at[s, 'run_start_date']
                    course_id=evaluation_list.at[s,'ms_course_id']
                    ei_text=str(equip_id)
                    ci_text=str(course_id)
                    rsd_text=str(run_start_date)

                    if rsd_text == 'NaT' or ei_text == 'NaN' or ci_text == 'NaN':
                        print('t_trainings_data run_start_date or equip_id or course_id is NULL')
                    else:
                        gps_data = read_frame(LocationTbl.objects.filter(run_start_date=run_start_date).filter(equip_id=equip_id))
                        if len(gps_data)==0:
                            print('not_gps_data')
                            ev_list.loc[s, 'result'] = 'err'
                            eval_list = ev_list[ev_list.index == s].drop('index', axis=1)
                            eval_list.to_sql('evaluation_list', con=engine, if_exists='append', index=False)
                        else:
                            """course情報取得"""
                            ms_driving_course_evaluationDf = pd.read_sql("SELECT * FROM ms_driving_course_evaluation",connection_config2)
                            course_a = ms_driving_course_evaluationDf[
                                ms_driving_course_evaluationDf['driving_course_id'] == course_id].reset_index()
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
                            print(run_start_date)

                            for t in range(0, len(gps_data)):
                                gps_data.loc[t, 'driving_course_id'] = course_id


                                for n in range(0, len(course) - 1):

                                    p1 = float(gps_data.at[t, 'latitude'])
                                    p2 = float(gps_data.at[t, 'longitude'])
                                    nwla = float(course.at[n, 'leftup_latitude'])
                                    nwlo = float(course.at[n, 'leftup_longitude'])
                                    nela = float(course.at[n, 'rightup_latitude'])
                                    nelo = float(course.at[n, 'rightup_longitude'])
                                    swla = float(course.at[n, 'leftdown_latitude'])
                                    swlo = float(course.at[n, 'leftdown_longitude'])
                                    sela = float(course.at[n, 'rightdown_latitude'])
                                    selo = float(course.at[n, 'rightdown_longitude'])

                                    B=main(p1,p2,nwla, nwlo,nela,nelo,swla,swlo,sela,selo)

                                    if B == True:
                                        gps_data.loc[t, 'block_no'] = str(course.at[n, 'evaluation_block_code'])
                                        gps_data.loc[t, 'update_time'] = datetime.date.today()
                                        break


                                    else:
                                        gps_data.loc[t, 'block_no'] = 'A0'
                                        gps_data.loc[t, 'update_time'] = datetime.date.today()




                            df_result2 = gps_data.loc[:, ['equip_id','measurement_date','run_start_date','latitude','longitude','velocity','block_no','driving_course_id','update_time']]

                            df_result2.to_sql('gps_block', con=engine, if_exists='append', index=False)
                            ev_list.loc[s, 'result'] = 'done'
                            eval_list = ev_list[ev_list.index == s].drop('index', axis=1)
                            eval_list.to_sql('evaluation_list', con=engine, if_exists='append', index=False)

                            print('block振り分け終了,走行データ評価開始')
                            print('評価解析開始')


                            # some_long_duration_process(A_equip_id,run_start_date)

        except KeyboardInterrupt:

            print('!!FINISH!!')
