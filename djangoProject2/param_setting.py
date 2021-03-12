import psycopg2
import pymysql.cursors

"""データベース設定※settings.pyのデータベース情報も変更必要"""
conn = pymysql.connect(host='localhost',  # 測定値データホスト
                       user='vrest',  # 測定値データユーザー名
                       password='vrest',  # 測定値データパスワード
                       db='ofa_system',  # 測定値データベース名
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor)
host_temp = 'localhost'  # 一時データホスト
user_temp = 'postgres'  # 一時データユーザー名
password_temp = 'postgres'  # 一時データパスワード
database_temp = 'postgres'  # 一時データデータベース名
port_temp = '5432'  # 一時データポート番号
connection_config2 = psycopg2.connect(host='124.146.154.219',  # MAPデータホスト
                                      dbname='safety_mobility',  # MAPデータベース名
                                      user='syssoft_admin',  # MAPデータユーザー名
                                      password='Sk4gADca')  # MAPデータパスワード
conn2 = psycopg2.connect(host=host_temp,
                         port=port_temp,
                         dbname=database_temp,
                         user=user_temp,
                         password=password_temp)
"""モデルデータ設定"""
model_equip_id = 3  # モデルデータの車体識別番号
model_run_start_date = ''  # モデルデータの走行開始日時
"""その他"""
significant_digits_vec = 6  # 有効数字
apihost = 'localhost'

connection_config1 = {'user': user_temp,
                      'database': database_temp,
                      'password': password_temp,
                      'host': host_temp,
                      'port': port_temp,}
