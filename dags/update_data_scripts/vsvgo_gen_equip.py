
import sqlalchemy as sa
import pandas as pd
import requests
import datetime

engine = sa.create_engine('postgresql://postgres:494193@192.168.252.36:5432/postgres')
connection=engine.connect()

def get_vsvgo_results(date):
    """
    функция get_vsvgo_results выполняет парсинг сайта БР системного оператора
    возвращает pandas DataFrame с данными генерации по типам на нужную дату.
    """
    response=requests.get(f"http://br.so-ups.ru/webapi/api/CommonInfo/GenEquipOptions?priceZone[]=1&startDate={datetime.datetime.strftime(date,'%Y.%m.%d')}",
                        verify=False)
    df_vsvgo=pd.DataFrame(response.json()[0]['m_Item2'])[['hour','aes_gen','ges_gen','Pmin_tes','P_tes']]
    df_vsvgo['date']=df_vsvgo['hour'].map(lambda x: datetime.datetime(year=date.year,
                                                                    month=date.month,
                                                                    day=date.day,
                                                                    hour=x))
    df_vsvgo.drop(columns='hour',inplace=True)
    # df_vsvgo.set_index('date',inplace=True)
    df_vsvgo=df_vsvgo[['date','aes_gen','ges_gen','Pmin_tes','P_tes']]
    return df_vsvgo

def vsvgo_predict(last_date):
    """
    функция vsvgo_predict создает датафрейм с будущими данными о генерации по типам с сайта БР СО ЕЭС
    
    параметры
    ---------------
    - last_date - последняя дата в таблице с отчетом с сайта АТС по данным РСВ.
    Начиная с этой даты будут парситься данные с сайта СО ЕЭС.
    """
    dates=pd.date_range(start=last_date+datetime.timedelta(days=1),
                        end=datetime.datetime.now().date()+datetime.timedelta(days=1),
                        freq='D')
    df_vsvgo=pd.DataFrame()
    for date in dates:
        try:
            df_vsvgo_day=get_vsvgo_results(date)
            df_vsvgo=pd.concat([df_vsvgo,df_vsvgo_day])
        except:
            break
    return df_vsvgo


def vsvgo_upload_data():
        """
        функция vsvgo_upload_data
        выполняет актуализация таблицы с данными отобранного
        ген. оборудования на ВСВГО
        """
        # поиск последней скачанной даты в БД
        request = ("""
                SELECT date
                FROM vsvgo_gen_equip
                ORDER BY date DESC
                LIMIT 1
                """)
        df = pd.read_sql_query(request,connection)
        end_date_db = pd.to_datetime(df.date.values[0]).date()
        # загрузка данных
        vsvgo_predict(end_date_db).to_sql('vsvgo_gen_equip', con=connection, index=False, if_exists='append')





