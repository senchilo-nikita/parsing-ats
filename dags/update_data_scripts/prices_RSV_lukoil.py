import sqlalchemy as sa
import pandas as pd
import numpy as np
import requests

import datetime
import time
import io
from tqdm import tqdm
import urllib3
urllib3.disable_warnings()
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
load_dotenv()
CONNECTION_STRING=os.getenv('CONNECTION_STRING')

engine = sa.create_engine(CONNECTION_STRING)
connection=engine.connect()


gen_id_dict = np.load('/dags/update_data_scripts/data/dict_1.npy',allow_pickle=True).item()
number_names_dict = np.load('/dags/update_data_scripts/data/dict_2.npy',allow_pickle=True).item()
lukoil_gen=np.load('/dags/update_data_scripts/data/lukoil_dict.npy',allow_pickle=True).item()

lukoil_nodes=[]
for comp in lukoil_gen.keys():
    lukoil_nodes+=(list(lukoil_gen[comp].keys()))
lukoil_node_names = {}
for d in list(lukoil_gen.values()):
    lukoil_node_names.update(d)

def number_to_station_name(row):
    number = row['Номер узла']
    try:
        return lukoil_node_names[number]
    except:
        return None

def lukoil_prices_download(range_dates,download_access):
    """
    Функция lukoil_prices_download выполняет загрузку в таблицу lukoil_prices БД postgres 
    отчет о ценах РСВ для станци Лукойла.
    Аргументы функции:
        range_dates - период дат, за которые нужно загрузить информацию
        download_access - аргумент, который разрешает загрузку данных
    """    
    if download_access:
        # try:
        for i in tqdm(range(len(range_dates))):
            date=range_dates[i] # перебор каждой даты в периоде
            all_comp_df = pd.DataFrame()
            if range_dates[0]-datetime.timedelta(days=1) < pd.to_datetime(date):
                today = date
                y = today.year
                m = today.month
                d = today.day

                if m < 10: m = '0' + str(m)
                if d < 10: d = '0' + str(d)
                excel_href = ''
                # парсинг необходимой страницы на сайте АТС с нужной датой
                url = 'https://www.atsenergo.ru/nreport?rname=big_nodes_prices_pub&rdate={}{}{}'.format(y,m, d)
                response = requests.get(url, verify=False)
                soup = BeautifulSoup(response.text, 'lxml')
                # поиск архива, в котором хранится необходимая информация
                for a in soup.find_all('a', href=True, title=True):
                    if 'Заархивированный' in a['title']:
                        excel_href = a

                link_end = str(excel_href).split('"')[1]
                link_end.replace('amp;', '')
                link = 'https://www.atsenergo.ru/nreport' + link_end
                r = requests.get(link, stream=True, verify=False)
                # скачивание файла
                fh = io.BytesIO(r.content)

                def date_fill(row):
                    return pd.to_datetime(datetime.datetime(year=date.year, month=date.month, day=date.day, hour=row['hour']))
                # обработка страниц датафрейма по каждому часу суток
                all_companies = list(lukoil_gen.keys())
                prices_hour = {h: [] for h in range(24)} # словарь с часами
                xls = pd.ExcelFile(fh)
                for h in range(24):
                    # формат таблицы до 1 июля 2021г и после отличаются количеством столбцов
                    if date >= pd.to_datetime('2021-07-01'):
                        prices_df = xls.parse(h, header=2, usecols=[0, 5])
                    else:
                        prices_df = xls.parse(h, header=2, usecols=[0, 4])
                    prices_df['Номер узла'] = prices_df['Номер узла'].astype('str')
                    # сопоставление номера узла с названием станции
                    prices_df['name'] = prices_df.apply(number_to_station_name, axis=1)
                    prices_df.dropna(inplace=True)
                    # заполнение цены для каждой ген компании
                    for gen_comp in all_companies:
                        prices_hour_gen = {}
                        numbers = list(lukoil_gen[gen_comp].keys())
                        prices_gen_comp = prices_df[prices_df['Номер узла'].isin(numbers)]
                        prices_gen_comp_dict = prices_gen_comp.groupby('name')['Цена, руб'].mean().to_dict()
                        prices_hour_gen[gen_comp] = prices_gen_comp_dict
                        prices_hour[h].append(prices_hour_gen)
                    # print(h)
                gen_prices = {}
                
                for gen_name in all_companies:
                    # заполнение цены для каждой станции по ген компании
                    g = all_companies.index(gen_name)
                    for h in range(24):
                        gen_prices[h] = prices_hour[h][g][gen_name]
                    gen_df = pd.DataFrame(gen_prices).T
                    gen_df_for_db = pd.melt(gen_df.reset_index(), id_vars='index',var_name='station', value_name='price')
                    gen_df_for_db.columns = ['hour', 'station', 'price']
                    gen_df_for_db['gen_company'] = gen_name
                    gen_df_for_db['date'] = gen_df_for_db.apply(date_fill, axis=1)
                    gen_df_for_db.drop(columns='hour', inplace=True)
                    all_comp_df = pd.concat([all_comp_df, gen_df_for_db])
                # сохраниние в БД
                all_comp_df.to_sql('lukoil_prices', con=connection, index=False, if_exists='append')
        # except:
        #     print('Something wrong')

# %%
def lukoil_prices_update(download_data=True):
        """
        функция lukoil_prices_update
        выполняет актуализацию таблицы lukoil_prices
        """
        # поиск последней скачанной даты в БД
        request = ("""
                SELECT date
                FROM lukoil_prices
                ORDER BY date DESC
                LIMIT 1
                """)
        df = pd.read_sql_query(request,connection)
        end_date_db = pd.to_datetime(df.date.values[0]).date()
        # создание периода дат для закачки
        range_dates = pd.date_range(start=(end_date_db)+datetime.timedelta(days=1), end=pd.to_datetime(datetime.datetime.now().date()))
        print(range_dates)
        # загрузка данных
        lukoil_prices_download(range_dates,download_data)


