import sqlalchemy as sa
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import datetime
import time
import io
from joblib import Parallel, delayed
import urllib3
urllib3.disable_warnings()
import os
from dotenv import load_dotenv

load_dotenv()
CONNECTION_STRING=os.getenv('CONNECTION_STRING')

engine = sa.create_engine(CONNECTION_STRING)
connection=engine.connect()

def date_fill(row):
    return pd.to_datetime(datetime.datetime(year=date.year, month=date.month, day=date.day, hour=row['hour']))

def parse_rsv_prices(node_number,date):
    y = date.year
    m = date.month
    d = date.day
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
    # обработка страниц датафрейма по каждому часу суток
    # all_companies = list(lukoil_gen.keys())
    prices_hour = {h: [] for h in range(24)} # словарь с часами
    day_prices = {}
    xls = pd.ExcelFile(fh)

    for h in range(24):
        # формат таблицы до 1 июля 2021г и после отличаются количеством столбцов
        if date >= pd.to_datetime('2021-07-01'):
            result=xls.parse(h, header=2, usecols=[0, 5])
            day_prices[h] = result.set_index('Номер узла').loc[node_number].values[0]
        else:
            result=xls.parse(h, header=2, usecols=[0, 4])
            day_prices[h] = result.set_index('Номер узла').loc[node_number].values[0]
    return day_prices


def update_kvadra_prices():
    request = ("""
                    SELECT date
                    FROM kvadra_prices
                    ORDER BY date DESC
                    LIMIT 1
                    """)
    dates_df = pd.read_sql_query(request,connection)
    end_date_db = pd.to_datetime(dates_df.date.values[0]).date()
    # создание периода дат для закачки
    range_dates = pd.date_range(start=(end_date_db)+datetime.timedelta(days=1), end=pd.to_datetime(datetime.datetime.now().date())+datetime.timedelta(days=1))
    print('Квадра: ',range_dates)
    if len(range_dates)>0:
        st_name='Орловская ТЭЦ'
        gen_comp="ПАО 'Квадра'"
        node_number=521618
        kvadra_prices={}
        for date in range_dates:
            
            if date> end_date_db:
                try:
                    kvadra_prices[date] = parse_rsv_prices(node_number,date)
                except: continue
        df=pd.melt(pd.DataFrame(kvadra_prices).T,var_name='hour',value_name='price',ignore_index=False)
        df.index=pd.to_datetime(df.index)
        df.index=df.index+pd.to_timedelta(df['hour'].values, unit='h')
        df.sort_index(inplace=True)
        df.drop(columns='hour',inplace=True)
        df.reset_index(inplace=True)
        df.columns=['date','price']
        df['station']=st_name
        df['gen_company']=gen_comp
        df=df[['station','price','gen_company','date']]
        if not df.empty:
            result=df.to_sql('kvadra_prices', con=connection, index=False, if_exists='append')
            print(f'Цены Квадра загружены в базу - {result} строк')
        else: 
            print('Нечего загружать')
            pass
    else: pass

