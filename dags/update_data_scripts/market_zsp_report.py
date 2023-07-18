# %%
import sqlalchemy as sa
import pandas as pd
import numpy as np
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
import requests
from bs4 import BeautifulSoup
import datetime
import time
import io
import re
from tqdm import tqdm
import urllib3
import os
urllib3.disable_warnings()

from dotenv import load_dotenv

load_dotenv()
CONNECTION_STRING=os.getenv('CONNECTION_STRING')

engine = sa.create_engine(CONNECTION_STRING)
connection = engine.connect()


def market_zsp_report_download(range_dates, download_access):
    """
    Функция market_zsp_report_download выполняет загрузку в таблицу market_zsp_report БД postgres
    отчет о торгах по ЗСП с сайта АО АТС актуальная информацию.
    Аргументы функции:
        range_dates - период дат, за которые нужно загрузить информацию
        download_access - аргумент, который разрешает загрузку данных
    """
    if download_access:
        try:
            for i in tqdm(range(len(range_dates))):
                date = range_dates[i]  # перебор каждой даты в периоде
                if range_dates[0] - datetime.timedelta(days=1) < pd.to_datetime(date):
                    today = date
                    y = today.year
                    m = today.month
                    d = today.day

                    if m < 10: m = '0' + str(m)
                    if d < 10: d = '0' + str(d)
                    excel_href = ''
                    # парсинг необходимой страницы на сайте АТС с нужной датой
                    url = 'https://www.atsenergo.ru/nreport?rname=trade_zsp&region=eur&rdate={}{}{}'.format(y, m, d)
                    response = requests.get(url, verify=False)
                    soup = BeautifulSoup(response.text, 'lxml')
                    # поиск архива, в котором хранится необходимая информация
                    for a in soup.find_all('a', href=True, title=True):
                        if 'Заархивированный' in a['title']:
                            excel_href = a

                    link_end = str(excel_href).split('"')[1]
                    link_end.replace('amp;', '')
                    link = 'https://www.atsenergo.ru/nreport' + link_end
                    r = requests.get(link, stream=True, verify=False)  # скачивание файла
                    fh = io.BytesIO(r.content)
                    # импорт а датафрейм и обработка
                    report_df = pd.io.excel.read_excel(fh, header=[4, 5])
                    new_columns = [i[0] + ', ' + i[1] if not re.search('Unnamed', i[1]) else i[0] for i in
                                   report_df.columns]
                    report_df.columns = new_columns
                    report_df.fillna(0, inplace=True)
                    report_df['date'] = date
                    report_melt = pd.melt(report_df, id_vars=['ЗСП', 'Час', 'date'],
                                          value_vars=list(report_df.columns[2:-1]), var_name='type')
                    report_melt.columns = ['zsp', 'hour', 'date', 'type', 'value']
                    report_melt = report_melt[['date', 'hour', 'zsp', 'type', 'value']]
                    # загрузка датафрейма в таблицу БД
                    report_melt.to_sql('market_zsp_report', con=connection, index=False, if_exists='append')
        except:
            return 'Something Wrong'


def market_zsp_report_update(download_data=True):
    """
    функция market_zsp_report_update
    выполняет актуализация таблицы market_zsp_report
    """
    # поиск последней скачанной даты в БД
    request = ("""
                SELECT date
                FROM market_zsp_report
                ORDER BY date DESC
                LIMIT 1
                """)
    df = pd.read_sql_query(request, connection)
    end_date_db = pd.to_datetime(df.date.values[0]).date()
    # создание периода дат для закачки
    range_dates = pd.date_range(start=(end_date_db) + datetime.timedelta(days=1),
                                end=pd.to_datetime(datetime.datetime.now().date()))
    # загрузка данных
    print(range_dates)
    market_zsp_report_download(range_dates, download_data)







