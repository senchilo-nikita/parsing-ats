
import sqlalchemy as sa
import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
import io
from tqdm import tqdm
import urllib3
urllib3.disable_warnings()
import os

from dotenv import load_dotenv

load_dotenv()
CONNECTION_STRING=os.getenv('CONNECTION_STRING')

engine = sa.create_engine(CONNECTION_STRING)
connection=engine.connect()


def overflows_zsp_download(range_dates,download_access):
    """
    Функция overflows_zsp_download выполняет загрузку в таблицу overflows_zsp БД postgres
    отчет о перетоках между ЗСП с сайта АО АТС актуальная информацию.
    Аргументы функции:
        range_dates - период дат, за которые нужно загрузить информацию
        download_access - аргумент, который разрешает загрузку данных
    """
    try:
        if download_access:
            for i in tqdm(range(len(range_dates))):
                date=range_dates[i]
                if range_dates[0]-datetime.timedelta(days=1) < pd.to_datetime(date):
                    today = date
                    y = today.year
                    m = today.month
                    d = today.day
                    if m < 10: m = '0' + str(m)
                    if d < 10: d = '0' + str(d)
                    excel_href = ''
                    url = 'https://www.atsenergo.ru/nreport?access=public&region=eur&rname=overflow_zsp&rdate={}{}{}'.format(y,m,d)
                    response = requests.get(url, verify=False)
                    soup = BeautifulSoup(response.text, 'lxml')
                    for a in soup.find_all('a', href=True, title=True):
                        if 'Заархивированный' in a['title']:
                            excel_href = a
                    link_end = str(excel_href).split('"')[1]
                    link_end.replace('amp;', '')
                    link = 'https://www.atsenergo.ru/nreport' + link_end
                    r = requests.get(link, stream=True, verify=False)
                    fh = io.BytesIO(r.content)
                    overflows_df = pd.io.excel.read_excel(fh, header=4, usecols=[0,1,2,3])
                    overflows_df.columns=['flow_from','flow_to','hour','volume']
                    overflows_df['date']=date
                    overflows_df=overflows_df[['date','hour','flow_from','flow_to','volume']]
                    overflows_df=overflows_df.iloc[1:]
                    overflows_df.to_sql('overflows_zsp', con=connection, index=False, if_exists='append')
    except:
        print('Something wrong')


def overflows_zsp_update(download_data=True):
        """
        функция overflows_zsp_update
        выполняет актуализация таблицы overflows_zsp_report
        """
        # поиск последней скачанной даты в БД
        request = ("""
                SELECT date
                FROM overflows_zsp
                ORDER BY date DESC
                LIMIT 1
                """)
        df = pd.read_sql_query(request,connection)
        end_date_db = pd.to_datetime(df.date.values[0]).date()
        # создание периода дат для закачки
        range_dates = pd.date_range(start=(end_date_db)+datetime.timedelta(days=1), end=pd.to_datetime(datetime.datetime.now().date()))
        # загрузка данных
        print(range_dates)
        overflows_zsp_download(range_dates,download_data)





