
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

def producers_types_download(range_dates,download_access):
    """
    Функция producers_types_download выполняет загрузку в таблицу producers_types БД postgres 
    отчет плановом почасовом производстве по типам станций с сайта АО АТС.
    Аргументы функции:
        range_dates - период дат, за которые нужно загрузить информацию
        download_access - аргумент, который разрешает загрузку данных
    """    
    if download_access:
        try:
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
                    # парсинг необходимой страницы на сайте АТС с нужной датой
                    url = 'https://www.atsenergo.ru/nreport?rname=trade_zone&region=eur&rdate={}{}{}'.format(y,m,d)
                    response = requests.get(url, verify=False)
                    soup = BeautifulSoup(response.text, 'lxml')
                    # поиск архива, в котором хранится необходимая информация
                    for a in soup.find_all('a', href=True, title=True):
                        if 'Заархивированный' in a['title']:
                            excel_href = a

                    link_end = str(excel_href).split('"')[1]
                    link_end.replace('amp;', '')
                    link = 'https://www.atsenergo.ru/nreport' + link_end
                    # скачивание файла
                    r = requests.get(link, stream=True, verify=False)
                    fh = io.BytesIO(r.content)
                    gen_df = pd.io.excel.read_excel(fh, header=6, usecols=[0,1,2,3])
                    gen_df.columns=['hour','ges','aes','tes']
                    gen_df['date']=date
                    gen_df=gen_df[['date','hour','aes','ges','tes']]
                    # сохраниние в БД
                    gen_df.to_sql('producers_types', con=connection, index=False, if_exists='append')
        except:
            print('Something Wrong')

def producers_types_update(download_data=True):
        """
        функция producers_types_update
        выполняет актуализация таблицы producers_types_report
        """
        # поиск последней скачанной даты в БД
        request = ("""
                SELECT date
                FROM producers_types
                ORDER BY date DESC
                LIMIT 1
                """)

        df = pd.read_sql_query(request,connection)
        end_date_db = pd.to_datetime(df.date.values[0]).date()
        range_dates = pd.date_range(start=(end_date_db)+datetime.timedelta(days=1), end=pd.to_datetime(datetime.datetime.now().date()))
        print(range_dates)
        producers_types_download(range_dates,download_data)




