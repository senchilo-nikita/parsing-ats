from update_data_scripts.prices_RSV_lukoil import lukoil_prices_update
from update_data_scripts.producers_types import producers_types_update
from update_data_scripts.overflows_zsp import overflows_zsp_update
from update_data_scripts.market_zsp_report import market_zsp_report_update
from update_data_scripts.prices_RSV_kvadra import update_kvadra_prices
from update_data_scripts.vsvgo_gen_equip import vsvgo_upload_data
import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


args = {
    'owner': 'airflow',
    "depends_on_past": False,
    "retries": 12,
    "retry_delay": datetime.timedelta(minutes=60)}

dockerops_kwargs = {
    "mount_tmp_dir": False,
    "mounts": [],
    "retries": 1,
    "api_version": "1.30",
    "docker_url": "tcp://docker-socket-proxy:2375", 
    "network_mode": "bridge",
}


with DAG(dag_id='dag_update_data', description='Update data for forecast',
          schedule_interval='30 * * * *',default_args=args,
          start_date=datetime.datetime(2022, 6, 13), catchup=False) as dag:

    lukoil_prices_operator = PythonOperator(task_id='lukoil_prices_task', 
                                            python_callable=lukoil_prices_update,
                                            dag=dag)
    
    kvadra_prices_operator = PythonOperator(task_id='kvadra_prices_task', 
                                            python_callable=update_kvadra_prices, 
                                            dag=dag)
    
    producers_types_operator = PythonOperator(task_id='producers_types_task', 
                                              python_callable=producers_types_update, 
                                              dag=dag)
    
    overflows_zsp_operator = PythonOperator(task_id='overflows_data_task', 
                                            python_callable=overflows_zsp_update, 
                                            dag=dag)
    
    market_report_operator = PythonOperator(task_id='market_report_task', 
                                            python_callable=market_zsp_report_update, 
                                            dag=dag)
    vsvgo_gen_operator = PythonOperator(task_id='vsvgo_gen_task', 
                                            python_callable=vsvgo_upload_data, 
                                            dag=dag)

    lukoil_prices_operator >> kvadra_prices_operator >> vsvgo_gen_operator >> producers_types_operator >> overflows_zsp_operator >> market_report_operator

