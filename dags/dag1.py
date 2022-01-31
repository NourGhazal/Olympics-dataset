import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from extract import Extract
from dataCleaning import DataCleaning
from dataIntegration import DataIntegration
from featureEngineering import FeatureEngineering
from save import Save


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'start_date': datetime(2021, 1, 1),
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'a',
    default_args=default_args,
    # description='A simple hello world DAG 1',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    # tags=['example'],
)


# def _extract(**kwargs):
#     noc_regions = pd.read_csv('~/..//mnt/c/dags/noc_regions.csv')
#     x = noc_regions.head()
#     print(x)

#     noc_regions_json = noc_regions.to_json(orient='index')
#     kwargs['ti'].xcom_push(key = 'noc_regions_key' , value = noc_regions_json)

# def _edit(**kwargs):
#     noc_regions_json = kwargs['ti'].xcom_pull(key = 'noc_regions_key' ,task_ids='extract')
#     noc_regions = pd.read_json(noc_regions_json, orient='index' ,dtype=False )

#     noc_regions_copy = noc_regions.copy()
#     noc_regions_copy.iloc[0].NOC = '555'
#     print('finish edited 5555')
#     print(noc_regions_copy.iloc[0])

#     noc_regions_copy_json = noc_regions_copy.to_json(orient='index')
#     kwargs['ti'].xcom_push(key = 'noc_regions_copy_key' , value = noc_regions_copy_json)

# def _save(**kwargs):
#     noc_regions_copy_json = kwargs['ti'].xcom_pull(key = 'noc_regions_copy_key' ,task_ids='edit')
#     noc_regions_copy = pd.read_json(noc_regions_copy_json, orient='index' ,dtype=False )
#     noc_regions_copy.to_csv("~/..//mnt/c/dags/saved/noc_regions_copy_saved.csv")
# noc_regions_copy.to_excel("saved/noc_regions_copy_saved_medals.xlsx")


extract = PythonOperator(
    task_id='extract',
    python_callable=Extract,
    dag=dag
)

data_cleaning = PythonOperator(
    task_id='data_cleaning',
    python_callable=DataCleaning,
    dag=dag
)

data_integration = PythonOperator(
    task_id='data_integration',
    python_callable=DataIntegration,
    dag=dag
)

feature_engineering = PythonOperator(
    task_id='feature_engineering',
    python_callable=FeatureEngineering,
    dag=dag
)

save = PythonOperator(
    task_id='save',
    python_callable=Save,
    dag=dag
)

# ex = PythonOperator(
#     task_id='extract',
#     python_callable=_extract,
#     dag=dag
# )
# ed = PythonOperator(
#     task_id='edit',
#     python_callable=_edit,
#     dag=dag
# )
# sa = PythonOperator(
#     task_id='save',
#     python_callable=_save,
#     dag=dag
# )

extract >> data_cleaning >> data_integration >> feature_engineering >> save

# ex >> ed >> sa
