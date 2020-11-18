import pandas as pd
import requests
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


args = {
    'owner' : 'airflow',
    'start_date' : days_ago(1)
}


dag = DAG(dag_id='d_dag', default_args=args, schedule_interval=None)

def retrieve_github_data(**context):
    df = pd.DataFrame(columns=['repository_ID', 'name', 'language']) 
    p_num=1
    while p_num <= 9:
        url = f'https://api.github.com/search/repositories?q=created:">2018-12-31"&sort=forks&order=desc&per_page=100&page={p_num}'
        results = requests.get(url).json()
        for repo in results['items']:
            tmp_dic = {'repository_ID': repo['id'],'name': repo['name'],'language': repo['language']}
            df = df.append(tmp_dic, ignore_index=True)
        p_num+=1

    top_languages = df['language'].value_counts()[:5].index.tolist()
    context['ti'].xcom_push(key='top_languages', value=top_languages)

    print('successfully retrieved data')

def print_top_language(**context):
    received_value = context['ti'].xcom_pull(key='top_languages')
    print(f'top 5 repository languages are {str(received_value)}')

with dag:
    task1 = PythonOperator(
        task_id ='t1',
        python_callable = retrieve_github_data,
        provide_context = True,
        retries = 2
    )

    task2 = PythonOperator(
        task_id='t2',
        python_callable = print_top_language,
        provide_context = True
    )


    task1 >> task2