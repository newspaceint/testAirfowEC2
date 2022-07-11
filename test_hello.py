from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime



def generateDataFrame(**outputFilename):
    df = pd.DataFrame({"name":["hello","world","1","2","3"]})
    df.to_csv(outputFilename["name"])
    return True

with DAG("main_test1_generateDF",start_date=datetime(2022,7,11),schedule_interval="@daily", catchup=False) as dag:
    generateDF = PythonOperator(task_id="generateDF_process",python_callable=generateDataFrame,op_kwargs={'name': 'file1.csv'})

generateDF