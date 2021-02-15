from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Spark Hello World"
file_path = "/usr/local/spark/resources/data/airflow.cfg"


default_args = {
    'owner': 'saurabh',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 25),
    'email': ['saurabh.srk15@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    dag_id='data_pipeline_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval="10 5 * * *",
)

EXEC_DATE = '{{ ds }}'

data_pipeline = SparkSubmitOperator(
    task_id='data_pipeline',
    conn_id='spark_default',
    application="/usr/local/spark/app/data_combine.py",
    verbose=1,
    name='data_pipeline',
    dag=dag,
    conf={"spark.master":spark_master},
    application_args=["/usr/data", EXEC_DATE],
)