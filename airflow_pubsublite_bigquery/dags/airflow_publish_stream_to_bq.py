"""
Produce stream data to allow the producer and consumer
to pubslish and subscribe from pubsublite and write
it to BQ
"""
import subprocess
from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import (
    BashOperator
)
# from airflow.contrib.operators.spark_submit_operator import(
#     SparkSubmitOperator
# )
from airflow.providers.google.cloud.hooks.dataproc import (
    DataprocHook
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.operators.python_operator import (
    PythonOperator
)
from airflow.operators.python import (
    BranchPythonOperator
)
from airflow.operators.dummy import (
    DummyOperator
)
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from pathlib import Path
import json
from mock_data_scripts.generate_mock_stream_data import run_pipeline
from gcp_config_parameters import *
from pubsublite_config import *

# gcp_info = Variable.get("gcp_info", deserialize_json=True)
# pubsub_info = Variable.get("pubsublite_info", deserialize_json=True)
# cluster_configs = Variable.get("cluster_info", deserialize_json=True)
bucket_folder = "generated_data"
dim_products = f"{bucket_folder}/dim_products.json"
dim_stores = f"{bucket_folder}/dim_stores.json"
parent_path = Path(__file__).resolve().parent
jars = [f"gs://{BUCKET_NAME}/jars/pubsublite-spark-sql-streaming-1.0.0-with-dependencies.jar",
        f"gs://{BUCKET_NAME}/jars/google-cloud-pubsublite-1.9.0.jar"
]

pyspark_producer_main_path = f"gs://{BUCKET_NAME}/pyspark_scripts/pubsublite_pyspark_stream_producer.py"
pyspark_consumer_main_path = f"gs://{BUCKET_NAME}/pyspark_scripts/pubsublite_pyspark_stream_consumer.py"
config_files = [
    f"gs://{BUCKET_NAME}/config_data/gcp_config_parameters.py",
    f"gs://{BUCKET_NAME}/config_data/pubsublite_config.py",
]

def read_cluster_config(**kwargs):
    cfg_path = parent_path = Path(__file__).resolve().parent
    try:
        with open(f"{cfg_path}/dataproc_cluster_config.json", "r") as f:
            cluster_configs = json.load(f)
    except Exception as e:
        raise Exception(f"Dataproc config file missing")
    kwargs["task_instance"].xcom_push(key="cluster_configs", value=cluster_configs)


def check_dataproc_cluster(**kwargs):
    dataproc_hook = DataprocHook(gcp_conn_id='google_cloud_default')
    cluster_configs = kwargs['ti'].xcom_pull(task_ids='load_cluster_config', key='cluster_configs')
    try:
        cluster = dataproc_hook.get_cluster(project_id=PROJECT_ID, 
                                            region=REGION, 
                                            cluster_name=cluster_configs["CLUSTER_NAME"])
    except NotFound:
        return 'create_cluster'
    return 'cluster_running' 


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False
}
dag = DAG(
    'publish_stream_to_bq',
    default_args=default_args,
    description="Task generates streaming data for producer and consumer",
    schedule_interval="@daily",
    start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    render_template_as_native_obj=True,
    tags=["dev"]
)


run_date = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"


generate_stream_data = PythonOperator(
    task_id="generate_stream_data",
    python_callable=run_pipeline,
    provide_context=True,
    dag=dag
)

load_cluster_config = PythonOperator(
    task_id="load_cluster_config",
    python_callable=read_cluster_config,
    provide_context=True,
    dag=dag
)

check_cluster_task = BranchPythonOperator(
    task_id='check_dataproc_cluster',
    python_callable=check_dataproc_cluster,
    provide_context=True,
    dag=dag,
)

# Dummy task to execute if the cluster is running
cluster_running = DummyOperator(
    task_id='cluster_running',
    dag=dag,
)


one_success = DummyOperator(
        task_id='one_task_success',
        dag=dag,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config='{{task_instance.xcom_pull(task_ids="load_cluster_config", key="cluster_configs")["CLUSTER_CONFIG"]}}',
    dag=dag
)


producer_job = DataprocSubmitPySparkJobOperator(
    task_id="producer_job",
    main=pyspark_producer_main_path,
    pyfiles=config_files,
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    dataproc_jars=jars,
    dag=dag
)

consumer_job = DataprocSubmitPySparkJobOperator(
    task_id="consumer_job",
    main=pyspark_consumer_main_path,
    pyfiles=config_files,
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    dataproc_jars=jars,
    dag=dag
)

all_success = DummyOperator(
        task_id='all_task_success',
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    cluster_name="{{task_instance.xcom_pull(task_ids='load_cluster_config', key='cluster_configs')['CLUSTER_NAME']}}",
    project_id=PROJECT_ID,
    region=REGION,
    dag=dag
)

generate_stream_data >> load_cluster_config >> check_cluster_task >> [cluster_running, create_cluster]
[cluster_running, create_cluster] >> one_success >> [consumer_job, producer_job]
[consumer_job, producer_job] >> all_success >> delete_cluster

