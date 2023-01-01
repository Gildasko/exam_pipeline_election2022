
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from nifi_api import collectFile1, collectFile2, updateAttribute1, updateAttribute2, putFile1, putFile2, publishFile1, publishFile2 

with DAG(
    dag_id="election_operator",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:


    collect_task1 = PythonOperator(
        task_id='collect_file_tour1',
        python_callable = collectFile1, 
        dag = dag
    )

    collect_task2 = PythonOperator(
        task_id='collect_file_tour2',
        python_callable = collectFile2, 
        dag = dag
    )

    update_file_name1 = PythonOperator(
        task_id='update_flow_file1',
        python_callable = updateAttribute1,
        dag = dag
    )

    update_file_name2 = PythonOperator(
        task_id='update_flow_file2',
        python_callable = updateAttribute2,
        dag = dag
    )

    put_file1 = PythonOperator(
        task_id='put_file_task1',
        python_callable = putFile1,
        dag = dag
    )

    put_file2 = PythonOperator(
        task_id='put_file_task2',
        python_callable = putFile2,
        dag = dag
    )

    topic1 = PythonOperator(
        task_id='topic_kafka_record1',
        python_callable = publishFile1,
        dag = dag
    )

    topic2 = PythonOperator(
        task_id='topic_kafka_record2',
        python_callable = publishFile2,
        dag = dag
    )

    data_transform_tour1 = SparkSubmitOperator(
        conn_id='spark_connect',
        task_id='spark_tour1',
        application ='/home/gildas/Bureau/ECE/scripts/tour1election22.py',
        verbose=True
        
    )

    data_transform_tour2 = SparkSubmitOperator(
        conn_id='spark_connect',
        task_id='spark_tour2',
        application ='/home/gildas/Bureau/ECE/scripts/tour2election22.py',
        verbose=True
        
    )


    collect_task1 >> update_file_name1 >> [put_file1, topic1] >> data_transform_tour1
    collect_task2 >> update_file_name2 >> [put_file2, topic2] >> data_transform_tour2

   