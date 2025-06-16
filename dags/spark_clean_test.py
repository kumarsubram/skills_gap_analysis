from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'spark_clean_test',
    description='Clean Spark test without copy commands',
    start_date=datetime(2025, 6, 15),
    schedule=None,
    catchup=False,
    tags=['spark', 'clean']
)

spark_job = BashOperator(
    task_id='run_clean_spark_job',
    bash_command='''
    echo "🚀 Starting clean Spark job..."
    
    /home/airflow/.local/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.executor.memory=512m \
        --conf spark.driver.memory=512m \
        --conf spark.executor.cores=1 \
        --deploy-mode client \
        /opt/airflow/include/spark_jobs/clean_spark_test.py
    
    echo "✅ Job completed!"
    ''',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)