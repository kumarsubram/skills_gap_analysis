from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello():
    print("🚀 Hello from Airflow 3.0!")
    print("✅ Your setup is working perfectly!")
    return "success"

def say_goodbye():
    print("👋 Goodbye from your demo DAG!")
    print("🎉 Ready for Spark integration!")
    return "complete"

dag = DAG(
    'hello_world_check',
    description='Super simple test DAG',
    start_date=datetime(2025, 6, 15),
    schedule=None,
    catchup=False,
    tags=['test', 'simple']
)

hello_task = PythonOperator(
    task_id='say_hello',
    python_callable=say_hello,
    dag=dag
)

goodbye_task = PythonOperator(
    task_id='say_goodbye',
    python_callable=say_goodbye,
    dag=dag
)

hello_task >> goodbye_task
