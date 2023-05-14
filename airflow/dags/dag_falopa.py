from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint

def _training_model():
    return randint(1,10)

def _choose_best_model(ti): # El Branch python operator lo que hace es ejecuta una task condicional a algo y otra condicional a que pase otra cosa
    accuracies = ti.xcom_pull(task_ids=[
        'traning_model_A',
        'traning_model_B',
        'traning_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


with DAG(dag_id="hello_world_dag",
         start_date=datetime(2023,4,26),
         schedule_interval="@hourly",
         catchup=False) as dag:
    
    train_model_A = PythonOperator(
        task_id="traning_model_A",
        python_callable=_training_model)
    
    train_model_B = PythonOperator(
        task_id="traning_model_B",
        python_callable=_training_model)
    
    train_model_C = PythonOperator(
        task_id="traning_model_C",
        python_callable=_training_model)

    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )
    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )     
    # Definir las dependencias entre las tasks
    [train_model_A,train_model_B,train_model_C] >> choose_best_model >> [accurate,inaccurate]