from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from random import uniform
from datetime import datetime
import numpy as np

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
#def _training_model():
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    #return accuracy #push accuracy to xcom
    ti.xcom_push(key='model_accuracy', value=accuracy)

def _choose_best_model(ti): #ti task instance object
    print('choose best model')
    accuracies = ti.xcom_pull(key='model_accuracy', task_ids=[
        'processing_tasks.training_model_a',
        'processing_tasks.training_model_b',
        'processing_tasks.training_model_c'
    ])
    print(accuracies)
    max_value = np.max(accuracies)
    print(max_value)
    ti.xcom_push(key='max_value',value=max_value)

def _is_accurate(ti):
    max_value = ti.xcom_pull(key='max_value', task_ids='task_4')
    if(max_value>5):
        #return ['accurate', 'inaccurate'] #execute multiple tasks
        return('accurate')
    return('inaccurate')

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        do_xcom_push=False #Don't push to xcom, bash does as default
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_model = PythonOperator(
        task_id='task_4',
        python_callable=_choose_best_model
    )

    is_accurate = BranchPythonOperator(
        task_id='is_accurate',
        python_callable=_is_accurate
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    storing = DummyOperator(
        task_id='storing',
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )

    downloading_data >> processing_tasks >> choose_model
    choose_model >> is_accurate >> [accurate, inaccurate] >> storing