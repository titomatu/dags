from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime
from subdags.subdag_parallel_dag import subdag_parallel_dag

default_args = {
    'start_date' : datetime(2022, 6, 7)
}

with DAG('parallel_taskgroup', schedule_interval='@daily', 
        default_args=default_args,
        catchup=False) as dag:
        #Define tasks/operators

        task_1 = BashOperator(
            task_id='task_1',
            bash_command='sleep 3'
        )

        with TaskGroup('processing_tasks') as processing_tasks:

            task_2 = BashOperator(
                task_id='task_2',
                bash_command='sleep 3'
            )

            with TaskGroup('spark_tasks') as spark_tasks:
                task_3 = BashOperator(
                    task_id='task_3',
                    bash_command='sleep 3'
                )
            
            with TaskGroup('flink_tasks') as flink_tasks:
                task_3 = BashOperator(
                    task_id='task_3',
                    bash_command='sleep 3'
                )            

        task_4 = BashOperator(
            task_id='task_4',
            bash_command='sleep 3'
        )

        task_1 >> processing_tasks >> task_4