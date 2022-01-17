# various imports
from urllib import response
import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import urllib.request as request
from faker import Faker # for creating fake names
import logging # for logging in airflow
import json # for json parsing
from random import randint # for getting random integers, choosing a random item in a response


default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None, # no schedule
    'retries': 1,   # number of retries
    'retry_delay': datetime.timedelta(minutes=5),   # delay between retries
}

first_dag = DAG(
    dag_id='first_dag_stub',
    default_args=default_args_dict,
    # catchup: in case of failure you can pause the DAG, when restarting the DAG it will 
    #   resume from the point when it left. But be careful, if you pause your dag and restart it 
    #   After a long time the diff of time between dags will cause something that i didn't understand but 
    #   its bad so thats why we put it to False.
    catchup=False,  
)
def get(url):
    # create the http request
    req = request.Request(url)
    r = request.urlopen(req)

    # decode data to json
    raw_data = r.read()
    encoding = r.info().get_content_charset('utf8')  # JSON default
    
    # parse json in python dict
    return json.loads(raw_data.decode(encoding))

def _generate_name():
    fake = Faker()
    name = fake.name()
    logging.info(f'Created name: {name}')

def _generate_class():
    # get the list of classes from the api
    url = 'https://www.dnd5eapi.co/api/classes'
    response = get(url)
    # choose a random class from there
    character_class = response["results"][randint(0, response["count"])]["index"]
    logging.info(f'Created class: {character_class}')

start = DummyOperator(
    task_id='start',
    dag=first_dag,
)

create_name = PythonOperator(
    task_id='create_name',
    dag=first_dag,
    python_callable=_generate_name,
)

create_attributes = DummyOperator(
    task_id='create_attributes',
    dag=first_dag,
)

create_languages = DummyOperator(
    task_id='create_languages',
    dag=first_dag,
)

create_class = PythonOperator(
    task_id='create_class',
    dag=first_dag,
    python_callable=_generate_class,
)

create_proficiency = DummyOperator(
    task_id='profficiency_choices',
    dag=first_dag,
)

create_race = DummyOperator(
    task_id='create_race',
    dag=first_dag,
)

create_level = DummyOperator(
    task_id='create_level',
    dag=first_dag,
)

create_spells = DummyOperator(
    task_id='create_spells',
    dag=first_dag,
)

remove_previous_characters = DummyOperator(
    task_id='remove_previous_characters',
    dag=first_dag,
)

add_new_characters = DummyOperator(
    task_id='add_new_characters',
    dag=first_dag,
)
end = DummyOperator(
    task_id='end',
    dag=first_dag,
    trigger_rule='none_failed',
)

creation_tasks =  [create_name, create_attributes, create_languages, create_class, create_proficiency, create_race, create_level, create_spells]
start >> creation_tasks
creation_tasks >> remove_previous_characters
remove_previous_characters >> add_new_characters
add_new_characters >> end