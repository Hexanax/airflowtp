# various imports
from urllib import response
from warnings import catch_warnings
import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import urllib.request as request
from faker import Faker  # for creating fake names
import logging  # for logging in airflow
import json  # for json parsing
from random import (
    randint,
)  # for getting random integers, choosing a random item in a response


default_args_dict = {
    "start_date": airflow.utils.dates.days_ago(0),
    "concurrency": 1,
    "schedule_interval": None,  # no schedule
    "retries": 1,  # number of retries
    "retry_delay": datetime.timedelta(minutes=5),  # delay between retries
}

first_dag = DAG(
    dag_id="first_dag_stub",
    default_args=default_args_dict,
    # catchup: in case of failure you can pause the DAG, when restarting the DAG it will
    #   resume from the point when it left. But be careful, if you pause your dag and restart it
    #   After a long time the diff of time between dags will cause something that i didn't understand but
    #   its bad so thats why we put it to False.
    catchup=False,
)

NUM_CHARACTERS = 5
MAX_LEVEL = 3


def get(url):
    # create the http request
    req = request.Request(url)
    r = request.urlopen(req)

    # decode data to json
    raw_data = r.read()
    encoding = r.info().get_content_charset("utf8")  # JSON default

    # parse json in python dict
    return json.loads(raw_data.decode(encoding))


def save_json(data, file_name):
    with open(f"dags/temp/{file_name}.json", "w") as outfile:
        json.dump(data, outfile)


def read_json(file_name):
    with open("dags/temp/names.json") as json_file:
        return json.load(json_file)


def _generate_name():
    fake = Faker()
    names = [fake.name() for _ in range(NUM_CHARACTERS)]
    try:
        save_json(names, "names")
        logging.info(f"Created names: {names}")
    except Exception:
        logging.info(f"Error saving names: {Exception.args}")


def _generate_level():
    levels = [randint(1, MAX_LEVEL) for _ in range(NUM_CHARACTERS)]
    try:
        save_json(levels, "levels")
        logging.info(f"Created levels: {levels}")
    except Exception:
        logging.info(f"Error saving levels: {Exception.args}")


def _generate_attribute():
    attributes = [
        {
            "strength": randint(6, 18),
            "dexterity": randint(2, 18),
            "constitution": randint(2, 18),
            "intelligence": randint(2, 18),
            "wisdom": randint(2, 18),
            "charisma": randint(2, 18),
        }
        for _ in range(NUM_CHARACTERS)
    ]
    try:
        save_json(attributes, "attributes")
        logging.info(f"Created attributes: {attributes}")
    except Exception:
        logging.info(f"Error saving attributes: {Exception.args}")


def _generate_class():
    # get the list of classes from the api
    try:
        url = "https://www.dnd5eapi.co/api/classes"
        response = get(url)
        # choose a random class from there
    except Exception:
        logging.info(f"Error retrieving classes from api: {Exception.args}")
        return
    character_classes = [
        response["results"][randint(0, response["count"] - 1)]["index"]
        for _ in range(NUM_CHARACTERS)
    ]
    logging.info(f"Created class: {character_classes}")
    try:
        save_json(character_classes, "classes")
    except Exception:
        logging.info(f"Error saving classes: {Exception.args}")


def _generate_language():
    # get the list of languages from the api
    MAX_LANGUAGES = 3
    try:
        url = "https://www.dnd5eapi.co/api/languages"
        response = get(url)
        # choose a random language because you said we could
        character_languages = [
            [
                response["results"][randint(0, response["count"] - 1)]["index"]
                for i in range(randint(1, MAX_LANGUAGES))
            ]
            for _ in range(NUM_CHARACTERS)
        ]
        logging.info(f"Created language: {character_languages}")
    except Exception:
        logging.info(f"Error retrieving languages: {Exception}")
    try:
        save_json(character_languages, "languages")
    except Exception:
        logging.info(f"Error saving languages: {Exception}")


def _generate_race():
    # get the list of races from the api
    try:
        url = "https://www.dnd5eapi.co/api/races"
        response = get(url)
        # choose a random race
        character_races = [
            response["results"][randint(0, response["count"] - 1)]["index"]
            for _ in range(NUM_CHARACTERS)
        ]
        logging.info(f"Created race: {character_races}")
    except Exception:
        logging.info(f"Error retrieving races: {Exception}")
    try:
        save_json(character_races, "races")
    except Exception:
        logging.info(f"Error saving races: {Exception}")


start = DummyOperator(
    task_id="start",
    dag=first_dag,
)

create_name = PythonOperator(
    task_id="create_name",
    dag=first_dag,
    python_callable=_generate_name,
)

create_attributes = PythonOperator(
    task_id="create_attributes",
    dag=first_dag,
    python_callable=_generate_attribute,
)

create_languages = PythonOperator(
    task_id="create_languages",
    dag=first_dag,
    python_callable=_generate_language,
)

create_class = PythonOperator(
    task_id="create_class",
    dag=first_dag,
    python_callable=_generate_class,
)

create_proficiency = DummyOperator(
    task_id="profficiency_choices",
    dag=first_dag,
)

create_race = PythonOperator(
    task_id="create_race",
    dag=first_dag,
    python_callable=_generate_race,
)

create_level = PythonOperator(
    task_id="create_level",
    dag=first_dag,
    python_callable=_generate_level,
)

create_spells = DummyOperator(
    task_id="create_spells",
    dag=first_dag,
)

remove_previous_characters = DummyOperator(
    task_id="remove_previous_characters",
    dag=first_dag,
    trigger_rule="none_failed",
)

add_new_characters = DummyOperator(
    task_id="add_new_characters",
    dag=first_dag,
)
end = DummyOperator(
    task_id="end",
    dag=first_dag,
    trigger_rule="none_failed",
)


start >> create_name >> remove_previous_characters
start >> create_attributes >> remove_previous_characters
start >> create_class >> create_spells >> remove_previous_characters

# Technically you would need the race to have the languages which is why I put the workflow like that
# however in the end I decided not to use the race to get the languages because it is not required 
# in the assignment and I do not feel like going above and beyond today.
start >> create_race >> create_languages >> remove_previous_characters
start >> create_level >> remove_previous_characters
remove_previous_characters >> add_new_characters
[create_class, create_race] >> create_proficiency >> remove_previous_characters
add_new_characters >> end
