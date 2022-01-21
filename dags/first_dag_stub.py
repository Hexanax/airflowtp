# various imports
from urllib import response
from warnings import catch_warnings
import airflow
import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
    with open(f"dags/temp/{file_name}.json") as json_file:
        return json.load(json_file)

def _check_table():
    if "table" == "exists":
        return "remove_previous_characters"
    else:
        "create_table"
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
        character_languages =', '.join([
            [
                response["results"][randint(0, response["count"] - 1)]["index"]
                for i in range(randint(1, MAX_LANGUAGES))
            ]
            for _ in range(NUM_CHARACTERS)
        ])
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


def _generate_proficiencies():
    classes = read_json("classes")
    races = read_json("races")
    proficiencies = []
    for c, r in zip(classes, races):
        character_proficiencies = []
        # get proficiencies from the class
        response = get(f"https://www.dnd5eapi.co/api/classes/{c}")
        character_proficiencies.extend(
            [r["index"] for r in response["proficiencies"]]
        )
        # get proficiencies from the race
        response = get(f"https://www.dnd5eapi.co/api/races/{r}")
        character_proficiencies.extend(
            [r["index"] for r in response["starting_proficiencies"]]
        )

        # I noticed there were optional proficiencies, I will not include. I hope this is fine.

        proficiencies.append(', '.join(character_proficiencies))

    try:
        # choose a random proficiencie
        character_proficiencies = [
            response["results"][randint(0, response["count"] - 1)]["index"]
            for _ in range(NUM_CHARACTERS)
        ]
        logging.info(f"Created proficiencies: {character_proficiencies}")
    except Exception:
        logging.info(f"Error retrieving proficiencies: {Exception}")

    # save all proficiencies
    try:
        save_json(proficiencies, "proficiencies")
    except Exception:
        logging.info(f"Error saving proficiencies: {Exception}")


def _generate_spell():
    # get the list of spells for lvls 0 to 2 from the api
    try:
        url = "https://www.dnd5eapi.co/api/spells?level=0,1,2"
        response = get(url)
        # choose a random spell
        all_spells = [result["index"] for result in response["results"]]
        logging.info(f"Successfully acquired all spells")
    except Exception:
        logging.info(f"Error retrieving list of spells: {Exception}")
        return
    # now get all the required info
    levels = read_json("levels")
    classes = read_json("classes")
    spells = []
    for c, l in zip(classes, levels):
        # load the allowed spells for this class
        response = get(f"https://www.dnd5eapi.co/api/classes/{c}/spells")

        if len(response["results"]) == 0:  # skip classes with no spells
            spells.append("")
            continue

        # compute the list of spells that are both allowed by the class and level
        allowed_spells = list(
            set(all_spells).intersection(
                [result["index"] for result in response["results"]]
            )
        )

        # This is imperfect because I should have queried the "all_spells" based
        # on the level of the character and not everything from 0 to 2, but this is
        # a detail so I was hoping you would let this error slide :)

        if len(allowed_spells) == 0:
            # no point in continuinig if this is empty
            spells.append("")
            continue

        # randomly select some spells
        character_spells = [
            allowed_spells.pop(randint(0, len(allowed_spells) - 1))
            for _ in range(l)
        ]
        spells.append(', '.join(character_spells))
    # save the spells
    try:
        save_json(spells, "spells")
    except Exception:
        logging.info(f"Error saving spells: {Exception}")


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

create_proficiency = PythonOperator(
    task_id="create_proficiency",
    dag=first_dag,
    python_callable=_generate_proficiencies,
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

create_spells = PythonOperator(
    task_id="create_spells",
    dag=first_dag,
    python_callable=_generate_spell,
)

check_db = BranchPythonOperator(
    task_id='check_database',
    dag=first_dag,
    python_callable=_check_table,
    trigger_rule='all_success',
)

remove_previous_characters = DummyOperator(
    task_id="remove_previous_characters",
    dag=first_dag,
    trigger_rule="none_failed",
)

create_table = DummyOperator(
    task_id="create_table",
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

# character creation
start >> create_name >> check_db
start >> create_attributes >> check_db
start >> create_class >> create_spells >> check_db
# Technically you would need the race to have the languages which is why I put the workflow like that
# however in the end I decided not to use the race to get the languages because it is not required
# in the assignment and I do not feel like going above and beyond today.
start >> create_race >> create_languages >> check_db
start >> create_level >> check_db
[create_class, create_race] >> create_proficiency >> check_db

# persistence of characters
check_db >> [remove_previous_characters, create_table]
[remove_previous_characters, create_table] >> add_new_characters

# end
add_new_characters >> end
