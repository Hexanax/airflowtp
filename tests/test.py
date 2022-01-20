import urllib.parse
import urllib.request
import json
import faker
from random import randint # for getting random integers, choosing a random item in a response
import logging


url = 'https://www.dnd5eapi.co/api/classes'
def get(url):
    # create the http request
    req = urllib.request.Request(url)
    r = urllib.request.urlopen(req)

    # decode data to json
    raw_data = r.read()
    encoding = r.info().get_content_charset('utf8')  # JSON default
    
    # parse json in python dict
    return json.loads(raw_data.decode(encoding))
def _generate_class():
    # get the list of classes from the api
    try:
        url = 'https://www.dnd5eapi.co/api/classes'
        response = get(url)
        # choose a random class from there
        character_classes = [response["results"][randint(0, response["count"])]["index"] for _ in range(5)]
        logging.info(f'Created class: {character_classes}')
    except Exception:
        logging.info(f"Error retrieving classes: {Exception.args}")
    try:
        save_json(character_classes, "classes")
    except Exception:
        logging.info(f"Error saving classes: {Exception.args}")

def save_json(data, file_name):
    with open(f'dags/temp/{file_name}.json', 'w') as outfile:
        json.dump(data, outfile)

def json_test_read():
    with open('dags/temp/names.json') as json_file:
        data = json.load(json_file)
        for d in data: print(f"{d}\n")
# get(url)
