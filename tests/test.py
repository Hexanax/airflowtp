import urllib.parse
import urllib.request
import json


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
