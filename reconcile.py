# -*- coding: utf-8 -*-

"""
An OpenRefine reconciliation service for the AAT API.
This code is adapted from https://github.com/lawlesst/fast-reconcile
"""
from flask import Flask, request, jsonify
from fuzzywuzzy import fuzz
import getopt
import json
from operator import itemgetter
import re
import requests
from sys import version_info
import urllib
import xml.etree.ElementTree as ET
# Help text processing
import text
import requests_cache
# cache calls to the API.
requests_cache.install_cache('getty_cache')

app = Flask(__name__)

# See if Python 3 for unicode/str use decisions
PY3 = version_info > (3,)


# Map the AAT query indexes to service types
default_query = {
    "id": "AATGetTermMatch",
    "name": "AAT term",
    "index": "term"
}

#to add some other services in the future (TGN, ULAN...)
full_query = []

full_query.append(default_query)

# Make a copy of the AAT mappings.
query_types = [{'id': item['id'], 'name': item['name']} for item in full_query]

def make_uri(getty_id):
    """
    Prepare an AAT url from the ID returned by the API.
    """
    getty_uri = 'http://vocab.getty.edu/aat/{}'.format(getty_id)
    return getty_uri

# Basic service metadata. There are a number of other documented options
# but this is all we need for a simple service.
metadata = {
    "name": "Getty Reconciliation Service",
    "defaultTypes": query_types,
    "view": {
        "url": "http://vocab.getty.edu/aat/{{id}}"
    }
}



def jsonpify(obj):
    """
    Helper to support JSONP
    """
    try:
        callback = request.args['callback']
        response = app.make_response("%s(%s)" % (callback, json.dumps(obj)))
        response.mimetype = "text/javascript"
        return response
    except KeyError:
        return jsonify(obj)


def search(raw_query):
    """ 
    This takes a term tries to query it against the api. It then parses the response and puts
    it into a dictionary and appends it to a list 'out'. The final results are sorted based on
    'score'.
    >>> reconcile.search('Romantic')
    [{'id': 'http://vocab.getty.edu/aat/300021476', 
        'name': 'Neo-Romantic ', 
        'score': 80, 
        'match': False, 
        'type': [{'id': 'AATGetTermMatch', 'name': 'AAT term', 'index': 'term'}]}, ... ]
    
    A "no match" should return an empty list:
    >>> reconcile.search('Davidé Schober')
    []
    """
    
    query_type_meta = full_query 
    api_base_url = 'http://vocabsservices.getty.edu/AATService.asmx/AATGetTermMatch'
    payload = {'term':raw_query.strip(), 'logop':'and', 'notes':''}
    out = []
    try:
        #send the request and the payload! 
        resp = requests.get(api_base_url, params=payload)
        app.logger.debug("AAT url is {}".format(resp.url))
        results = ET.fromstring(resp.content)
    except getopt.GetoptError as e:
        app.logger.warning(e)
        #This just returns an empty list if there's an error"
        return out.append('Error, see logs')

    for child in results.iter('Preferred_Parent'):
        match = False
        # the termid is NOT the ID ! We have to find it in the first prefrered parent
        # search and grab the groups
        regex = re.search(r"(?P<name>.*)\[(?P<result_id>.+?)\]", child.text.split(',')[0]) 
        if regex:
            score = fuzz.token_sort_ratio(raw_query, regex.group('name'))
            name = regex.group('name')
            result_id = regex.group('result_id')

            if score > 95:
                match = True

            app.logger.debug("Label is {}. Score is {}. URI is {}".format(name, score, make_uri(result_id)))

            resource = {
                "id": make_uri(result_id),
                "name": name,
                "score": score,
                "match": match,
                "type": query_type_meta
            }
        # Attach a successful search. If nothing exists carry on 
        out.append(resource)
    # Sort this list containing prefterms by score
    sorted_out = sorted(out, key=itemgetter('score'), reverse=True)
    # Refine only will handle top 10 matches.
    return sorted_out[:10]

@app.route("/", methods=['POST', 'GET'])
def reconcile():
    # If a 'queries' parameter is supplied then it is a dictionary
    # of (key, query) pairs representing a batch of queries. We
    # should return a dictionary of (key, results) pairs.
    queries = request.form.get('queries')
    if queries:
        queries = json.loads(queries)
        results = {}
        for (key, query) in queries.items():
            data = search(query['query'])
            results[key] = {"result": data}
        return jsonpify(results)
    # If neither a 'query' nor 'queries' parameter is supplied then
    # we should return the service metadata.
    return jsonpify(metadata)


if __name__ == '__main__':
    from optparse import OptionParser

    oparser = OptionParser()
    oparser.add_option('-d', '--debug', action='store_true', default=False)
    opts, args = oparser.parse_args()
    app.debug = opts.debug
    app.run(host='0.0.0.0')
