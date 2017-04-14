# Author: R Santosh
# Ecexute: python CustomParser.py
# Objective:
# 1.Reading each file the Logs folder and collection the Swift Object
# 2.Calculating the md5 digest of the object and then
# 3.Storing the log filename and Digest in dictionary
# 4.The dictionary is parsed and indexed for the Elastic Search

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from hashlib import md5

es = Elasticsearch()


def get_logs():
    """ This is just a mock, no need to make it complex"""
    return ["Logs/log{}.txt".format(i) for i in range(1, 5)]


def filter_logs(logs):
    """ A generator that yields a es formatted dictionary for matching lines
    
    :param logs: A list of log files to process
    """
    for log in logs:
        with open(log) as lf:
            for i, ln in enumerate(lf):
                try:
                    verb, obj, _, status = ln.split()[8:12]
                except ValueError:
                    continue  # If it cannot unpack the vars, it's not a valid line anyways.

                if verb in ("PUT", "DELETE") and status == "404":
                    yield dict(verb=verb,
                               origin="{}:{}".format(log, i + 1),
                               obj=md5(obj[4:]).hexdigest(),
                               status=status,
                               _index="support-logs",
                               _type="log")


def send_to_es(bulk_data):
    es.indices.create(index='support-logs', ignore=400)
    helpers.bulk(es, bulk_data)
    print("bulk update complete")


def reset_es_index():
    """ Use to quickly clear all data from the es index"""
    es.indices.delete(index='support-logs', ignore=[400, 404])


def get_full_line(origin):
    """ Takes the origin result from es and finds the full value of the log line"""
    location, line = origin.split(":")
    with open(location, 'r') as f:
        for i, l in enumerate(f):
            if i == int(line) + 1:
                return l


def find_by_obj(obj):
    """ Gets results for a given swift object
    
    example: find_by_obj("AUTH_test/bucket/transport.js")
    
    :type obj: str
    :param obj: The full path of the object -- in the form of: ACCOUNT/CONTAINER/OBJECT
    :return: list of matching log lines
    """

    if obj.startswith("/v1/"):
        obj = obj[4:]

    res = es.search(index='support-logs', doc_type="log", body={"query": {"match": {"obj": md5(obj).hexdigest()}}})
    print("Found {} result(s)".format(res['hits']['total']))

    return [get_full_line(doc['_source']['origin']) for doc in res['hits']['hits']]


if __name__ == "__main__":
    log_matches = filter_logs(get_logs())
    send_to_es(bulk_data=log_matches)
