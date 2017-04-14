# Author: R Santosh
# Ecexute: python CustomParser.py -l Logs
# Objective:
# 1.Reading each file the Logs folder and collection the Swift Object
# 2.Calculating the md5 digest of the object and then
# 3.Storing the log filename and Digest in dictionary
# 4.The dictionary is parsed and indexed for the Elastic Search


import os.path
import sys
import argparse
import hashlib
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch()


def process_file(logfile):
    print "*********************" + logfile + "*******************"
    myList = []
    with open(logfile) as lf:
        for line in lf:
            if '404' in line:
                myList.append(line)

    # var = myList[0].split()
    # for i, j in enumerate(var):
    #     print i, j
    # print myList
    myDict = dict()
    for item in myList:
        line = item.split()
        # print logfile + "--->" + line[9]
        object_key = hashlib.md5(line[9]).hexdigest()
        myDict[object_key] = logfile

    # print myDict
    return myDict


if __name__ =="__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-l", "--Logs", required=True, help="Path to the Logs folder")
    args = vars(ap.parse_args())
    list_of_files = []

    for dirname, subdir, filenames in os.walk(args["Logs"]):
        for filename in os.listdir(dirname):
            if filename.endswith(".txt"):
                abs_path = "%s/%s" % (dirname, filename)
                list_of_files.append(abs_path)

    for item in list_of_files:
        dict_body = process_file(item)
        idx = 1

        for k, v in dict_body.iteritems():
            # for each key-value pair, store it as a field and string inside the specified index of elastic search.
            doc = {
                k: v
            }
            res = es.index(index='test-index', doc_type='swiftlog', id=idx, body=doc)
            print(res)
        idx += 1
