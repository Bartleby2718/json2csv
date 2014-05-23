# -*- coding:utf-8 -*-
""" 
------------------------------------------------------------------------------------------------------------------------
    author: Lier
    date: 2014. 5. 23.
------------------------------------------------------------------------------------------------------------------------
    
"""

import logging

from elasticsearch import Elasticsearch


logger = logging.getLogger(__name__)

from csv import *


class Converter(object):
    @classmethod
    def search(cls, index, doc_type, body):
        es = Elasticsearch('http://bapul-search.cloudapp.net:9200')

        result = es.search(index=index, doc_type=doc_type, body=body)
        return result['hits']['hits']

    @classmethod
    def get_fieldnames(cls, arr):
        return arr[0].keys()

    @classmethod
    def __convert_dict_to_utf8__(cls, d):
        for key, value in d.items():
            if isinstance(value, dict):
                cls.__convert_dict_to_utf8__(value)
            elif isinstance(value, str):
                d[key] = value.encode('utf-8')

    @classmethod
    def test(cls):
        body = {
            "query": {
                "bool": {
                    "must": [{"term": {"reply.principal": "happenstantial"}}],
                    "must_not": [],
                    "should": []}},
            "from": 0,
            "size": 10,
            "sort": [],
            "facets": {}
        }
        rows = cls.search(index='bapul', doc_type='reply', body=body)

        file = open('/Users/Lier/Downloads/file.csv', 'w', encoding='utf-8')
        writer = DictWriter(file, fieldnames=rows[0]['_source'].keys())

        for row in rows:
            print(row['_source'])
            # cls.__convert_dict_to_utf8__(row)
            writer.writerow(row['_source'])