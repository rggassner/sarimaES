#!/usr/bin/python3
from datetime import datetime,timedelta
from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np

#ElasticSearch Timeout
ES_TIMEOUT=60

def getIpDstFlowSum(ip,now_time,TRAINING_START_HOURS=None,TRAINING_END_HOURS=None):
    str_starttime=(now_time-timedelta(hours=TRAINING_START_HOURS)).strftime('%Y-%m-%dT%H:00:00-03:00')
    es = Elasticsearch(hosts=['http://127.0.0.1:9200'], request_timeout=ES_TIMEOUT)
    res = es.search(index="netflow-*", aggs={ "2": { "date_histogram": { "field": "@timestamp", "calendar_interval": "1h", "time_zone": "America/Sao_Paulo", "min_doc_count": 1 }, "aggs": { "1": { "sum": { "field": "flows" } } } } }, query = { "bool": { "must": [], "filter": [ { "match_all": {} } , { "match_phrase": { "agg.keyword": "dstip" } }, { "match_phrase": { "order.keyword": "flows" } }, { "match_phrase": { "filter.keyword": "all" } }, { "match_phrase": { "dst.keyword": ip } }, { "range": { "@timestamp": { "gte": str_starttime, "lte": (now_time).strftime('%Y-%m-%dT%H:00:00-03:00'), "format": "strict_date_optional_time" } } } ] } } )
    my_tarray = []
    my_sarray = []
    for thour in range(TRAINING_START_HOURS,0,-1):
        str_index_time=(now_time-timedelta(hours=thour)).strftime('%Y-%m-%dT%H:00:00.000-03:00')
        found=False
        count=0
        for etime in res['aggregations']['2']['buckets']:
            if etime['key_as_string']==str_index_time:
                found=True
                count=etime['1']['value']
        if thour > TRAINING_END_HOURS:
            my_tarray += [count]
        else:
            my_sarray += [count]
    start_stime=(now_time-timedelta(hours=TRAINING_END_HOURS)).strftime('%Y-%m-%dT%H:00:00.000-03:00')
    time_tidx = pd.date_range(start=str_starttime,periods=len(my_tarray),freq='H')
    time_sidx = pd.date_range(start=start_stime,periods=len(my_sarray),freq='H')
    training = pd.DataFrame({'index':time_tidx,'raw':np.array(my_tarray)}) 
    training = training.set_index('index')
    training.rename(columns={'raw':'Histórico'}, inplace=True)
    scoring = pd.DataFrame({'index':time_sidx,'raw':np.array(my_sarray)}) 
    scoring = scoring.set_index('index')
    scoring.rename(columns={'raw':'Ocorrido'}, inplace=True)
    return([training,scoring])

def getIpDstByteSum(ip,now_time,TRAINING_START_HOURS=None,TRAINING_END_HOURS=None):
    str_starttime=(now_time-timedelta(hours=TRAINING_START_HOURS)).strftime('%Y-%m-%dT%H:00:00-03:00')
    es = Elasticsearch(hosts=['http://127.0.0.1:9200'], request_timeout=ES_TIMEOUT)
    res = es.search(index="netflow-*", aggs= { "2": { "date_histogram": { "field": "@timestamp", "calendar_interval": "1h", "time_zone": "America/Sao_Paulo", "min_doc_count": 1 }, "aggs": { "1": { "sum": { "field": "bytes" } } } } }, query={ "bool": { "must": [], "filter": [ { "match_all": {} } , { "match_phrase": { "agg.keyword": "dstip" } }, { "match_phrase": { "order.keyword": "bytes" } }, { "match_phrase": { "filter.keyword": "all" } }, { "match_phrase": { "dst.keyword": ip } }, { "range": { "@timestamp": { "gte": str_starttime, "lte": (now_time).strftime('%Y-%m-%dT%H:00:00-03:00'), "format": "strict_date_optional_time" } } } ], "should": [], "must_not": [] } } )
    my_tarray = []
    my_sarray = []
    for thour in range(TRAINING_START_HOURS,0,-1):
        str_index_time=(now_time-timedelta(hours=thour)).strftime('%Y-%m-%dT%H:00:00.000-03:00')
        found=False
        count=0
        for etime in res['aggregations']['2']['buckets']:
            if etime['key_as_string']==str_index_time:
                found=True
                count=etime['1']['value']
        if thour > TRAINING_END_HOURS:
            my_tarray += [count]
        else:
            my_sarray += [count]
    start_stime=(now_time-timedelta(hours=TRAINING_END_HOURS)).strftime('%Y-%m-%dT%H:00:00.000-03:00')
    time_tidx = pd.date_range(start=str_starttime,periods=len(my_tarray),freq='H')
    time_sidx = pd.date_range(start=start_stime,periods=len(my_sarray),freq='H')
    training = pd.DataFrame({'index':time_tidx,'raw':np.array(my_tarray)}) 
    training = training.set_index('index')
    training.rename(columns={'raw':'Histórico'}, inplace=True)
    scoring = pd.DataFrame({'index':time_sidx,'raw':np.array(my_sarray)}) 
    scoring = scoring.set_index('index')
    scoring.rename(columns={'raw':'Ocorrido'}, inplace=True)
    return([training,scoring])

  targets = {
        '10.10.10.10':'aba.com'}
