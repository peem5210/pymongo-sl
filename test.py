from typing import Any
from pymongo_sl.mongo_client import MongoClientSL 
from pymongo import MongoClient 
from redis import Redis
from tqdm import tqdm
from util.func import *
import time
import numpy as np

load_env()
redis = Redis(host=ENV("REDIS_HOST"), port=ENV("REDIS_PORT"), password=ENV("REDIS_PASSWORD"))


def tests(mongo_client: MongoClient, documents: list[dict[str, Any]]):
    global redis


    collection = mongo_client[ENV("MONGODB_DATABASE")][ENV("MONGODB_COLLECTION")]    
    collection_ = mongo_client.mongo.mongo
    
    timing = []
    result_list = []
    for document in tqdm(documents):
        timing_and_append_result(timing, result_list, collection.find_one, ({"_id":document["_id"]}, {"_id":True, "read":True, "region":True}))
        timing_and_append_result(timing, result_list, collection.find_one, filter = {"_id":document["_id"]}, projection = {"_id":True, "read":True, "region":True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":document["_id"]}, projection = {"_id":True, "read":True, "region":True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":document["_id"]}, projection = {"_id":True, "read":True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":document["_id"]}, projection = {})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":document["_id"]}, projection = [])
        timing_and_append_result(timing, result_list, collection_.find_one, filter = {"_id":document["_id"]}, projection = {"_id":False})
        timing_and_append_result(timing, result_list, collection_.find_one, filter = {"_id":document["_id"]}, projection = {"_id":True, "read":True, "region":True})
        timing_and_append_result(timing, result_list, collection_.find_one, filter = {"_id":document["_id"]}, projection = {"_id":True, "read":True})
        timing_and_append_result(timing, result_list, collection_.find_one, filter = {"_id":document["_id"]}, projection = {})
        timing_and_append_result(timing, result_list, collection_.find_one, filter = {"_id":document["_id"]}, projection = [])
        timing_and_append_result(timing, result_list, collection_.find_one, filter = {"_id":document["_id"]}, projection = {"_id":False})
        timing_and_append_result(timing, result_list, collection_.find_one, {"_id":document["_id"]})
        
    
    #clear cache
    # for x in documents: redis.delete(str(x["_id"]))
    
    return result_list, timing

def timing_and_append_result(t, l, func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    stop = time.perf_counter()
    t.append(stop-start)
    l.append(result)
    return l, t

def validate_and_measure(result_nt: list, timing_nt: list, result_sl: list, timing_sl: list):
    print(f"{np.average(timing_nt) = } {np.average(timing_sl) = }")
    for idx, sl in enumerate(result_sl):
        nt = result_nt[idx]
        assert nt == sl, f"{nt} should equal to {sl}"
        
if __name__ == '__main__':
    mongo_nt = MongoClient(
        host = ENV("MONGODB_HOST_SGP_1"), 
        port = ENV("MONGODB_PORT", True), 
        username = ENV("MONGODB_USERNAME"), 
        password = ENV("MONGODB_PASSWORD"),
        )

    mongo_sl = MongoClientSL(
        host = ENV("MONGODB_HOST_SGP_1"), 
        port = ENV("MONGODB_PORT", True), 
        username = ENV("MONGODB_USERNAME"), 
        password = ENV("MONGODB_PASSWORD"),
        redis_client = redis
        )
    
    documents = [x for x in mongo_nt.mongo.mongo.find(filter={"region":"SGP", "read":False}, limit=5)]    
    result_nt, timing_nt = tests(mongo_nt, documents)
    result_sl, timing_sl = tests(mongo_sl, documents)

    validate_and_measure(result_nt, timing_nt, result_sl, timing_sl)