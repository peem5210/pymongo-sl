from pymongo_sl.mongo_client import MongoClientSL as MongoClient
from pymongo import MongoClient as NativeMongoClient
from redis import Redis
from tqdm import tqdm
from util.func import *
import time
import numpy as np

load_env()
redis = Redis(host=ENV("REDIS_HOST"), port=ENV("REDIS_PORT"), password=ENV("REDIS_PASSWORD"))


def tests(mongo: MongoClient, temp):
    global redis

    collection = mongo[ENV("MONGODB_DATABASE")][ENV("MONGODB_COLLECTION")]    
    result_list = []
    timing = []
    
    for i in tqdm(range(len(temp))):
        timing_and_append_result(timing, result_list, collection.find_one, ({"_id":temp[i]["_id"]}, {"_id":True, "read":True, "region":True}))
        timing_and_append_result(timing, result_list, collection.find_one, filter = {"_id":temp[i]["_id"]}, projection = {"_id":True, "read":True, "region":True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":temp[i]["_id"]}, projection = {"_id":True, "read":True, "region":True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":temp[i]["_id"]}, projection = {"_id":True, "read":True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":temp[i]["_id"]}, projection = {})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":temp[i]["_id"]}, projection = [])
        timing_and_append_result(timing, result_list, collection.find_one, {"_id":temp[i]["_id"]}, projection = {"_id":False})
        
    
    #clear cache
    # for x in temp: redis.delete(str(x["_id"]))
    
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
    mongo_nt = NativeMongoClient(
        host = ENV("MONGODB_HOST_SGP_1"), 
        port = ENV("MONGODB_PORT", True), 
        username = ENV("MONGODB_USERNAME"), 
        password = ENV("MONGODB_PASSWORD"),
        )

    mongo_sl = MongoClient(
        host = ENV("MONGODB_HOST_SGP_1"), 
        port = ENV("MONGODB_PORT", True), 
        username = ENV("MONGODB_USERNAME"), 
        password = ENV("MONGODB_PASSWORD"),
        redis_client = redis
        )
    
    temp = [x for x in mongo_nt.mongo.mongo.find(filter={"region":"SGP", "read":False}, limit=5)]
        
    result_nt, timing_nt = tests(mongo_nt, temp)
    result_sl, timing_sl = tests(mongo_sl, temp)

    validate_and_measure(result_nt, timing_nt, result_sl, timing_sl)