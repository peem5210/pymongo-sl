from enum import Enum
import os
import sys
import time
import random
    
import cProfile
import pstats
import numpy as np
import pandas as pd
from pymongo.collection import Collection
from redis import Redis
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId
from tqdm import tqdm
import argparse

from enum import Enum, auto

from rcache.rcache import Rcache
from util.func import *
class CacheMode(Enum):
    LOCAL = auto()
    REDIS = auto()
    NO_CACHE = auto()
    IDEAL = auto()
    NOT_EXIST = auto()

class RcacheProfiling:
    def __init__(self):
        print("Connecting redis...")
        self.redis = Redis(
            host = ENV("REDIS_HOST"), 
            port = ENV("REDIS_PORT", True),
            password= ENV("REDIS_PASSWORD")
        )
        print("Finished")
        print("Connecting mongo...")
        self.collection_connections = {}
        for prefix in MONGODB_HOST_PREFIXES:
            self.mongo = MongoClient(host = ENV(f"MONGODB_HOST_{prefix}"), port = ENV("MONGODB_PORT", True), username= ENV("MONGODB_USERNAME"), password=ENV("MONGODB_PASSWORD"))
            self.db = self.mongo[ENV("MONGODB_DATABASE")]
            self.collection_connections[prefix] = (self.db[ENV("MONGODB_COLLECTION")])
        print("Finished")
        self.local_cache = {}
    def get_cache(self, objectid: str):
        if CACHE_MODE == CacheMode.REDIS:
            return self.redis.get(str(objectid))
        elif CACHE_MODE == CacheMode.LOCAL:
            return self.local_cache[str(objectid)]
        elif CACHE_MODE == CacheMode.NO_CACHE:
            return None
        else:
            raise Exception("Exhuasive handling exception")   
        
    def populate_cache(self, object_list: list[tuple[ObjectId, str]]):
        if CACHE_MODE == CacheMode.REDIS:
            print(f"populating cache with mode:{CACHE_MODE.name}...")
            for _id, region in tqdm(object_list):
                self.redis.set(str(_id), str(region))
        elif CACHE_MODE == CacheMode.LOCAL:
            print(f"populating cache with mode:{CACHE_MODE.name}...")
            for _id, region in tqdm(object_list):
                self.local_cache[str(_id)] = region 
        elif CACHE_MODE == CacheMode.NO_CACHE:
            return
        elif CACHE_MODE == CacheMode.IDEAL:
            return
        else:
            raise Exception("Exhuasive handling exception")       

    def run(self):
        #Load object to memory
        print("fetching all object from the DB")
        object_list = \
            [(x['_id'], x['region']) for x in self.collection_connections["SGP_1"].find(limit=NUM_SELECT_FROM_DB, projection=["_id", "region"], filter={"region":"SGP"})] +\
            [(x['_id'], x['region']) for x in self.collection_connections["SGP_1"].find(limit=NUM_SELECT_FROM_DB, projection=["_id", "region"], filter={"region":"ORE"})]
            
        print("finished")
        #Load object to cache
        self.populate_cache(object_list)

        
        #Randomly read object from mongo
        max_iter = len(object_list)-1
        read_time = []
        update_time = []
        result = []
        for prefix, connection in self.collection_connections.items():
            summary = {}
            print(f"\n\ntesting on {prefix} connection")
            for _ in tqdm(range(NUM_RANDOM)): 
                rint = random.randint(0, max_iter)
                object_id, region = object_list[rint]

                if (not IS_CROSS_REGION) and (region not in prefix):
                    continue
                
                if region not in summary:
                    summary[region] = 1
                else:
                    summary[region] += 1
                rint = random.randint(0,1)
                if CACHE_MODE != CacheMode.IDEAL :
                    region = None
                if rint == 0:
                    s1 = time.perf_counter()
                    if CACHE_MODE != CacheMode.IDEAL :
                        region = self.get_cache(object_id)
                    self.read_object(connection, object_id, region=region)
                    s2 = time.perf_counter()
                    read_time.append(s2-s1)
                elif rint == 1:
                    s1 = time.perf_counter()
                    if CACHE_MODE != CacheMode.IDEAL :
                        region = self.get_cache(object_id)
                    self.update_object(connection, object_id,{"$set":{"read":True}} , region=region)
                    s2 = time.perf_counter()
                    update_time.append(s2-s1)
                    
            "create object"        
            "delete object"
            
            print(f"result:\nread avg ({prefix})[{IS_CROSS_REGION = }]:{np.average(read_time)}\nupdate avg ({prefix})[{IS_CROSS_REGION = }]:{np.average(update_time)}")
            print(summary)
            result.append((prefix, CACHE_MODE.name, np.average(read_time), np.average(update_time), IS_CROSS_REGION))
        return result
    
    def create_object(self):
        "In case of object creation, caching won't help"
        "In Snaplogic, we would read parent object and retreive the region from the parent object"
        pass
    
    def read_object(self, connection: Collection, _id: ObjectId, region: str = None):
        if region is not None:
            if type(region) == bytes: 
                return connection.find_one({"_id":_id, "region":region.decode("utf-8")})  
            else:
                return connection.find_one({"_id":_id, "region":region})  
        return connection.find_one({"_id":_id})    
    
    def update_object(self, connection: Collection, _id: ObjectId, update: dict, region: str = None):
        """update:: expr should be something like {'$set':{"read":True}}"""
        if region is not None:
            if type(region) == bytes: 
                region = region.decode("utf-8")
            assert type(region) == str, f"type of region should be string: {type(region)}"
            return connection.update_one({"_id":_id, "region":region}, update)
        return connection.update_one({"_id":_id}, update)
    
    def delete_object(self, _id: ObjectId):
        "test for delete is pass for now"
        pass
        # return self.collection.find_one_and_delete({"_id:":_id})
    
def populate_mongo_task(rcache: RcacheProfiling):
    region = REGIONS[random.randint(0, len(REGIONS)-1)]
    rcache.collection.insert_one({"region":region, "read":False})

def populate_mongo(rcache: RcacheProfiling):
    # with futures.ProcessPoolExecutor() as pool:
        for _ in tqdm(range(NUM_INSERT_TO_DB)):
            # future_result = pool.submit(populate_mongo_task, rcache)  
            populate_mongo_task(rcache)
            # future_result.add_done_callback(total_error)

REGIONS = ['SGP', 'ORE']
NUM_SELECT_FROM_DB = 10_000
NUM_RANDOM = 1000
CACHE_MODE = CacheMode.LOCAL
MONGODB_HOST_PREFIXES = ["SGP_1", "SGP_2", "ORE_1", "ORE_2"]
NUM_INSERT_TO_DB = 100_000
IS_CROSS_REGION = False

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--select', '-s', default=10)
    parser.add_argument('--random-iter', '-r', default=10)
    parser.add_argument('--cache-mode', '-m', default="local")
    parser.add_argument('--populate', '-p',action='store_true')
    parser.add_argument('--cross-region', '-c',action='store_true')
    parser.add_argument('--all-test', '-a',action='store_true')
    args = parser.parse_args(sys.argv[1:])
    print(args)
    NUM_SELECT_FROM_DB = int(args.select)
    NUM_RANDOM = int(args.random_iter)
    IS_CROSS_REGION = args.cross_region
    if args.cache_mode == "local":
        CACHE_MODE=CacheMode.LOCAL
    elif args.cache_mode == "redis":
        CACHE_MODE=CacheMode.REDIS
    elif args.cache_mode == "no_cache":
        CACHE_MODE=CacheMode.NO_CACHE
    elif args.cache_mode == "ideal":
        CACHE_MODE=CacheMode.IDEAL
    else:
        sys.exit(1)
    
    print(f'''Running with configuration\n\n{REGIONS = }\n{NUM_SELECT_FROM_DB = }\n{NUM_RANDOM = }\n{CACHE_MODE = }\n\n{NUM_INSERT_TO_DB = }\n\n\n''')
    load_dotenv(os.path.join(os.path.dirname('./'), '.env'))
    
    rcache = RcacheProfiling()

    # s1 = time.time()
    # print([rcache.collection.find_one({"region":"SGP"})])
    # s2 = time.time()
    # print([rcache.collection.find_one({"region":"ORE"})])
    # s3 = time.time()
    # print(f"query region SGP: {s2-s1} \nquery region ORE: {s3-s2}")
    # s1 = time.time()
    # print([rcache.collection.find_one({"_id":ObjectId("6162d48cd20b9f4b16d39ff4")})])
    # s2 = time.time()
    # print([rcache.collection.find_one({"_id":ObjectId("6162d410d20b9f4b16d39c59")})])
    # s3 = time.time()
    # print(f"query _id SGP: {s2-s1} \nquery _id ORE: {s3-s2}")
    # s1 = time.time()
    # print([rcache.collection.find_one({"_id":ObjectId("6162d48cd20b9f4b16d39ff4"), "region":"SGP"})])
    # s2 = time.time()
    # print([rcache.collection.find_one({"_id":ObjectId("6162d410d20b9f4b16d39c59"), "region":"ORE"})])
    # s3 = time.time()
    # print(f"query _id, region SGP: {s2-s1} \nquery _id, region ORE: {s3-s2}")

    if args.populate:
        populate_mongo(rcache)
    elif args.all_test:
        result = []
        for CM in [CacheMode.NO_CACHE, CacheMode.LOCAL, CacheMode.REDIS, CacheMode.IDEAL]:
            CACHE_MODE = CM
            for ICR in [False, True]:
                IS_CROSS_REGION = ICR
                result += rcache.run()
            
        pd.DataFrame(data=result, columns=["region", "cache_mode", "average_read_time", "average_update_time", "is_cross_region"]).to_csv("all_result.csv")
        #  result.append((prefix, CACHE_MODE.name, np.average(read_time), np.average(update_time)))
    else:
        with cProfile.Profile() as pr:
            rcache.run()
        stats = pstats.Stats(pr)
        stats.sort_stats(pstats.SortKey.TIME)
        # stats.print_stats()
        stats.dump_stats(filename="rcache_profiling.prof")
    
    
    