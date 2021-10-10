from enum import Enum
import os
import time
import random
import numpy as np
from redis import Redis
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId
from tqdm import tqdm

from enum import Enum, auto

from rcache.rcache import Rcache
from util.func import *
class CacheMode(Enum):
    LOCAL = auto()
    REDIS = auto()
    NOT_EXIST = auto()

class RcacheProfiling:
    def __init__(self):
        self.redis = Redis(
            host = ENV("REDIS_HOST"), 
            port = ENV("REDIS_PORT", True),
            password= ENV("REDIS_PASSWORD")
        )
        self.mongo = MongoClient(ENV("MONGODB_HOST"), ENV("MONGODB_PORT", True))
        self.db = self.mongo[ENV("MONGODB_DATABASE")]
        self.collection = self.db[ENV("MONGODB_COLLECTION")]
        self.local_cache = {}
    def get_cache(self, objectid: str):
        if CACHE_MODE == CacheMode.REDIS:
            return self.redis.get(str(objectid))
        elif CACHE_MODE == CacheMode.LOCAL:
            return self.local_cache[str(objectid)]
        else:
            raise Exception("Exhuasive handling exception")   
        
    def populate_cache(self, object_list: list[tuple[ObjectId, str]]):
        if CACHE_MODE == CacheMode.REDIS:
            for _id, region in tqdm(object_list):
                self.redis.set(str(_id), region)
        elif CACHE_MODE == CacheMode.LOCAL:
            for _id, region in tqdm(object_list):
                self.local_cache[str(_id)] = region 
        else:
            raise Exception("Exhuasive handling exception")       

    def run(self):
        #Load object to memory
        print("fetching all object from the DB")
        object_list = [(x['_id'], x['region']) for x in self.collection.find(limit=NUM_SELECT_FROM_DB, projection=["_id", "region"])]
        print("finished")
        #Load object to cache
        self.populate_cache(object_list)

        
        #Randomly read object from mongo
        max_iter = len(object_list)-1
        print("starting test for loop...")
        read_time = []
        update_time = []
        for _ in tqdm(range(NUM_RANDOM)): 
            
            rint = random.randint(0, max_iter)
            object_id = object_list[rint][0] 
            
            #Read or Update
            
            rint = random.randint(0,1)
            region = None
            if rint == 0:
                s1 = time.perf_counter()
                region = self.get_cache(object_id)
                self.read_object(object_id, region=region)
                s2 = time.perf_counter()
                read_time.append(s2-s1)
            elif rint == 1:
                s1 = time.perf_counter()
                region = self.get_cache(object_id)
                self.update_object(object_id,{"$set":{"region":False}} , region=region)
                s2 = time.perf_counter()
                update_time.append(s2-s1)
                
            
        "update object"
        
        "delete object"
        
        print(f"\nfinished \n\nresult:\nread avg:{np.average(read_time)}\nupdate avg:{np.average(update_time)}")

    
    def create_object(self):
        "In case of object creation, caching won't help"
        "In Snaplogic, we would read parent object and retreive the region from the parent object"
        pass
    
    def read_object(self, _id: ObjectId, region: str = None):
        if region is not None:
            return self.collection.find_one({"_id":_id, "region":region})    
        return self.collection.find_one({"_id":_id})    
    
    def update_object(self, _id: ObjectId, update: dict, region: str = None):
        """update:: expr should be something like {'$set':{"read":True}}"""
        if region is not None:
            return self.collection.update_one({"_id":_id, "region":region}, update)
        return self.collection.update_one({"_id":_id}, update)
    
    def delete_object(self, _id: ObjectId):
        "test for delete is pass for now"
        pass
        # return self.collection.find_one_and_delete({"_id:":_id})
    
def populate_mongo_task(rcache: RcacheProfiling):
    region = REGIONS[random.randint(0, len(REGIONS)-1)]
    rcache.collection.insert_one({"region":region, "read":False})

def populate_mongo(rcache: RcacheProfiling):
    # with futures.ProcessPoolExecutor() as pool:
        for _ in range(NUM_INSERT_TO_DB):
            # future_result = pool.submit(populate_mongo_task, rcache)  
            populate_mongo_task(rcache)
            # future_result.add_done_callback(total_error)

REGIONS = ['SGP', 'ORE']
NUM_SELECT_FROM_DB = 10000
NUM_RANDOM = 1000
CACHE_MODE = CacheMode.LOCAL


NUM_INSERT_TO_DB = 10

if __name__ == '__main__':
    print(f'''Running with configuration\n\n{REGIONS = }\n{NUM_SELECT_FROM_DB = }\n{NUM_RANDOM = }\n{CACHE_MODE = }\n\n{NUM_INSERT_TO_DB = }\n\n\n''')
    load_dotenv(os.path.join(os.path.dirname('./'), '.env'))
    
    import cProfile
    import pstats
    rcache = RcacheProfiling()
    with cProfile.Profile() as pr:
        rcache.run()
    
    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    # stats.print_stats()
    stats.dump_stats(filename="rcache_profiling.prof")
    
    
    ""
    # populate_mongo(rcache)
    # print([x for x in rcache.collection.find({"region":"TEST_REGION"})])
    