import os
import sys
import time
import random

import argparse
import pandas as pd
from redis import Redis
from dotenv import load_dotenv

from pymongo import MongoClient
from pymongo.collection import Collection
# from pymongo_sl import MongoClientSL as MongoClient
# from pymongo_sl.collection import CollectionSL as Collection

from bson.objectid import ObjectId
from tqdm import tqdm
from enum import Enum, auto

# from util.func import *
def env(variable: str, to_int: bool = False):
    result: str = os.environ.get(variable, "")
    if to_int and isinstance(result, str):
        return int(result)
    assert result is not None, f"Environment variable '{variable}' not exist"
    return result

class CacheMode(Enum):
    LOCAL = auto()
    REDIS = auto()
    NO_CACHE = auto()
    IDEAL = auto()
    NOT_EXIST = auto()


class RcacheProfiling:
    def __init__(self, populate=False):
        if not populate:
            print("Connecting redis...")
            self.redis = Redis(
                host=env("REDIS_HOST"),
                port=env("REDIS_PORT", True),
                password=env("REDIS_PASSWORD")
            )
            print("Finished")
            print("Connecting mongo...")
        self.collection_connections: dict[str, Collection] = {}
        for prefix in MONGODB_HOST_PREFIXES:
            print(f'Connecting to {prefix}: {env(f"MONGODB_HOST_{prefix}")}')
            mongo = MongoClient(host=env(f"MONGODB_HOST_{prefix}"), port=env("MONGODB_PORT", True),
                                username=env("MONGODB_USERNAME"), password=env("MONGODB_PASSWORD"))
            db = mongo[env("MONGODB_DATABASE")]
            self.collection_connections[prefix] = (db[env("MONGODB_COLLECTION")])
        print("Finished")
        self.local_cache = {}
        self.region_object_list: map[str, list[(ObjectId, str)]] = {}

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

    def load_from_db(self, region):
        assert NUM_SELECT_FROM_DB < 1000000, f"{NUM_SELECT_FROM_DB < 1000000 = }"
        return [(x['_id'], x['region']) for x in
                list(self.collection_connections.values())[0].find(limit=NUM_SELECT_FROM_DB,
                                                                   skip=random.randint(0, 1000000 - NUM_SELECT_FROM_DB),
                                                                   projection=["_id", "region"],
                                                                   filter={"region": region})]

    def run_read_with_hit_percentage(self, interval, region, connection_str):
        CACHE_MODE = CacheMode.REDIS
        for r in REGIONS:
            if r != region:
                continue
            self.region_object_list[r] = self.load_from_db(r)
            self.populate_cache(self.region_object_list[r])

        connection = self.collection_connections[connection_str]
        result: list[(float, float, int, int)] = []
        for percent in list(range(0, 100 + interval, interval)) + [97, 99]:
            print(f"Testing with cache hit {percent = }")
            read_time = []
            summary = {True: 0, False: 0}
            for random_iter in tqdm(range(NUM_RANDOM)):
                is_hit = random_iter / NUM_RANDOM < percent / 100
                object_id, region = self.region_object_list[region][random.randint(0, NUM_SELECT_FROM_DB - 1)]
                s1 = time.perf_counter()
                summary[is_hit] += 1
                if CACHE_MODE != CacheMode.IDEAL:
                    cached_region = self.get_cache(object_id)
                    if is_hit:
                        self.read_object(connection, object_id, region=cached_region)
                    else:
                        self.read_object(connection, object_id)
                else:
                    self.read_object(connection, object_id, region=region)
                s2 = time.perf_counter()
                read_time.append(s2 - s1)
            read_avg = sum(read_time) / len(read_time)
            result.append((percent, read_avg * 1000, summary[True], summary[False]))
            print(summary)
            print(f"result:\nread avg ({connection_str})[{IS_CROSS_REGION = }]:{read_avg}\n")
        pd.DataFrame(result, columns=['hit_percentage', 'average_read_time_ms', 'num_hit', 'num_miss']).to_csv(
            f"./result/{OUTPUT}_hit_tests.csv")

    def run(self):
        # Load object to memory
        # print("fetching and populating all object from the DB")
        max_iter = NUM_SELECT_FROM_DB

        if len(self.region_object_list.keys()) == 0:
            for region in REGIONS:
                self.region_object_list[region] = self.load_from_db(region)
        for region in REGIONS:
            self.populate_cache(self.region_object_list[region])
        # print("finished")
        # print(self.region_object_list)

        # Randomly read object from mongo
        result = []
        for prefix, connection in self.collection_connections.items():
            summary = {}
            print(f"\n\ntesting on {prefix} connection")
            read_time = []
            update_time = []
            for _ in (range(NUM_RANDOM)):
                rint = random.randint(0, max_iter - 1)
                object_id, region = None, None
                if IS_CROSS_REGION:
                    if prefix.startswith("ORE"):
                        object_id, region = self.region_object_list["SGP"][rint]
                    elif prefix.startswith("SGP"):
                        object_id, region = self.region_object_list["ORE"][rint]
                    else:
                        print(f"WTF IS THIS {prefix}")
                else:
                    object_id, region = self.region_object_list[prefix[:3]][rint]

                if region not in summary:
                    summary[region] = 1
                else:
                    summary[region] += 1
                rint = random.randint(0, 1)

                if rint == 0:
                    s1 = time.perf_counter()
                    if CACHE_MODE != CacheMode.IDEAL:
                        self.read_object(connection, object_id, region=self.get_cache(object_id))
                    else:
                        self.read_object(connection, object_id, region=region)
                    s2 = time.perf_counter()
                    read_time.append(s2 - s1)
                elif rint == 1:
                    s1 = time.perf_counter()
                    if CACHE_MODE != CacheMode.IDEAL:
                        self.update_object(connection, object_id, {"$set": {"read": True}},
                                           region=self.get_cache(object_id))
                    else:
                        self.update_object(connection, object_id, {"$set": {"read": True}}, region=region)
                    s2 = time.perf_counter()
                    update_time.append(s2 - s1)
            "create object"
            "delete object"
            read_avg = sum(read_time) / len(read_time)
            update_avg = sum(update_time) / len(update_time)

            print(
                f"result:\nread avg ({prefix})[{IS_CROSS_REGION = }]:{read_avg}\nupdate avg ({prefix})[{IS_CROSS_REGION = }]:{update_avg}")
            print(summary)
            result.append((prefix, CACHE_MODE.name, IS_CROSS_REGION, read_avg, update_avg))
        return result

    def create_object(self):
        "In case of object creation, caching won't help"
        "In Snaplogic, we would read parent object and retreive the region from the parent object"
        pass

    def read_object(self, connection: Collection, _id: ObjectId, region: str = None):
        if region is not None:
            if type(region) == bytes:
                region = region.decode("utf-8")
            assert type(region) == str, f"type of region should be string: {type(region)}"
            return connection.find_one({"_id": _id, "region": region})
        return connection.find_one({"_id": _id})

    def update_object(self, connection: Collection, _id: ObjectId, update: dict, region: str = None):
        """update:: expr should be something like {'$set':{"read":True}}"""
        if region is not None:
            if type(region) == bytes:
                region = region.decode("utf-8")
            assert type(region) == str, f"type of region should be string: {type(region)}"
            return connection.update_one({"_id": _id, "region": region}, update)
        return connection.update_one({"_id": _id}, update)

    def delete_object(self, _id: ObjectId):
        "test for delete is pass for now"
        pass
        # return self.collection.find_one_and_delete({"_id:":_id})


def populate_mongo_task(rcache: RcacheProfiling):
    rcache.collection_connections["SGP_1"].insert_many(
        [{"region": REGIONS[random.randint(0, len(REGIONS) - 1)], "read": False, "group":random.randint(0, 10)} for _ in
         range(NUM_INSERT_TO_DB // 10)], ordered=False)


def populate_mongo(rcache: RcacheProfiling):
    print(f"populating mongodb with {NUM_INSERT_TO_DB // 10} documents per iteration")
    for _ in tqdm(range(10)):
        populate_mongo_task(rcache)


REGIONS = ['SGP', 'ORE']
NUM_SELECT_FROM_DB = 10_000
NUM_RANDOM = 1000
CACHE_MODE = CacheMode.LOCAL
MONGODB_HOST_PREFIXES = ["SGP_1", "ORE_1", "SGP_2", "ORE_2"]
# MONGODB_HOST_PREFIXES = ["ORE_1"]
NUM_INSERT_TO_DB = 10_000
IS_CROSS_REGION = False

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--select', '-s', default=10)
    parser.add_argument('--random-iter', '-r', default=10)
    parser.add_argument('--cache-mode', '-m', default="local")
    parser.add_argument('--populate', '-p', action='store_true')
    parser.add_argument('--cross-region', '-c', action='store_true')
    parser.add_argument('--all-test', '-a', action='store_true')
    parser.add_argument('--output', '-o', default='all_result')
    parser.add_argument('--connection', default='SGP_1,ORE_1')
    parser.add_argument('--hit-tests', '-ht', action='store_true')
    args = parser.parse_args(sys.argv[1:])
    print(args)
    HIT_TESTS = args.hit_tests
    MONGODB_HOST_PREFIXES = args.connection.split(',')
    NUM_SELECT_FROM_DB = int(args.select)
    NUM_RANDOM = int(args.random_iter)
    IS_CROSS_REGION = args.cross_region
    OUTPUT = args.output
    if args.cache_mode == "local":
        CACHE_MODE = CacheMode.LOCAL
    elif args.cache_mode == "redis":
        CACHE_MODE = CacheMode.REDIS
    elif args.cache_mode == "no_cache":
        CACHE_MODE = CacheMode.NO_CACHE
    elif args.cache_mode == "ideal":
        CACHE_MODE = CacheMode.IDEAL
    else:
        sys.exit(1)

    load_dotenv(os.path.join(os.path.dirname('./'), '.dev.env'))

    rcache = RcacheProfiling(args.populate)

    if args.populate:
        populate_mongo(rcache)
    elif args.all_test:
        result = []
        for CM in [CacheMode.NO_CACHE, CacheMode.LOCAL, CacheMode.REDIS, CacheMode.IDEAL]:
            CACHE_MODE = CM
            for ICR in [False, True]:
                IS_CROSS_REGION = ICR
                print(
                    f'''Running with configuration {NUM_SELECT_FROM_DB = }, {NUM_RANDOM = }\n{REGIONS = }, {CACHE_MODE = }''')
                result += rcache.run()

        pd.DataFrame(data=result, columns=["region", "cache_mode", "is_cross_region", "average_read_time",
                                           "average_update_time"]).to_csv(f"./result/{OUTPUT}_{NUM_RANDOM}.csv")
        #  result.append((prefix, CACHE_MODE.name, np.average(read_time), np.average(update_time)))
    elif args.hit_tests:
        rcache.run_read_with_hit_percentage(5, "SGP", "SGP_1")
    else:
        print(
            f'''Running with configuration\n\n{REGIONS = }\n{NUM_SELECT_FROM_DB = }\n{NUM_RANDOM = }\n{CACHE_MODE = }\n\n{NUM_INSERT_TO_DB = }\n\n\n''')
        rcache.run()
