import time
import numpy as np
from tqdm import tqdm
from pymongo import MongoClient

from pymongo_sl import MongoClientSL
from pymongo_sl.cache_client import LocalCacheClient
from util.func import env, load_env


load_env()
# redis = Redis(host=ENV("REDIS_HOST"), port=ENV("REDIS_PORT"), password=ENV("REDIS_PASSWORD"))


def tests(mongo_nt, mongo_sl, documents):
    # global redis
    # for x in documents:
    #     redis.delete(str(x["_id"]))

    collection_nt = mongo_nt[env("MONGODB_DATABASE")][env("MONGODB_COLLECTION")]
    collection_sl = mongo_sl[env("MONGODB_DATABASE")][env("MONGODB_COLLECTION")]

    timing = []
    for document in tqdm(documents):
        validate(timing, collection_nt.find, collection_sl.find, {"_id": document["_id"]}, {"_id": True, "read": True, "region": True}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, {"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, {"_id": document["_id"]}, projection={"_id": True, "read": True}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, {"_id": document["_id"]}, projection={}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, {"_id": document["_id"]}, projection=[], limit=10)
        validate(timing, collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": False}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": True, "read": True}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection=[], limit=10)
        validate(timing, collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": False}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, {"_id": document["_id"]}, limit=10)
        validate(timing, collection_nt.find, collection_sl.find, limit=10)

        validate(timing, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, {"_id": True, "read": True, "region": True})
        validate(timing, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
        validate(timing, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
        validate(timing, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, projection={"_id": True, "read": True})
        validate(timing, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, projection={})
        validate(timing, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, projection=[])
        validate(timing, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": False})
        validate(timing, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
        validate(timing, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True})
        validate(timing, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={})
        validate(timing, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection=[])
        validate(timing, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": False})
        validate(timing, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]})
        validate(timing, collection_nt.find_one, collection_sl.find_one)

    return [x for x, _ in timing], [x for _, x in timing]


def validate(timing, func_nt, func_sl, *args, **kwargs):
    start = time.perf_counter()
    result_nt = [x for x in func_nt(*args, **kwargs)]
    mid = time.perf_counter()
    result_sl = [x for x in func_sl(*args, **kwargs)]
    last = time.perf_counter()
    assert result_nt == result_sl, f"With {args = } and {kwargs = }, the assertion is failed \n " \
                                   f"{result_nt = } should equal to {result_sl = }"
    return timing.append((mid-start, last-mid))


def measure(timing_nt, timing_sl):
    print(f"{np.average(timing_nt) = } {np.average(timing_sl) = }")


if __name__ == '__main__':
    mongo_nt = MongoClient(
        host=env("MONGODB_HOST_SGP_1"),
        port=env("MONGODB_PORT", True),
        username=env("MONGODB_USERNAME"),
        password=env("MONGODB_PASSWORD"),
    )
    cache_client = LocalCacheClient()
    # cache_client = CacheClient(client=redis)
    mongo_sl = MongoClientSL(
        host=env("MONGODB_HOST_SGP_1"),
        port=env("MONGODB_PORT", True),
        username=env("MONGODB_USERNAME"),
        password=env("MONGODB_PASSWORD"),
        cache_client=cache_client
    )

    documents = [x for x in mongo_nt.mongo.mongo.find(filter={"region": "SGP", "read": False}, limit=2)]
    timing_nt, timing_sl = tests(mongo_nt, mongo_sl, documents)
    measure(timing_nt, timing_sl)
