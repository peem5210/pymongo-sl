from typing import Any

from pymongo.cursor import Cursor
from pymongo_sl.cursor import CursorSL
from pymongo_sl.mongo_client import MongoClientSL
from pymongo_sl.cache_client import LocalCacheClient, CacheClient
from pymongo import MongoClient  # type: ignore
from redis import Redis  # type: ignore
from tqdm import tqdm  # type: ignore
from util.func import ENV, load_env
import time
import numpy as np  # type: ignore

load_env()
redis = Redis(host=ENV("REDIS_HOST"), port=ENV("REDIS_PORT"), password=ENV("REDIS_PASSWORD"))


def find_tests(mongo_nt, mongo_sl, documents: list[dict[str, Any]]):
    global redis
    for x in documents: redis.delete(str(x["_id"]))

    collection_nt = mongo_nt[ENV("MONGODB_DATABASE")][ENV("MONGODB_COLLECTION")]
    collection_sl = mongo_sl[ENV("MONGODB_DATABASE")][ENV("MONGODB_COLLECTION")]
    # assert collection == collection_, "Collection should be equal"

    timing: list[float] = []
    result_list: list[dict[str, Any]] = []
    for document in tqdm(documents):
        validate(collection_nt.find, collection_sl.find, {"_id": document["_id"]},{"_id": True, "read": True, "region": True}, limit=10)
        validate(collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10)
        validate(collection_nt.find, collection_sl.find, {"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10)
        validate(collection_nt.find, collection_sl.find, {"_id": document["_id"]}, projection={"_id": True, "read": True}, limit=10)
        validate(collection_nt.find, collection_sl.find, {"_id": document["_id"]}, projection={}, limit=10)
        validate(collection_nt.find, collection_sl.find, {"_id": document["_id"]}, projection=[], limit=10)
        validate(collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": False}, limit=10)
        validate(collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10)
        validate(collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": True, "read": True}, limit=10)
        validate(collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={}, limit=10)
        validate(collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection=[], limit=10)
        validate(collection_nt.find, collection_sl.find, filter={"_id": document["_id"]}, projection={"_id": False}, limit=10)
        validate(collection_nt.find, collection_sl.find, {"_id": document["_id"]}, limit=10)
        validate(collection_nt.find, collection_sl.find, limit=10)
    return result_list, timing


def find_one_tests(mongo_client: MongoClient, documents: list[dict[str, Any]]):
    global redis

    collection = mongo_client[ENV("MONGODB_DATABASE")][ENV("MONGODB_COLLECTION")]
    collection_ = mongo_client.mongo.mongo

    timing: list[float] = []
    result_list: list[dict[str, Any]] = []
    for document in tqdm(documents):
        timing_and_append_result(timing, result_list, collection.find_one, {"_id": document["_id"]}, {"_id": True, "read": True, "region": True})
        timing_and_append_result(timing, result_list, collection.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id": document["_id"]}, projection={"_id": True, "read": True})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id": document["_id"]}, projection={})
        timing_and_append_result(timing, result_list, collection.find_one, {"_id": document["_id"]}, projection=[])
        timing_and_append_result(timing, result_list, collection_.find_one, filter={"_id": document["_id"]}, projection={"_id": False})
        timing_and_append_result(timing, result_list, collection_.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
        timing_and_append_result(timing, result_list, collection_.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True})
        timing_and_append_result(timing, result_list, collection_.find_one, filter={"_id": document["_id"]}, projection={})
        timing_and_append_result(timing, result_list, collection_.find_one, filter={"_id": document["_id"]}, projection=[])
        timing_and_append_result(timing, result_list, collection_.find_one, filter={"_id": document["_id"]}, projection={"_id": False})
        timing_and_append_result(timing, result_list, collection_.find_one, {"_id": document["_id"]})
        timing_and_append_result(timing, result_list, collection_.find_one)

    # clear cache

    return result_list, timing

def validate(func_nt, func_sl, *args, **kwargs):
    result_nt = [x for x in func_nt(*args, **kwargs)]
    result_sl = [x for x in func_sl(*args, **kwargs)]
    assert  result_nt == result_sl, f"With {args = } and {kwargs = }, the assertion is failed \n {result_nt = }, {result_sl = } "

def timing_and_append_result(t, l, func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    if isinstance(result, CursorSL) or isinstance(result, Cursor):
        temp = []
        for x in result:
            temp.append(x)
        result = temp
    stop = time.perf_counter()
    t.append(stop - start)
    l.append(result)
    return l, t


def validate_and_measure(result_nt, timing_nt, result_sl, timing_sl):
    print(f"{np.average(timing_nt) = } {np.average(timing_sl) = }")
    for idx, sl in enumerate(result_sl):
        nt = result_nt[idx]
        assert nt == sl, f"{nt} should equal to {sl} at {idx = }"


if __name__ == '__main__':
    mongo_nt = MongoClient(
        host=ENV("MONGODB_HOST_SGP_1"),
        port=ENV("MONGODB_PORT", True),
        username=ENV("MONGODB_USERNAME"),
        password=ENV("MONGODB_PASSWORD"),
    )
    cache_client = LocalCacheClient()
    # cache_client = CacheClient(client=redis)
    mongo_sl = MongoClientSL(
        host=ENV("MONGODB_HOST_SGP_1"),
        port=ENV("MONGODB_PORT", True),
        username=ENV("MONGODB_USERNAME"),
        password=ENV("MONGODB_PASSWORD"),
        cache_client=cache_client
    )

    documents = [x for x in mongo_nt.mongo.mongo.find(filter={"region": "SGP", "read": False}, limit=2)]
    find_tests(mongo_nt, mongo_sl, documents)

    result_nt, timing_nt = find_one_tests(mongo_nt, documents)
    result_sl, timing_sl = find_one_tests(mongo_sl, documents)
    validate_and_measure(result_nt, timing_nt, result_sl, timing_sl)
