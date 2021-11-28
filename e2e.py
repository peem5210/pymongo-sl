import time
import numpy as np
from tqdm import tqdm
from pymongo import MongoClient

from pymongo_sl import MongoClientSL
from pymongo_sl.cache_client import LocalCacheClient
from util.func import env, load_env


load_env(name='.dev')


def run(mongo_nt, mongo_sl, documents):
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

    validate_update(timing, collection_nt.update_many, collection_sl.update_many, filter={"group": 2}, update={'$set': {"read": True}})

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


def validate_update(timing, func_nt, func_sl, filter, *args, **kwargs):
    start = time.perf_counter()
    result_nt = func_nt(filter, *args, **kwargs)
    mid1 = time.perf_counter()

    # TODO: undo an update at this step, so the update will be applied to the same data on both sl and nt

    mid2 = time.perf_counter()
    result_sl = func_sl(filter, *args, **kwargs)
    last = time.perf_counter()

    # TODO: undo an update at this step as well

    # TODO: add a proper validation to make sure that the updated data is the same,
    #  for now the validation only check for the number of `matched_count` and `modified_count` only
    print([result_nt.matched_count, result_nt.modified_count], [result_sl.matched_count, result_sl.modified_count])
    assert [result_nt.matched_count, result_nt.modified_count] == [result_sl.matched_count, result_sl.modified_count], f"With {args = } and {kwargs = }, the assertion is failed \n " \
                                   f"{result_nt = } should equal to {result_sl = }"
    return timing.append((mid1-start, last-mid2))


def measure(timing_nt, timing_sl):
    print(f"{np.average(timing_nt) = } {np.average(timing_sl) = }")


if __name__ == '__main__':
    # TODO: before executing the script VVV
    """Prerequisites:
        1. Setup .dev.env
        2. Please run local MongoDB via ./docker-compose.yaml
        3. Please populate MongoDB with `python3 research/rcache_profiling.py --populate`
    """

    mongo_nt = MongoClient(
        host=env("MONGODB_HOST_SGP_1"),
        port=env("MONGODB_PORT", True),
        username=env("MONGODB_USERNAME"),
        password=env("MONGODB_PASSWORD"),
    )
    cache_client = LocalCacheClient()
    mongo_sl = MongoClientSL(
        host=env("MONGODB_HOST_SGP_1"),
        port=env("MONGODB_PORT", True),
        username=env("MONGODB_USERNAME"),
        password=env("MONGODB_PASSWORD"),
        cache_client=cache_client
    )

    documents = [x for x in mongo_nt.mongo.mongo.find(filter={"region": "SGP", "read": False}, limit=2)]
    timing_nt, timing_sl = run(mongo_nt, mongo_sl, documents)
    measure(timing_nt, timing_sl)
