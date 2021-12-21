from pymongo import MongoClient

from e2e.report import Report
from pymongo_sl import MongoClientSL
from pymongo_sl.cache_client import LocalCacheClient
from e2e.test.find import validate_find
from e2e.test.find_one import validate_find_one
from e2e.test.find_and_modify import validate_find_and_modify
from e2e.test.update_many import validate_update_many
from e2e.test.update_one import validate_update_one
from util.func import env, load_env


def run(collection_nt, collection_sl, documents):
    report = Report()
    for document in documents:
        """for functions that queried one document should be in here"""
        validate_find_one(report, document, collection_nt, collection_sl)
        validate_find_and_modify(report, document, collection_nt, collection_sl)
        validate_update_one(report, document, collection_nt, collection_sl)
    """functions that query multiple documents"""
    validate_find(report, documents, collection_nt, collection_sl)
    validate_update_many(report, None, collection_nt, collection_sl)
    print(report)






if __name__ == '__main__':
    # TODO: before executing the script
    #   1. Create .dev.env
    #   2. Please run local MongoDB via ./docker-compose.yaml
    #   3. Please populate MongoDB with `python3 rcache_profiling.py --populate`

    load_env(name='.dev')
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

    collection_nt = mongo_nt[env("MONGODB_DATABASE")][env("MONGODB_COLLECTION")]
    collection_sl = mongo_sl[env("MONGODB_DATABASE")][env("MONGODB_COLLECTION")]

    run(collection_nt, collection_sl, documents)
