from e2e_tests.validator import validate_cursor, validate_args_list, args


def validate_find(timing, documents, collection_nt, collection_sl):
    validate_args_list(timing, validate_cursor, collection_nt.find, collection_sl.find, [
        args({"_id": True, "read": True, "region": True}, limit=10),
        args({"read": False}, projection={"_id": True, "read": True, "region": True}, limit=10),
        args({}, projection={"_id": True, "read": True, "region": True}, limit=10),
        args({"read": False}, projection={"_id": True, "read": True}, limit=10),
        args({"read": False}, projection={}, limit=10),
        args({"read": False}, projection=[], limit=10),
        args({"group": 2}, projection={"_id": False}, limit=10),
        args(filter={"group": 2}, projection={"_id": True, "read": True, "region": True}, limit=10),
        args(filter={"group": 2}, projection={"_id": True, "read": True}, limit=10),
        args(filter={"group": 2}, projection={}, limit=10),
        args(filter={"group": 2}, projection=[], limit=10),
        args(filter={"group": 2}, projection={"_id": False}, limit=10),
        args(filter={"group": 2}, limit=10),
        # TODO: Investigate performance issues with below function with local test
        #  Suspect that with 'region' field applied to local mongo cluster without shard, the performance would be bad
        #  compared to those normal query
        args(filter={"_id": {"$in": [doc['_id'] for doc in documents][:10]}}, same_region=True, exc_kwargs=['same_region']),
        args(filter={"_id": {"$in": [doc['_id'] for doc in documents][:10]}}, same_region=False, exc_kwargs=['same_region']),
        args(filter={"_id": {"$nin": [doc['_id'] for doc in documents][:10]}}, same_region=True, exc_kwargs=['same_region']),
        args(limit=10)
    ])
