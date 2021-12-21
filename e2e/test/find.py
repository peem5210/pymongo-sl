from e2e.validator import validate_cursor


def validate_find(report, documents, collection_nt, collection_sl):
    validate_cursor(report, collection_nt.find, collection_sl.find, {"_id": True, "read": True, "region": True}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, {"read": False}, projection={"_id": True, "read": True, "region": True}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, {}, projection={"_id": True, "read": True, "region": True}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, {"read": False}, projection={"_id": True, "read": True}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, {"read": False}, projection={}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, {"read": False}, projection=[], limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, {"group": 2}, projection={"_id": False}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"group": 2}, projection={"_id": True, "read": True, "region": True}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"group": 2}, projection={"_id": True, "read": True}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"group": 2}, projection={}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"group": 2}, projection=[], limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"group": 2}, projection={"_id": False}, limit=10)
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"group": 2}, limit=10)
    # TODO: Investigate performance issues with below functions on local env
    #  Suspect that with 'region' field applied to local mongo cluster without sharding, the performance would be bad
    #  compared to those normal query
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"_id": {"$in": [doc['_id'] for doc in documents][:10]}}, same_region=True)
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"_id": {"$in": [doc['_id'] for doc in documents][:10]}}, same_region=False)
    validate_cursor(report, collection_nt.find, collection_sl.find, filter={"_id": {"$nin": [doc['_id'] for doc in documents][:10]}}, same_region=True)
    validate_cursor(report, collection_nt.find, collection_sl.find, limit=10)
