from e2e_tests.validator import validate_cursor, validate_args_list, args


def validate_find(timing, document, collection_nt, collection_sl):
    validate_args_list(timing, validate_cursor, collection_nt.find, collection_sl.find, [
        args({"_id": document["_id"]}, {"_id": True, "read": True, "region": True}, limit=10),
        args(filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10),
        args({"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10),
        args({"_id": document["_id"]}, projection={"_id": True, "read": True}, limit=10),
        args({"_id": document["_id"]}, projection={}, limit=10),
        args({"_id": document["_id"]}, projection=[], limit=10),
        args(filter={"_id": document["_id"]}, projection={"_id": False}, limit=10),
        args(filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}, limit=10),
        args(filter={"_id": document["_id"]}, projection={"_id": True, "read": True}, limit=10),
        args(filter={"_id": document["_id"]}, projection={}, limit=10),
        args(filter={"_id": document["_id"]}, projection=[], limit=10),
        args(filter={"_id": document["_id"]}, projection={"_id": False}, limit=10),
        args({"_id": document["_id"]}, limit=10),
        args(limit=10)
    ])
