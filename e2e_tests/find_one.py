from e2e_tests.validator import validate_document, validate_args_list, args


def validate_find_one(timing, document, collection_nt, collection_sl):
    validate_args_list(timing, validate_document, collection_nt.find_one, collection_sl.find_one, [
        args({"_id": document["_id"]}, {"_id": True, "read": True, "region": True}),
        args(filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}),
        args({"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}),
        args({"_id": document["_id"]}, projection={"_id": True, "read": True}),
        args({"_id": document["_id"]}, projection={}),
        args({"_id": document["_id"]}, projection=[]),
        args(filter={"_id": document["_id"]}, projection={"_id": False}),
        args(filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True}),
        args(filter={"_id": document["_id"]}, projection={"_id": True, "read": True}),
        args(filter={"_id": document["_id"]}, projection={}),
        args(filter={"_id": document["_id"]}, projection=[]),
        args(filter={"_id": document["_id"]}, projection={"_id": False}),
        args({"_id": document["_id"]}),
        args(),
    ])
