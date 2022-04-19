from e2e.validator import validate_document
from pymongo_sl.keywords import KW

def validate_find_and_modify(report, document, collection_nt, collection_sl):
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, query={"_id": document["_id"]}, fields={"_id": True, "read": True, KW.region: True}, update={"$set": {"hello": "world"}})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, query={"_id": document["_id"]}, fields={"_id": True, "read": True, KW.region: True}, update={"$set": {"hello": "world"}}, enable_cache=True)
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, {"_id": document["_id"]}, {"$set": {"hello": "world"}},fields={"_id": True, "read": True, KW.region: True})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, {"_id": document["_id"]}, {"$set": {"hello": "world"}}, fields={"_id": True, "read": True})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, {"_id": document["_id"]}, {"$set": {"hello": "world"}}, fields={})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, {"_id": document["_id"]}, {"$set": {"hello": "world"}}, fields=[])
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, query={"_id": document["_id"]}, fields={"_id": False}, update={"$set": {"hello": "world"}})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, query={"_id": document["_id"]}, fields={"_id": True, "read": True, KW.region: True},update={"$set": {"hello": "world"}})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, query={"_id": document["_id"]}, fields={"_id": True, "read": True}, update={"$set": {"hello": "world"}})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, query={"_id": document["_id"]}, fields={}, update={"$set": {"hello": "world"}})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, query={"_id": document["_id"]}, fields=[], update={"$set": {"hello": "world"}})
    validate_document(report, collection_nt.find_and_modify, collection_sl.find_and_modify, query={"_id": document["_id"]}, fields={"_id": False}, update={"$set": {"hello": "world"}})
