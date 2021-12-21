from e2e.validator import validate_document


def validate_find_one(report, document, collection_nt, collection_sl):
    validate_document(report, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, {"_id": True, "read": True, "region": True})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, projection={"_id": True, "read": True})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, projection={})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]}, projection=[])
    validate_document(report, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": False})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True, "region": True})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": True, "read": True})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection=[])
    validate_document(report, collection_nt.find_one, collection_sl.find_one, filter={"_id": document["_id"]}, projection={"_id": False})
    validate_document(report, collection_nt.find_one, collection_sl.find_one, {"_id": document["_id"]})
    validate_document(report, collection_nt.find_one, collection_sl.find_one)
