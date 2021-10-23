from pymongo_sl.mongo_client import MongoClientSL as MongoClient
# from pymongo import MongoClient
from util.func import *

load_env()

mongo = MongoClient(host = ENV("MONGODB_HOST_SGP_1"), 
                    port = ENV("MONGODB_PORT", True), 
                    username = ENV("MONGODB_USERNAME"), 
                    password = ENV("MONGODB_PASSWORD"))

db = mongo[ENV("MONGODB_DATABASE")]
collection = db[ENV("MONGODB_COLLECTION")]

print(mongo.list_database_names())
# result = [x for x in collection.find(filter={"region":"SGP"}, limit=10)]

# print(result, collection.find_one(filter={"region":"SGP"}))
