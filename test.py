from bson.objectid import ObjectId
from pymongo_sl.mongo_client import MongoClientSL as MongoClient
from redis import Redis
# from pymongo import MongoClient
from util.func import *

load_env()

mongo = MongoClient(
                    host = ENV("MONGODB_HOST_SGP_1"), 
                    port = ENV("MONGODB_PORT", True), 
                    username = ENV("MONGODB_USERNAME"), 
                    password = ENV("MONGODB_PASSWORD"),
                    # redis_host = ENV("REDIS_HOST"),
                    # redis_port = ENV("REDIS_PORT", True),
                    # redis_password = ENV("REDIS_PASSWORD"),
                    redis_client = Redis(host=ENV("REDIS_HOST"), port=ENV("REDIS_PORT"), password=ENV("REDIS_PASSWORD"))
                    )

db = mongo[ENV("MONGODB_DATABASE")]
collection = db[ENV("MONGODB_COLLECTION")]

# print(mongo.list_database_names())
result = [x for x in collection.find(filter={"region":"SGP"}, limit=10)]
for r in result:
    print(r)
    
print("GETTING")
print(collection.find_one(filter={"_id":ObjectId("6163c701557e7a07a7461570")}))
