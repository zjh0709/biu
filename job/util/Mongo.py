import pymongo
from ..config import MONGODB_HOST, MONGODB_PORT, MONGODB_DB


client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
db = client.get_database(MONGODB_DB)
