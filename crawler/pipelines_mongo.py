import os
import time
from typing import Any, Dict
from pymongo import MongoClient, UpdateOne

# collection.create_index([("body_id", 1)])
# collection.create_index([("body", 1)])

class MongoPipeline:
    def __init__(self, uri: str, db_name: str, coll_name: str):
        self.uri = uri
        self.db_name = db_name
        self.coll_name = coll_name
        self.client = None
        self.coll = None

    @classmethod
    def from_crawler(cls, crawler):
        uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        db = os.getenv("MONGO_DB", "kedra")
        coll = os.getenv("MONGO_COLLECTION", "decisions")
        return cls(uri, db, coll)

    def open_spider(self, spider):
        self.client = MongoClient(self.uri, connect=True)
        self.coll = self.client[self.db_name][self.coll_name]
        self.coll.create_index("identifier", unique=False)
        self.coll.create_index([("identifier", 1), ("detail_url", 1)], unique=True)

    def close_spider(self, spider):
        if self.client:
            self.client.close()

    def process_item(self, item, spider):
        doc: Dict[str, Any] = dict(item)
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        doc["updated_at"] = now
        filt = {"identifier": doc.get("identifier"), "detail_url": doc.get("detail_url")}
        update = {"$set": doc, "$setOnInsert": {"first_seen": now}}
        self.coll.update_one(filt, update, upsert=True)
        return item
