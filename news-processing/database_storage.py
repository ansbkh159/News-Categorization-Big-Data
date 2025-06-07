from pymongo import MongoClient
from os import environ
from dotenv import load_dotenv
from datetime import datetime, timezone, time


load_dotenv()

class NewsDB:
    def __init__(self, db_name="mydatabase"):
        self.uri = environ['MONGODB_URI']
        self.client = MongoClient(self.uri)
        self.db = self.client[db_name]
        self.news_collection = self.db["News"]
    
    def insert_news(self, content, category):
        document = {
            "content": content,
            "category": category,
            "created_at": datetime.now(timezone.utc)
        }
        result = self.news_collection.insert_one(document)
        return result.inserted_id

    def show_all_news(self):
        all_docs = self.news_collection.find()
        for doc in all_docs:
            print(doc)

    def find_after(self, timestamp: datetime):
        cursor = self.news_collection.find({"created_at": {"$gt": timestamp}})
        return list(cursor)



if __name__ == "__main__":
    news_db = NewsDB()
    
    # new_id = news_db.insert_news("Breaking news: Python class created!", "Programming")
    # print(f"Inserted document with id: {new_id}")
    fixed_time = datetime.combine(datetime.now(timezone.utc).date(), time(13, 33, tzinfo=timezone.utc))

    recent_docs = news_db.find_after(fixed_time)

    for doc in recent_docs:
        print(doc)