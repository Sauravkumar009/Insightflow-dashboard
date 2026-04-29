from pymongo import MongoClient

uri = "mongodb+srv://2025aim1009_db_user:lOuGs2tZMyhwVHK6@cluster0.ov7v1g2.mongodb.net/insightflow?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(uri)
db = client["insightflow"]

print("Connected!")
print("Collections:", db.list_collection_names())
print()
for col in db.list_collection_names():
    count = db[col].count_documents({})
    sample = db[col].find_one({}, {"_id": 0})
    print(f"  {col}: {count} documents")
    print(f"  Sample: {sample}")
    print()

client.close()