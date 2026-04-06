import os
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference
from bson import ObjectId

app = FastAPI()

MONGODB_URI = os.getenv("MONGODB_URI")
if not MONGODB_URI:
    raise ValueError("Falta la variable de entorno MONGODB_URI")

client = MongoClient(MONGODB_URI)
db = client["ev_db"]
base_collection = db["vehicles"]

@app.get("/")
def root():
    return {"message": "EV API running"}

# 1) Fast but unsafe write
@app.post("/insert-fast")
def insert_fast(record: dict):
    try:
        fast_collection = base_collection.with_options(
            write_concern=WriteConcern(w=1)
        )
        result = fast_collection.insert_one(record)
        return {"inserted_id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 2) Highly durable write
@app.post("/insert-safe")
def insert_safe(record: dict):
    try:
        safe_collection = base_collection.with_options(
            write_concern=WriteConcern(w="majority")
        )
        result = safe_collection.insert_one(record)
        return {"inserted_id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 3) Strongly consistent read
@app.get("/count-tesla-primary")
def count_tesla_primary():
    try:
        primary_collection = base_collection.with_options(
            read_preference=ReadPreference.PRIMARY
        )
        total = primary_collection.count_documents({"Make": "TESLA"})
        if total == 0:
            total = primary_collection.count_documents({"Make": "Tesla"})
        return {"count": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 4) Eventually consistent analytical read
@app.get("/count-bmw-secondary")
def count_bmw_secondary():
    try:
        secondary_collection = base_collection.with_options(
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )
        total = secondary_collection.count_documents({"Make": "BMW"})
        if total == 0:
            total = secondary_collection.count_documents({"Make": "Bmw"})
        return {"count": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
