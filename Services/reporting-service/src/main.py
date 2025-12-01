import os
import logging
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pythonjsonlogger import jsonlogger

# --- 1. Log settings ---
logger = logging.getLogger("reporting-service")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# --- 2. Configuration ---
MONGO_URI = "mongodb://mongodb:27017/"

# --- 3. DB connection ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["therapy_db"]
analysis_collection = db["analysis_results"]

app = FastAPI()

@app.get("/analysis/{video_id}")
async def get_analysis(video_id: str):
    logger.info("Fetching analysis request", extra={"video_id": video_id})

    # Search the database
    result = analysis_collection.find_one({"video_id": video_id})

    if not result:
        logger.warning("Analysis not found", extra={"video_id": video_id})
        raise HTTPException(status_code=404, detail="Analysis not found")

    # Convert the Mongo ObjectID to a string (because JSON doesn't recognize it)
    result["_id"] = str(result["_id"])

    logger.info("Analysis returned successfully", extra={"video_id": video_id})
    return result

@app.get("/videos")
async def list_videos():
    """Returns a list of all analyzed videos"""
    cursor = analysis_collection.find({}, {"video_id": 1, "timestamp": 1, "_id": 0})
    videos = list(cursor)
    return {"count": len(videos), "videos": videos}