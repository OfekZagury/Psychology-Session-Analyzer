import os
import time
import json
import uuid
import logging
from fastapi import FastAPI, UploadFile, File, HTTPException
from minio import Minio
from minio.error import S3Error
import pika
from pythonjsonlogger import jsonlogger

# --- 1. Logging Configuration ---
logger = logging.getLogger("upload-service")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# --- 2. Environment Variables ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "videos"

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "guest")

app = FastAPI()

# --- 3. MinIO Client Setup ---
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


# --- 4. RabbitMQ Helper ---
def publish_message(message: dict):
    """
    Connects to RabbitMQ, declares queue, publishes message, and closes connection.
    In production, you might keep a connection open, but this is safer for stateless REST APIs.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Declare the queue to ensure it exists
        channel.queue_declare(queue='video_queue', durable=True)

        # Publish
        channel.basic_publish(
            exchange='',
            routing_key='video_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        connection.close()
        logger.info("Message published to RabbitMQ", extra={"mq_payload": message})

    except Exception as e:
        logger.error("Failed to publish to RabbitMQ", extra={"error": str(e)})
        raise e


# --- 5. API Endpoints ---

@app.on_event("startup")
async def startup_event():
    """Ensure bucket exists on startup"""
    if not minio_client.bucket_exists(BUCKET_NAME):
        try:
            minio_client.make_bucket(BUCKET_NAME)
            logger.info(f"Bucket '{BUCKET_NAME}' created")
        except S3Error as e:
            logger.error("MinIO bucket check failed", extra={"error": str(e)})


@app.post("/upload")
async def upload_video(file: UploadFile = File(...)):
    video_id = str(uuid.uuid4())
    file_name = f"{video_id}.mp4"  # Assuming mp4 for simplicity, or preserve original extension

    logger.info("Received video upload request", extra={"original_filename": file.filename, "video_id": video_id})

    try:
        # 1. Upload to MinIO
        # UploadFile.file is a Python SpooledTemporaryFile, which is file-like
        # We need to know the size for MinIO put_object, or use -1 for part_size to auto-detect but put_object needs length
        # For simplicity in FastAPI, we can get size by seeking to end and back
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)

        minio_client.put_object(
            BUCKET_NAME,
            file_name,
            file.file,
            length=file_size,
            content_type=file.content_type
        )
        logger.info("Video uploaded to MinIO", extra={"bucket": BUCKET_NAME, "path": file_name})

        # 2. Publish to RabbitMQ
        message = {
            "video_id": video_id,
            "file_path": file_name,
            "original_filename": file.filename
        }
        publish_message(message)

        return {
            "message": "Video uploaded successfully",
            "video_id": video_id,
            "status": "processing_started"
        }

    except Exception as e:
        logger.error("Upload process failed", extra={"error": str(e)})
        raise HTTPException(status_code=500, detail=str(e))