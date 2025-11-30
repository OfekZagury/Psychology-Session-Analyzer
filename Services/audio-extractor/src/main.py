import os
import json
import pika
import logging
import tempfile
from minio import Minio
from moviepy.editor import VideoFileClip
from pythonjsonlogger import jsonlogger

# --- 1. Log settings ---
logger = logging.getLogger("audio-extractor")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# --- 2. Environment variables ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "guest")

# --- 3. Connecting to MinIO ---
minio_client = Minio(
MINIO_ENDPOINT,
access_key=MINIO_ACCESS_KEY,
secret_key=MINIO_SECRET_KEY,
secure=False
)

def process_video(ch, method, properties, body):
    """This is the function that is run every time a new message arrives"""

    # Convert the message from Bytes to Dict
    message = json.loads(body)
    video_id = message['video_id']
    video_path = message['file_path']

    logger.info("Started processing video", extra={"video_id": video_id})

    try:
        # A. Downloading the video from MinIO to a local temporary file
        # We create a temporary file so as not to pollute the disk
        temp_video = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        temp_audio = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
        temp_video.close()
        temp_audio.close()

        minio_client.fget_object("videos", video_path, temp_video.name)

        # B. Extracting the audio with MoviePy
        video_clip = VideoFileClip(temp_video.name)
        video_clip.audio.write_audiofile(temp_audio.name, logger=None) # logger=None Silences moviepy logs
        video_clip.close()

        # C. Upload the MP3 back to MinIO (to the audio bucket)
        audio_filename = f"{video_id}.mp3"
        minio_client.fput_object(
        "audio",
        audio_filename,
        temp_audio.name,
        content_type="audio/mpeg"
        )
        logger.info("Audio uploaded to MinIO", extra={"path": audio_filename})

        # D. Send a message to the next service (Transcription)
        next_message = {
        "video_id": video_id,
        "audio_path": audio_filename
        }

        ch.basic_publish(
        exchange='',
        routing_key='audio_extracted', # Next queue name
        body=json.dumps(next_message),
        properties=pika.BasicProperties(delivery_mode=2)
        )

        # E. Clean up temporary files
        os.remove(temp_video.name)
        os.remove(temp_audio.name)

        # F. RabbitMQ confirmation successfully completed! (ACK)
        # Only now the message will be deleted from the previous queue
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Processing complete", extra={"video_id": video_id})

    except Exception as e:
        logger.error("Failed to process video", extra={"error": str(e), "video_id": video_id})
        # In case of critical failure, we do not send an Ack so that the message returns to the queue (or goes to Dead Letter)
        # Here for simplicity we send a Nack (negative)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    logger.info("Audio Extractor Service Started")

    # Connection to RabbitMQ
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, heartbeat=600, blocked_connection_timeout=300)

    # Simple Retry mechanism for connection (in case the service comes up before Rabbit)
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            break
        except pika.exceptions.AMQPConnectionError:
            logger.warning("RabbitMQ not ready yet, retrying in 5 seconds...")
            import time
            time.sleep(5)

    # Declare the queues (make sure they exist)
    channel.queue_declare(queue='video_queue', durable=True)
    channel.queue_declare(queue='audio_extracted', durable=True)

    # Definition: Send me only one task at a time (to avoid overloading the processor)
    channel.basic_qos(prefetch_count=1)

    # Listen to the queue
    channel.basic_consume(queue='video_queue', on_message_callback=process_video)

    logger.info("Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()