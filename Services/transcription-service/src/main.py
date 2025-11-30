import os
import json
import time
import pika
import logging
import requests
import tempfile
from minio import Minio
from pythonjsonlogger import jsonlogger

# --- 1. Log settings ---
logger = logging.getLogger("transcription-service")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# --- 2. Configuration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "guest")

ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")

# --- 3. Clients ---
minio_client = Minio(
MINIO_ENDPOINT,
access_key=MINIO_ACCESS_KEY,
secret_key=MINIO_SECRET_KEY,
secure=False
)

def upload_to_assemblyai(file_path):
    """Uploads the physical file to AssemblyAI servers so they can access it"""
    headers = {'authorization': ASSEMBLYAI_API_KEY}

    def read_file(path):
        with open(path, 'rb') as f:
            while True:
                data = f.read(5242880) # Read in chunks of 5MB
                if not data:
                    break
                yield data

    response = requests.post(
    'https://api.assemblyai.com/v2/upload',
    headers=headers,
    data=read_file(file_path)
    )
    response.raise_for_status()
    return response.json()['upload_url']

def transcribe_audio(upload_url):
    """Sends a transcription request and includes speaker identification"""
    headers = {
    "authorization": ASSEMBLYAI_API_KEY,
    "content-type": "application/json"
    }

    json_data = {
    "audio_url": upload_url,
    "speaker_labels": True, # distinguishes between a therapist and a patient
    "language_code": "en"
    }

    response = requests.post(
    'https://api.assemblyai.com/v2/transcript',
    json=json_data,
    headers=headers
    )
    response.raise_for_status()
    return response.json()['id']

def wait_for_result(transcript_id):
    """Waiting for the processing to finish (Polling)"""
    headers = {'authorization': ASSEMBLYAI_API_KEY}

    while True:
        response = requests.get(
        f'https://api.assemblyai.com/v2/transcript/{transcript_id}',
        headers=headers
        )
        response.raise_for_status()
        data = response.json()

        if data['status'] == 'completed':
            return data
        elif data['status'] == 'error':
            raise Exception(f"Transcription failed: {data['error']}")

        logger.info("Waiting for transcription...", extra={"status": data['status']})
        time.sleep(5) # We'll check again in 5 seconds

def process_audio(ch, method, properties, body):
    message = json.loads(body)
    video_id = message['video_id']
    audio_path = message['audio_path']

    logger.info("Starting transcription process", extra={"video_id": video_id})

    try:
        # 1. Downloading the MP3 from MinIO to a temporary file
        temp_audio = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
        temp_audio.close()

        minio_client.fget_object("audio", audio_path, temp_audio.name)

        # 2. Uploading to AssemblyAI
        logger.info("Uploading to AssemblyAI...", extra={"video_id": video_id})
        upload_url = upload_to_assemblyai(temp_audio.name)

        # 3. Starting transcription
        logger.info("Requesting transcription...", extra={"video_id": video_id})
        transcript_id = transcribe_audio(upload_url)

        # 4. Waiting for the result
        result_data = wait_for_result(transcript_id)

        # 5. Extracting only the important information (the text and the speakers)
        # We only forward the utterances (who said what and when)
        transcript_output = {
        "video_id": video_id,
        "text": result_data['text'],
        "utterances": result_data['utterances'] # List of {speaker: 'A', text: 'Hello', ...}
        }

        # 6. Sending to RabbitMQ (to the next service: Analysis)
        ch.basic_publish(
        exchange='',
        routing_key='transcription_completed',
        body=json.dumps(transcript_output),
        properties=pika.BasicProperties(delivery_mode=2)
        )

        # 7. Cleaning
        os.remove(temp_audio.name)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Transcription completed and published", extra={"video_id": video_id})

    except Exception as e:
        logger.error("Transcription failed", extra={"error": str(e), "video_id": video_id})
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    logger.info("Transcription Service Started")

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, heartbeat=600)

    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            break
        except pika.exceptions.AMQPConnectionError:
            time.sleep(5)

    channel.queue_declare(queue='audio_extracted', durable=True)
    channel.queue_declare(queue='transcription_completed', durable=True)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='audio_extracted', on_message_callback=process_audio)

    logger.info("Waiting for audio to transcribe...")
    channel.start_consuming()


if __name__ == "__main__":
    main()