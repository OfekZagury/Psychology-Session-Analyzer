import os
import json
import hashlib
import pika
import logging
import redis
from pymongo import MongoClient
from openai import OpenAI
from pythonjsonlogger import jsonlogger

# --- 1. Log settings ---
logger = logging.getLogger("analysis-service")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# --- 2. Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "guest")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
MONGO_URI = "mongodb://mongodb:27017/"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- 3. Customers ---
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["therapy_db"]
analysis_collection = db["analysis_results"]
openai_client = OpenAI(api_key=OPENAI_API_KEY)


def generate_cache_key(text):
    """Creates a unique identifier for the text so that we can check if we have analyzed it before"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def analyze_with_llm(transcript_text, utterances):
    """Sends the text to ChatGPT and requests a psychological analysis in JSON format"""

    system_prompt = """"
    You are an expert psychological therapist assistant.
    Analyze the following session transcript.
    1. Identify who is the Therapist and who is the Patient based on the conversation flow.
    2. For each sentence, determine the 'Topic' and the 'Emotion' (Happy, Sad, Anxious, Angry, Neutral, Hopeful).

    Return the output strictly in valid JSON format like this:
    {
        "participants": {"A": "Therapist", "B": "Patient"},
        "analysis": [
            {"speaker": "A", "text": "...", "topic": "...", "emotion": "..."},
            {"speaker": "B", "text": "...", "topic": "...", "emotion": "..."}
        ],
        "summary": "Brief summary of the session"
    }
    """

    # We convert the speaker list to readable text for LLM
    conversation_str = ""
    for u in utterances:
        conversation_str += f"Speaker {u['speaker']}: {u['text']}\n"

    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo-1106",  # Fast model that supports JSON
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": conversation_str}
        ]
    )

    return json.loads(response.choices[0].message.content)


def process_analysis(ch, method, properties, body):
    message = json.loads(body)
    video_id = message['video_id']
    transcript_text = message['text']
    utterances = message['utterances']

    logger.info("Starting analysis", extra={"video_id": video_id})

    try:
        # 1. Checking the Redis Cache
        # We check based on the content of the conversation. If we have analyzed this conversation before - we will take it from there.
        cache_key = generate_cache_key(transcript_text)
        cached_result = redis_client.get(cache_key)

        final_analysis = None

        if cached_result:
            logger.info("Cache HIT - Fetching from Redis", extra={"video_id": video_id})
            final_analysis = json.loads(cached_result)
        else:
            logger.info("Cache MISS - Calling OpenAI", extra={"video_id": video_id})
            # 2. Calling LLM
            llm_result = analyze_with_llm(transcript_text, utterances)

            # Building the final object for saving
            final_analysis = {
                "video_id": video_id,
                "timestamp": message.get('timestamp', None),
                "raw_transcript": transcript_text,
                "ai_analysis": llm_result
            }

            # 3. Saving in Redis (for 24 hours)
            redis_client.setex(cache_key, 86400, json.dumps(final_analysis))

        # 4. Save to MongoDB (the permanent database)
        # We use update_one with upsert=True to avoid duplicates
        analysis_collection.update_one(
            {"video_id": video_id},
            {"$set": final_analysis},
            upsert=True
        )
        logger.info("Saved to MongoDB", extra={"video_id": video_id})

        # 5. End message (optional - if there was an additional service, e.g. sending an email)
        ch.basic_publish(
            exchange='',
            routing_key='analysis_completed',
            body=json.dumps({"video_id": video_id, "status": "ready"}),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Analysis complete", extra={"video_id": video_id})

    except Exception as e:
        logger.error("Analysis failed", extra={"error": str(e), "video_id": video_id})
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    logger.info("Analysis Service Started")

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, heartbeat=600)

    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            break
        except pika.exceptions.AMQPConnectionError:
            import time
            time.sleep(5)

    channel.queue_declare(queue='transcription_completed', durable=True)
    channel.queue_declare(queue='analysis_completed', durable=True)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='transcription_completed', on_message_callback=process_analysis)

    logger.info("Waiting for transcripts...")
    channel.start_consuming()


if __name__ == "__main__":
    main()

