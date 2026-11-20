"""Unit tests for process_analysis (the Redis cache path).

Redis, MongoDB, RabbitMQ and the LLM are replaced by in-memory fakes, so the
tests need no running containers and no API keys.

Run from services/analysis-service:
    pip install -r requirements.txt
    python -m unittest discover tests
"""
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# main.py creates its clients at import time; patch them so importing it opens no connections.
with mock.patch("redis.Redis"), mock.patch("pymongo.MongoClient"), mock.patch("openai.OpenAI"):
    import main


LLM_RESULT = {
    "participants": {"A": "Therapist", "B": "Patient"},
    "summary": "The patient talks about finally confronting their mother.",
    "breakthroughs": [],
    "emotional_triggers": [],
}


class FakeRedis:
    """Dict-backed stand-in for redis.Redis (get / setex only)."""

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self.store[key] = value.encode("utf-8")  # redis-py returns bytes


class FakeCollection:
    """Just enough of a pymongo collection: update_one with upsert, find_one by equality."""

    def __init__(self):
        self.docs = []

    def _match(self, query):
        return next((d for d in self.docs if all(d.get(k) == v for k, v in query.items())), None)

    def find_one(self, query):
        doc = self._match(query)
        return dict(doc) if doc else None

    def update_one(self, query, update, upsert=False):
        doc = self._match(query)
        if doc is None:
            if not upsert:
                return
            # Like MongoDB: an upserted document starts from the query fields, then $set is applied.
            doc = dict(query)
            self.docs.append(doc)
        doc.update(update["$set"])


def make_message(video_id):
    """A transcription_completed message; the transcript is the same for every upload."""
    return json.dumps({
        "video_id": video_id,
        "text": "How was your week? I finally told my mother how I feel.",
        "utterances": [
            {"speaker": "A", "text": "How was your week?"},
            {"speaker": "B", "text": "I finally told my mother how I feel."},
        ],
    })


class ProcessAnalysisCacheTest(unittest.TestCase):

    def setUp(self):
        self.redis = FakeRedis()
        self.collection = FakeCollection()
        self.llm = mock.Mock(return_value=LLM_RESULT)
        for name, fake in [("redis_client", self.redis),
                           ("analysis_collection", self.collection),
                           ("analyze_with_llm", self.llm)]:
            patcher = mock.patch.object(main, name, fake)
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_analysis(self, video_id):
        channel = mock.Mock()
        main.process_analysis(channel, mock.Mock(), None, make_message(video_id))
        channel.basic_nack.assert_not_called()
        channel.basic_ack.assert_called_once()
        return channel

    def test_cache_miss_calls_llm_and_saves_result(self):
        self.run_analysis("first-upload")

        self.llm.assert_called_once()
        self.assertEqual(len(self.redis.store), 1)
        saved = self.collection.find_one({"video_id": "first-upload"})
        self.assertEqual(saved["ai_analysis"], LLM_RESULT)

    def test_reupload_of_same_session_is_retrievable_under_new_video_id(self):
        self.run_analysis("first-upload")
        channel = self.run_analysis("second-upload")

        # The second upload was served from the cache, not from the LLM.
        self.llm.assert_called_once()

        # Same query the reporting service runs for GET /analysis/{video_id}.
        saved = self.collection.find_one({"video_id": "second-upload"})
        self.assertIsNotNone(saved, "re-upload is not retrievable by its own video_id")
        self.assertEqual(saved["ai_analysis"], LLM_RESULT)

        # The first upload keeps its own document.
        self.assertIsNotNone(self.collection.find_one({"video_id": "first-upload"}))
        self.assertEqual(sorted(d["video_id"] for d in self.collection.docs),
                         ["first-upload", "second-upload"])

        published = json.loads(channel.basic_publish.call_args.kwargs["body"])
        self.assertEqual(published["video_id"], "second-upload")

    def test_redelivered_message_does_not_duplicate_document(self):
        self.run_analysis("first-upload")
        self.run_analysis("first-upload")

        self.assertEqual(len(self.collection.docs), 1)


if __name__ == "__main__":
    unittest.main()
