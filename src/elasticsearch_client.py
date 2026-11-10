
import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError


load_dotenv()

ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    "http://localhost:9200"
)

ELASTICSEARCH_INDEX = os.getenv(
    "ELASTICSEARCH_INDEX",
    "traces-generic.otel-default"
)


ELASTICSEARCH_INDEX_BEFORE = os.getenv(
    "ELASTICSEARCH_INDEX_BEFORE",
    "traces-before.otel-default"
)

ELASTICSEARCH_INDEX_AFTER = os.getenv(
    "ELASTICSEARCH_INDEX_AFTER",
    "traces-after.otel-default"
)



def create_client():
    """Create a connection to Elasticsearch."""
    client = Elasticsearch(ELASTICSEARCH_URL)

    return client


def test_connection():
    """Check whether Elasticsearch is reachable."""
    client = create_client()

    try:
        if client.ping():
            print("Successfully connected to Elasticsearch!")
            print(f"Elasticsearch URL: {ELASTICSEARCH_URL}")
            print(f"Trace index: {ELASTICSEARCH_INDEX}")
            return client

        print("Could not establish a connection to Elasticsearch.")
        return None

    except ConnectionError as error:
        print(f"Elasticsearch connection failed: {error}")
        return None


