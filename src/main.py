from elasticsearch_client import test_connection
from trace_explorer import run_trace_explorer


def main():
    client = test_connection()

    if client is None:
        return

    try:
        run_trace_explorer(client)
    finally:
        client.close()


if __name__ == "__main__":
    main()
