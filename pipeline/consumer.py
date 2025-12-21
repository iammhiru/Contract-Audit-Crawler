import os
import json
from kafka import KafkaProducer

CRAWL_DIR = "/home/hieu/PycharmProjects/Project3/crawl"
ISSUE_DIR = "/home/hieu/PycharmProjects/Project3/issues_output"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    request_timeout_ms=10000,
    api_version_auto_timeout_ms=10000
)

# ---------- Send crawl folder ----------
def send_crawl_files():
    for file in os.listdir(CRAWL_DIR):
        if not file.endswith(".json"):
            continue
        path = os.path.join(CRAWL_DIR, file)

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)  # 1 JSON object

        producer.send("crawl-topic", data)
        print(f"✔ Sent crawl file → {file}")


# ---------- Send issues_output folder ----------
def send_issue_files():
    for file in os.listdir(ISSUE_DIR):
        if not file.endswith(".json"):
            continue
        path = os.path.join(ISSUE_DIR, file)

        with open(path, "r", encoding="utf-8") as f:
            arr = json.load(f)  # array of objects

        if isinstance(arr, list):
            for obj in arr:
                producer.send("issues-topic", obj)
            print(f"ent {len(arr)} issues from → {file}")
        else:
            print(f"File {file} is not an array")


def main():
    send_crawl_files()
    send_issue_files()
    producer.flush()
    print("Finished sending all messages to Kafka!")


if __name__ == "__main__":
    main()
