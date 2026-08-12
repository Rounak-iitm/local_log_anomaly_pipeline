import csv
import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "server-logs"
CSV_FILE = "data/logs/server_logs.csv"

ENDPOINTS = [
    "/api/products",
    "/api/login",
    "/api/order",
    "/api/users",
    "/api/search",
    "/api/payment",
]

METHODS = {
    "/api/products": "GET",
    "/api/login": "GET",
    "/api/order": "POST",
    "/api/users": "GET",
    "/api/search": "GET",
    "/api/payment": "POST",
}


def generate_log():

    endpoint = random.choice(ENDPOINTS)
    method = METHODS[endpoint]

    ip = f"192.168.1.{random.randint(10, 30)}"

    status_code = random.choices(
        [200, 201, 400, 404, 500],
        weights=[75, 8, 7, 7, 3],
    )[0]

    response_time = random.randint(50, 500)

    if random.random() < 0.10:

        anomaly_type = random.choice(
            [
                "slow_response",
                "server_error",
                "suspicious_ip",
            ]
        )

        if anomaly_type == "slow_response":
            response_time = random.randint(1500, 5000)

        elif anomaly_type == "server_error":
            status_code = random.choice(
                [500, 502, 503]
            )

        elif anomaly_type == "suspicious_ip":
            ip = f"10.0.0.{random.randint(1, 255)}"

    return {
        "timestamp": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "ip": ip,
        "method": method,
        "endpoint": endpoint,
        "status_code": status_code,
        "response_time": response_time,
    }


def main():

    os.makedirs("data/logs", exist_ok=True)

    file_exists = os.path.exists(CSV_FILE)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value:
            json.dumps(value).encode("utf-8"),
    )

    csv_file = open(
        CSV_FILE,
        "a",
        newline="",
        encoding="utf-8",
    )

    writer = csv.DictWriter(
        csv_file,
        fieldnames=[
            "timestamp",
            "ip",
            "method",
            "endpoint",
            "status_code",
            "response_time",
        ],
    )

    if not file_exists:
        writer.writeheader()

    print("Log producer started.")
    print(f"Kafka topic: {TOPIC}")
    print(f"CSV output: {CSV_FILE}")
    print("Press Ctrl+C to stop.\n")

    try:

        while True:

            log = generate_log()

            producer.send(
                TOPIC,
                value=log,
            )

            producer.flush()

            writer.writerow(log)
            csv_file.flush()

            print(
                f"{log['timestamp']} | "
                f"{log['ip']} | "
                f"{log['method']} | "
                f"{log['endpoint']} | "
                f"{log['status_code']} | "
                f"{log['response_time']}ms"
            )

            time.sleep(0.5)

    except KeyboardInterrupt:

        print("\nProducer stopped.")

    finally:

        producer.close()
        csv_file.close()


if __name__ == "__main__":
    main()