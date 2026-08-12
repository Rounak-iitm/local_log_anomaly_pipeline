import json
import os

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction


BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "server-logs"

ANOMALY_OUTPUT = "data/anomalies/anomalies.jsonl"


class ParseLog(MapFunction):

    def map(self, value):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None


class DetectAnomaly(MapFunction):

    def map(self, log):
        if log is None:
            return None

        status_code = int(log["status_code"])
        response_time = int(log["response_time"])
        ip = log["ip"]

        reasons = []

        if response_time > 1000:
            reasons.append("high_response_time")

        if status_code >= 500:
            reasons.append("server_error")

        if ip.startswith("10."):
            reasons.append("suspicious_ip")

        if not reasons:
            return None

        log["anomaly"] = True
        log["reasons"] = reasons

        return log


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("log-anomaly-flink")
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    logs = env.from_source(
        source,
        watermark_strategy=None,
        source_name="Kafka Log Source",
    )

    parsed_logs = logs.map(
        ParseLog(),
        output_type=Types.STRING(),
    )

    anomalies = parsed_logs.map(
        DetectAnomaly(),
        output_type=Types.STRING(),
    )

    # Remove normal records.
    anomalies = anomalies.filter(
        lambda x: x is not None
    )

    anomalies.print()

    env.execute("Log Anomaly Detection")


if __name__ == "__main__":
    main()