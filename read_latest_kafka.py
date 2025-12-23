#!/usr/bin/env python3
import argparse
from kafka import KafkaConsumer, TopicPartition

def main():
    p = argparse.ArgumentParser(description="Read last N messages from a Kafka topic and print their value.")
    p.add_argument("--bootstrap", required=True, help="Bootstrap server, e.g. 10.0.0.5:9092")
    p.add_argument("--topic", required=True, help="Topic name")
    p.add_argument("--n", type=int, required=True, help="How many latest messages to read")
    p.add_argument("--timeout-ms", type=int, default=5000, help="Stop if no new data within this time (ms)")
    p.add_argument("--group-id", default=None, help="Optional consumer group id (usually not needed for this script)")
    args = p.parse_args()

    consumer = KafkaConsumer(
        bootstrap_servers=[args.bootstrap],
        enable_auto_commit=False,
        auto_offset_reset="latest",  # we will seek manually anyway
        consumer_timeout_ms=args.timeout_ms,
        group_id=args.group_id,
        # If your messages are UTF-8 text, decode here; otherwise remove this and print raw bytes.
        value_deserializer=lambda b: b.decode("utf-8", errors="replace") if b is not None else None,
    )

    # Discover partitions and assign explicitly
    partitions = consumer.partitions_for_topic(args.topic)
    if not partitions:
        raise SystemExit(f"Topic not found or no partitions available: {args.topic}")

    tps = [TopicPartition(args.topic, part) for part in sorted(partitions)]
    consumer.assign(tps)

    # Seek to "end - n" per partition (approx last N overall; exact global last-N requires more bookkeeping)
    end_offsets = consumer.end_offsets(tps)
    for tp in tps:
        end = end_offsets[tp]
        start = max(0, end - args.n)
        consumer.seek(tp, start)

    printed = 0
    try:
        for msg in consumer:
            # msg.value is already deserialized to str if you kept value_deserializer
            print(msg.value)
            printed += 1
            if printed >= args.n:
                break
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
