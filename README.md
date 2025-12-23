# kafka_py_utils
kafka utilities in python


## basic read
`python read_latest_kafka.py --bootstrap 11.22.33.44:9092 --topic DWH --n 10`

## With a longer wait before it stops
`python read_latest_kafka.py --bootstrap 11.22.33.44:9092 --topic DWH --n 50 --timeout-ms 20000`

## With a consumer group id
`python read_latest_kafka.py --bootstrap 11.22.33.44:9092 --topic DWH --n 10 --group-id debug-reader-1`

## move kafka messages (no headers) from source to destination topic
`python relay_kafka_tail_grok.py --src-bootstrap "11.22.33.44:9092" --src-topic DWH --dst-bootstrap "66.77.88.154:9094,66.77.88.155:9094" --dst-topic DWH --n 200`
