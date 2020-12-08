from kafka import KafkaConsumer
import json


def deserialize_message(message):
    return json.loads(message.decode('utf-8'))


def consumer_server():
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092', group_id='test_kafka_server_sf',
                             auto_offset_reset='earliest')
    consumer.subscribe(['com.sanfrancisco.police.calls_for_service.v1'])

    for message in consumer:
        print(deserialize_message(message.value))


if __name__ == '__main__':
    consumer_server()
