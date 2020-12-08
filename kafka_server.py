import producer_server


def run_kafka_server():
    input_file = "police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.sanfrancisco.police.calls_for_service.v1",
        bootstrap_servers="localhost:9092",
        client_id="sanfrancisco_import_police_calls_for_service"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
