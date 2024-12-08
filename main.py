from faker import Faker
import json
import time
import sys
import random
import argparse
from confluent_kafka import Producer
from pizzaproducer import PizzaProvider
from realstockproducer import RealStockProvider
from userbehaviourproducer import UserBehaviorProvider



MAX_NUMBER_PIZZAS_IN_ORDER = 10
MAX_ADDITIONAL_TOPPINGS_IN_PIZZA = 5

fake = Faker()
Faker.seed(4321)



def delivery_callback(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic: {msg.topic()} [{msg.partition()}]')


def produce_msgs(
    security_protocol="SSL",
    sasl_mechanism="SCRAM-SHA-256",
    cert_folder="~/kafka-pizza/",
    username="",
    password="",
    hostname="hostname",
    port="1234",
    topic_name="pizza-orders",
    nr_messages=-1,
    max_waiting_time_in_sec=5,
    subject="pizza",
):
    
    # Create a Kafka producer
    conf = {
        "bootstrap.servers": f"{hostname}:{port}",
        "security.protocol": security_protocol,
    }

    if security_protocol == "SSL":
        conf.update({
            "ssl.ca.location": cert_folder + "/ca.pem",
            "ssl.certificate.location": cert_folder + "/service.cert",
            "ssl.key.location": cert_folder + "/service.key",
        })

    elif security_protocol == "SASL_SSL":
        conf.update({
            "sasl.mechanisms": sasl_mechanism,
            "sasl.username": username,
            "sasl.password": password,
            "ssl.ca.location": cert_folder + "/ca.pem" if cert_folder else None,
        })

    producer = Producer(conf)

    if nr_messages <= 0:
        nr_messages = float("inf")
    i = 0

    if subject == "stock":
        fake.add_provider(StockProvider)
    elif subject == "realstock":
        fake.add_provider(RealStockProvider)
    elif subject == "metric":
        fake.add_provider(MetricProvider)
    elif subject == "advancedmetric":
        fake.add_provider(MetricAdvancedProvider)
    elif subject == "userbehaviour":
        fake.add_provider(UserBehaviorProvider)
    elif subject == "bet":
        fake.add_provider(UserBetsProvider)
    elif subject == "rolling":
        fake.add_provider(RollingProvider)
    else:
        fake.add_provider(PizzaProvider)

    while i < nr_messages:
        if subject in [
            "stock",
            "userbehaviour",
            "realstock",
            "metric",
            "bet",
            "rolling",
            "advancedmetric",
        ]:
            message, key = fake.produce_msg()
        else:
            message, key = fake.produce_msg(
                fake,
                i,
                MAX_NUMBER_PIZZAS_IN_ORDER,
                MAX_ADDITIONAL_TOPPINGS_IN_PIZZA,
            )

        print(f"Sending: {message}")
        # Sending the message to Kafka
        producer.produce(
            topic_name,
            key=json.dumps(key).encode("utf-8"),
            value=json.dumps(message).encode("utf-8"),
            callback=delivery_callback,
        )

        # Allowing time between messages
        sleep_time = random.uniform(0, max_waiting_time_in_sec)
        print(f"Sleeping for... {sleep_time}s")
        time.sleep(sleep_time)

        # Force flushing every 100 messages
        if (i % 100) == 0:
            producer.flush()
        i += 1

    producer.flush()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--security-protocol", required=True, help="Security protocol (PLAINTEXT, SSL, SASL_SSL)")
    parser.add_argument("--sasl-mechanism", required=False, help="SASL mechanism (PLAIN, SCRAM-SHA-256, etc.)")
    parser.add_argument("--cert-folder", required=False, help="Path to SSL certificate folder")
    parser.add_argument("--username", required=False, help="Username for SASL_SSL")
    parser.add_argument("--password", required=False, help="Password for SASL_SSL")
    parser.add_argument("--host", required=True, help="Kafka Host (e.g., from Aiven Console)")
    parser.add_argument("--port", required=True, help="Kafka Port")
    parser.add_argument("--topic-name", required=True, help="Kafka Topic Name")
    parser.add_argument("--nr-messages", required=True, help="Number of messages to produce (0 for unlimited)")
    parser.add_argument("--max-waiting-time", required=True, help="Max waiting time between messages (seconds)")
    parser.add_argument("--subject", required=False, help="Type of content to produce (e.g., pizza, stock, etc.)")
    args = parser.parse_args()

    produce_msgs(
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        cert_folder=args.cert_folder,
        username=args.username,
        password=args.password,
        hostname=args.host,
        port=args.port,
        topic_name=args.topic_name,
        nr_messages=int(args.nr_messages),
        max_waiting_time_in_sec=float(args.max_waiting_time),
        subject=args.subject,
    )
    print("Message production completed.")

if __name__ == "__main__":
    main()













# producer_config = {
#     'bootstrap.servers': 'kafka-demo-kafka-starter.f.aivencloud.com:26540',
#     'security.protocol': 'SSL',
#     'ssl.ca.location': './ca.pem',
#     'ssl.certificate.location': './service.cert',
#     'ssl.key.location': './service.key',
# }
# producer = Producer(producer_config)

# producer.produce("test-topic", key="key1", value="hello world")
# producer.flush()
