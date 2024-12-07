from confluent_kafka import Producer

producer_config = {
    'bootstrap.servers': 'kafka-demo-kafka-starter.f.aivencloud.com:26540',
    'security.protocol': 'SSL',
    'ssl.ca.location': './ca.pem',
    'ssl.certificate.location': './service.cert',
    'ssl.key.location': './service.key',
}
producer = Producer(producer_config)

producer.produce("test-topic", key="key1", value="hello world")
producer.flush()
