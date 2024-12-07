import json 
from kafka import KafkaProducer

folderName = "./"
producer = KafkaProducer(
    bootstrap_servers="kafka-demo-kafka-starter.f.aivencloud.com:26540",
    security_protocol='SSL',
    ssl_cafile=f'{folderName}/ca.pem',
    ssl_certfile=f'{folderName}/service.cert',
    ssl_keyfile=f'{folderName}/service.key',
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii')

)

producer.send("test-topic",
              key={"key":1},
              value={"message": "hello world"}
              )

producer.flush()