#!/usr/bin/python3
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='kafka-7ab3a9e-justinraj1984-b417.aivencloud.com:14211',
                         security_protocol='SSL',
                         ssl_cafile='ca.pem',
                         ssl_certfile='access_cert.pem',
                         ssl_keyfile='key.pem')

# Write My example Kafka message to exercise_topic
producer.send("exercise_topic", bytes("My example Kafka message", 'utf8'))
producer.flush()
