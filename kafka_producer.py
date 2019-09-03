#!/usr/bin/python3
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.producer.future import FutureRecordMetadata
from EventLog import EventLog
import base64
import os
import datetime


producer = KafkaProducer(bootstrap_servers='kafka-7ab3a9e-justinraj1984-b417.aivencloud.com:14211',
                         security_protocol='SSL',
                         ssl_cafile='ca.pem',
                         ssl_certfile='access_cert.pem',
                         ssl_keyfile='key.pem')

try:
    # Write My example Kafka message to exercise_topic
    sent_msg = producer.send("exercise_topic", bytes(
        "My example Kafka message", 'utf8')).get()
    producer.flush()
    print(sent_msg)

    # Write a EventLog object to Kafka event_topic
    newEvent = EventLog.EventLog(EventLog(), clientProcess="PID-"+str(os.getpid()),
                                 inputLogMessage="Generated event message for process :"+str(os.getpid()), logDate=datetime.datetime.now())
    print(""+str(newEvent.clientProcess) + ";" +
          str(newEvent.inputLogMessage) + ";"+str(newEvent.logDate))
    message = str(newEvent.clientProcess)+";"+str(
        newEvent.inputLogMessage)+";"+str(newEvent.logDate)
    sent_msg = producer.send(
        "event_topic", value=bytes(message, "utf-8")).get()
    producer.flush()
    print(sent_msg)
except (Exception) as error:
    print("Error: " + str(error.__str__))
    print("Error: " + str(error))
