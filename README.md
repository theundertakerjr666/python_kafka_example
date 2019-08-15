# Kafka Python client to public cloud

- send events to a Kafka topic (a producer) which will then be read by a Kafka consumer
- The consumer application must then store the consumed data to a PostgreSQL database.

To Run

---

- Install Python3 and pip manager followed by the kafka plugin
- > > > pip install kafka-python

- Install PostgreSQL plugin
- > > > pip install psycopg2

* Execute kafka_producer.py to create messages
* Execute kafka_consumer.py to read messages and insert them into Postgresql
* The examples require certificates to connect. Please request for them  from me.

Kafka topic :
exercise_topic

Postgresql db:
- exercise_db
Postgresql table:
- exercise_table
