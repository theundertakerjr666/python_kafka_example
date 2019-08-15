#!/usr/bin/python3
from kafka import KafkaConsumer
from kafka import TopicPartition
from configparser import ConfigParser
import psycopg2


TOPIC = 'exercise_topic'
consumer = KafkaConsumer(bootstrap_servers='kafka-7ab3a9e-justinraj1984-b417.aivencloud.com:14211',
                         security_protocol='SSL',
                         ssl_cafile='ca.pem',
                         ssl_certfile='access_cert.pem',
                         ssl_keyfile='key.pem')

# Read and print all messages from exercise_topic
consumer.assign([TopicPartition(TOPIC, 0)])
consumer.seek_to_beginning(TopicPartition(TOPIC, 0))


# Next, store the messages in PostgreSQL
parser = ConfigParser()
parser.read('database.ini')
db_connection_values = {}
params = parser.items('postgresql')
for param in params:
    db_connection_values[param[0]] = param[1]
# Connect to PostgreSQL
print('Connecting to the PostgreSQL database')
conn = None
conn = psycopg2.connect(**db_connection_values)

# create a cursor
try:
    cur = conn.cursor()
    # cur.execute('SELECT version()')
    sql_str = """
            CREATE TABLE exercise_table (
                my_values VARCHAR(255)
            )
            """
    # Code commented after table is created
    # cur.execute(sql_str)

    for msg in consumer:
        print(msg)
        msg_value = msg.value.decode("utf-8")
        # sql = """'INSERT INTO exercise_table(my_values) VALUES(%s);"""
        sql = "INSERT INTO exercise_table(my_values) VALUES('" + \
            msg_value+"');"
        cur.execute(sql)
        conn.commit()
        cur.execute("SELECT my_values FROM exercise_table")
        db_result = cur.fetchone()
        print('Result :' + str(db_result))
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print("Error: "+str(error))
finally:
    if conn is not None:
        conn.close()
