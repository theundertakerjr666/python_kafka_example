#!/usr/bin/python3
from kafka import KafkaConsumer
from kafka import TopicPartition

from configparser import ConfigParser
from EventLog import EventLog
import psycopg2
import datetime

selection = True
while(selection):
    inputSel = input(
        """Please\n
        select 1 if you wish to read/write events from the sample topic\n
        select 2 if you wish to read/write events from the eventlog topic\nSelection : """)
    if inputSel == "1" or "2":
        selection = False


EXERCISE_TOPIC = 'exercise_topic'
EVENT_TOPIC = 'event_topic'
consumer = KafkaConsumer(bootstrap_servers='kafka-7ab3a9e-justinraj1984-b417.aivencloud.com:14211',
                         security_protocol='SSL',
                         ssl_cafile='ca.pem',
                         ssl_certfile='access_cert.pem',
                         ssl_keyfile='key.pem',
                         consumer_timeout_ms=10000)


if inputSel == "1":
    # Open connection to PostgreSQL
    parser = ConfigParser()
    parser.read('database.ini')
    db_connection_values = {}
    params = parser.items('postgresql_exercise')
    for param in params:
        db_connection_values[param[0]] = param[1]
    # Connect to PostgreSQL
    print('Connecting to the PostgreSQL database')
    conn = None
    conn = psycopg2.connect(**db_connection_values)
    # Read all messages from exercise_topic
    consumer.assign([TopicPartition(EXERCISE_TOPIC, 0)])
    consumer.seek_to_beginning(TopicPartition(EXERCISE_TOPIC, 0))
    consumer.seek_to_beginning

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
            print("%s:%d:%d: key=%s value=%s" % (msg.topic,
                                                 msg.partition, msg.offset, msg.key, msg.value))
            msg_value = msg.value()
            msg_value = msg.value.decode("utf-8")
            print("Message retrieved from topic :") + str(msg_value)
            sql = "INSERT INTO exercise_table(my_values) VALUES('" + \
                msg_value+"');"
            print(sql)
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

if inputSel == "2":
    # Open connection to PostgreSQL
    parser = ConfigParser()
    parser.read('database.ini')
    db_connection_values = {}
    params = parser.items('postgresql_eventlog')
    for param in params:
        db_connection_values[param[0]] = param[1]
    # Connect to PostgreSQL
    print('Connecting to the PostgreSQL database')
    conn = None
    conn = psycopg2.connect(**db_connection_values)
    # Read all messages from event_topic
    consumer.assign([TopicPartition(EVENT_TOPIC, 0)])
    consumer.seek_to_beginning(TopicPartition(EVENT_TOPIC, 0))
    consumer.seek_to_beginning

    # create a cursor
    try:
        cur = conn.cursor()
        # cur.execute('SELECT version()')
        sql_str = """
                CREATE TABLE message_table (
                    client_process VARCHAR(255),
                    log_message VARCHAR(255),
                    log_date VARCHAR(255)
                )
                """
        # Code commented after table is created
        # cur.execute(sql_str)

        for msg in consumer:
            print(msg)
            print("%s:%d:%d: key=%s value=%s" % (msg.topic,
                                                 msg.partition, msg.offset, msg.key, msg.value))
            # msg_value = EventLog()
            msg_value = msg.value.decode("utf8")
            x = msg_value.split(";")

            print("Message retrieved from topic :")
            print(x)
            if len(x) == 3:
                sql = "INSERT INTO message_table(client_process, log_message, log_date) VALUES('" + \
                    x[0]+"','"+x[1] + "','"+x[2]+"');"
                cur.execute(sql)
                conn.commit()
            elif len(x) == 1:
                sql = "INSERT INTO message_table(log_message, log_date) VALUES('" + \
                    x[0]+"','"+str(datetime.datetime.now()) + "');"
                cur.execute(sql)
                conn.commit()
            else:
                sql = 'SELECT version()'
                cur.execute(sql)

        print('Result start:')
        cur.execute(
            "SELECT client_process, log_message, log_date FROM message_table")
        db_result = cur.fetchone()
        print('Result :' + str(db_result))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: "+str(error))
    finally:
        if conn is not None:
            conn.close()
