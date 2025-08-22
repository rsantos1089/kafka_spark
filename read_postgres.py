import psycopg2
from kafka import KafkaProducer
import json
import time
from kafka.errors import KafkaError

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

#postgres connection
db_properties = {
    'host' : 'localhost',
    'dbname' : 'postgres',
    'user' : 'postgres',
    'password' : "postgres",
    'port' : '5433'

}

#kafka configuration
kafka_topic = 'topic-1'
kafka_bootstrap_servers = 'localhost:9092'

#Create a kafka producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_bootstrap_servers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 2, 15)
)
INTERVAL = 2
# connect Postgres
try:
    connection = psycopg2.connect(**db_properties)
    cursor = connection.cursor()
    query = "select * from public.tickets"
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    for i in range(len(rows)):
        if i%100 == 0:
            time.sleep(INTERVAL)
        else :
            data = dict(zip(column_names,rows[i]))
            key = str(data['id']).encode(encoding="utf-8")
            print(data)
            producer.send(kafka_topic,key=key,value=data).add_callback(on_send_success).add_errback(on_send_error)

    # Ensure all messages are sent
    producer.flush()
except Exception as e:
    print(f"Error: {str(e)}")
    raise
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    producer.close()