import os
from datetime import datetime,timezone,timedelta

import pika
from clickhouse_driver import Client
from br2dl import dbms_clickhouse as dbms

MQ_HOST  = os.environ.get('MQ_HOST') or 'localhost'
MQ_port  = os.environ.get('MQ_PORT') or 5672
MQ_queue = os.environ.get('MQ_QUEUE')

db_host = os.environ.get('DB_HOST') or 'localhost'
db_port = os.environ.get('DB_PORT') or 9000

print(MQ_HOST)
print(MQ_port)

def callback(channel, method, properties, body):

    try:
        topic = method.routing_key.replace("/", "__")
        payload = str(body.decode('utf-8'))

        source_id = topic.replace('/', '_')
        types, values = dbms.json2lcickhouse(payload)

        serialized = dbms.serialize_schema(types, source_id)
        data_table_name = dbms.get_table_name_with_insert_if_new_schema(client, source_id, types, serialized, schema_cache)
        dbms.insert_data(client, data_table_name, values)

    except Error as e:
        print(e)

    finally:
        channel.basic_ack(delivery_tag = method.delivery_tag)


# initialize rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters(host=MQ_HOST, port=MQ_port))
channel = connection.channel()

channel.queue_declare(queue=MQ_queue)
channel.basic_consume(MQ_queue, callback)

# initialize clickhouse
client = Client(db_host, db_port)
dbms.create_schema_table(client) # init
schema_cache = dbms.select_all_schemas(client)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()