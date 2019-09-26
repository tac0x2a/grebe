import os
from datetime import datetime,timezone,timedelta

import pika
from clickhouse_driver import Client

from br2dl import dbms_clickhouse as dbms

# Argument Parsing
import argparse
parser = argparse.ArgumentParser(description='Forward JSON message from RabbitMQ to Clickhouse')
parser.add_argument('queue_name', help='Queue name to subscribe on RabbitMQ') # Required
parser.add_argument('-mh', help='RabbitMQ host', default='localhost') # Optional
parser.add_argument('-mp', help='RabbitMQ port', default=5672) # Optional
parser.add_argument('-dh', help='Clickhouse host', default='localhost') # Optional
parser.add_argument('-dp', help='Clickhouse port by native connection', default= 9000) # Optional
args = parser.parse_args()

MQ_QNAME = args.queue_name # os.environ.get('MQ_QNAME')
MQ_HOST  = args.mh # os.environ.get('MQ_HOST') or 'localhost'
MQ_POST  = args.mp # os.environ.get('MQ_POST') or 5672
DB_HOST = args.dh # os.environ.get('DB_HOST') or 'localhost'
DB_PORT = args.dp # os.environ.get('DB_PORT') or 9000

print(args)

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
connection = pika.BlockingConnection(pika.ConnectionParameters(host=MQ_HOST, port=MQ_POST))
channel = connection.channel()

channel.queue_declare(queue=MQ_QNAME)
channel.basic_consume(MQ_QNAME, callback)

# initialize clickhouse
client = Client(DB_HOST, DB_PORT)
dbms.create_schema_table(client) # init
schema_cache = dbms.select_all_schemas(client)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()