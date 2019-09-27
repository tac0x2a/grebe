#!/usr/bin/env python

import os
from datetime import datetime,timezone,timedelta

import pika
from clickhouse_driver import Client

from br2dl import dbms_clickhouse as dbms

# Logger
import logging
import logging.handlers

# Argument Parsing
import argparse
parser = argparse.ArgumentParser(description='Forward JSON message from RabbitMQ to Clickhouse')
parser.add_argument('queue_name', help='Queue name to subscribe on RabbitMQ') # Required
parser.add_argument('-mh', help='RabbitMQ host', default='localhost')
parser.add_argument('-mp', help='RabbitMQ port', type=int, default=5672)
parser.add_argument('-dh', help='Clickhouse host', default='localhost')
parser.add_argument('-dp', help='Clickhouse port by native connection', type=int, default= 9000)

parser.add_argument('--log-level', help='Log level', choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default='INFO')
parser.add_argument('--log-format', help='Log format by \'logging\' package', default='[%(levelname)s] %(asctime)s | %(pathname)s(L%(lineno)s) | %(message)s') # Optional

parser.add_argument('--log-file', help='Log file path')
parser.add_argument('--log-file-count', help='Log file keep count',  type=int, default=1000)
parser.add_argument('--log-file-size', help='Size of each log file',  type=int, default=1000000) #default 1MB

args = parser.parse_args()

MQ_QNAME = args.queue_name # os.environ.get('MQ_QNAME')
MQ_HOST  = args.mh # os.environ.get('MQ_HOST') or 'localhost'
MQ_POST  = args.mp # os.environ.get('MQ_POST') or 5672
DB_HOST = args.dh # os.environ.get('DB_HOST') or 'localhost'
DB_PORT = args.dp # os.environ.get('DB_PORT') or 9000

logging.basicConfig(level=args.log_level, format=args.log_format)

if args.log_file:
    fh = logging.handlers.RotatingFileHandler(args.log_file, maxBytes=args.log_file_size, backupCount=args.log_file_count)
    fh.setFormatter(logging.Formatter(args.log_format))
    fh.setLevel(args.log_level)
    logging.getLogger().addHandler(fh)

logger = logging.getLogger("Grebe")
logger.info(args)

def callback(channel, method, properties, body):
    logger.debug("receive '{}({})'".format(method.routing_key, method.delivery_tag))
    try:
        topic = method.routing_key.replace("/", "__")
        payload = str(body.decode('utf-8'))

        source_id = topic.replace('/', '_')
        types, values = dbms.json2lcickhouse(payload)

        serialized = dbms.serialize_schema(types, source_id)
        data_table_name = dbms.get_table_name_with_insert_if_new_schema(client, source_id, types, serialized, schema_cache)
        dbms.insert_data(client, data_table_name, values)

        logger.debug(serialized)

    except Exception as e:
        logger.error(e, exc_info=e)
        # Fixme: If maximum retry count exceeded, need to dump message to localfile, or some secure storage.
        logger.error("Forward failed. Re-sending [exchange={}, routing_key={}, body={}]".format(args.queue_name, topic, body))
        channel.basic_publish(exchange=args.queue_name, routing_key=topic, body=body)
        logger.error("Re-send complete.")

    finally:
        channel.basic_ack(delivery_tag = method.delivery_tag)
        logger.debug("return basic_ack '{}({})'".format(method.routing_key, method.delivery_tag))


# initialize rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters(host=MQ_HOST, port=MQ_POST))
channel = connection.channel()


channel.queue_declare(queue=MQ_QNAME)
channel.basic_consume(MQ_QNAME, callback)
logger.info("RabbitMQ connected({}:{})".format(MQ_HOST,MQ_POST))

# initialize clickhouse
client = Client(DB_HOST, DB_PORT)
logger.info("Clickhouse connected({}:{})".format(DB_HOST,DB_PORT))

dbms.create_schema_table(client) # init
schema_cache = dbms.select_all_schemas(client)
logger.info("Load {} schemas from DB".format(len(schema_cache)))
logger.debug("Schemas: {}".format([ s for s in schema_cache.values()]) )

logger.info("start_consuming...")
channel.start_consuming()
