#!/usr/bin/env python

from clickhouse_driver import Client
import pika

import logging
import logging.handlers
import os
from datetime import datetime, timezone, timedelta

from br2dl import dbms_clickhouse as dbms
from br2dl.schema_store_yaml import SchemaStoreYAML
from br2dl.schema_store_clickhouse import SchemaStoreClickhouse

# Argument Parsing
import argparse
parser = argparse.ArgumentParser(description='Forward JSON message from RabbitMQ to Clickhouse')
parser.add_argument('queue_name', help='Queue name to subscribe on RabbitMQ')  # Required
parser.add_argument('-mh', help='RabbitMQ host', default='localhost')
parser.add_argument('-mp', help='RabbitMQ port', type=int, default=5672)
parser.add_argument('-dh', help='Clickhouse host', default='localhost')
parser.add_argument('-dp', help='Clickhouse port by native connection', type=int, default=9000)

parser.add_argument('--schema-store', help='Schema store location', choices=['local', 'rdb'], default="local")
parser.add_argument('--local-schema-dir', help='Schema DB directory path when schema-sotre is local', default="schemas")

parser.add_argument('--log-level', help='Log level', choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default='INFO')
parser.add_argument('--log-format', help='Log format by \'logging\' package', default='[%(levelname)s] %(asctime)s | %(pathname)s(L%(lineno)s) | %(message)s')  # Optional

parser.add_argument('--log-file', help='Log file path')
parser.add_argument('--log-file-count', help='Log file keep count', type=int, default=1000)
parser.add_argument('--log-file-size', help='Size of each log file', type=int, default=1000000)  # default 1MB

parser.add_argument('--retry-max-count', help='Max count of retry to processing. Message is discard when exceeded max count.', type=int, default=3)

args = parser.parse_args()

MQ_QNAME = args.queue_name
MQ_HOST = args.mh
MQ_POST = args.mp
DB_HOST = args.dh
DB_PORT = args.dp
RETRY_MAX = args.retry_max_count

SCHEMA_STORE = args.schema_store
SCHEMA_DIR = args.local_schema_dir


# Logger initialize

logging.basicConfig(level=args.log_level, format=args.log_format)

if args.log_file:
    dir = os.path.dirname(args.log_file)
    if not os.path.exists(dir):
        os.makedirs(dir)

    fh = logging.handlers.RotatingFileHandler(args.log_file, maxBytes=args.log_file_size, backupCount=args.log_file_count)
    fh.setFormatter(logging.Formatter(args.log_format))
    fh.setLevel(args.log_level)
    logging.getLogger().addHandler(fh)

logger = logging.getLogger("Grebe")
logger.info(args)


# Retry processing
def send_retry(channel, method, properties, body):
    RetryCountKey = "x-grebe-retry-count"

    current_retry_count = 0

    if properties.headers and properties.headers.get(RetryCountKey):
        current_retry_count = int(properties.headers.get(RetryCountKey))

    if current_retry_count >= RETRY_MAX:
        logger.error("Retry count exceeded!!({}). Discard Message = [exchange={}, routing_key={}, body={}]".format(RETRY_MAX, args.queue_name, method.routing_key, body))
        return

    props = pika.BasicProperties(
        headers={RetryCountKey: current_retry_count + 1}
    )
    logger.debug("Re-sending [exchange={}, routing_key={}, props={}, body={}]".format(args.queue_name, method.routing_key, props, body))
    channel.basic_publish(exchange=args.queue_name, routing_key=method.routing_key, properties=props, body=body)
    logger.warning("Re-send complete. ({})".format(current_retry_count + 1))


def callback(channel, method, properties, body):
    logger.debug("receive '{}({})'".format(method.routing_key, method.delivery_tag))
    try:
        # replace delimiter in topic mqtt/amqp.
        topic = method.routing_key
        source_id = topic.replace("/", "_").replace(".", "_")
        payload = str(body.decode('utf-8'))

        types, values = dbms.json2lcickhouse(payload)

        serialized = dbms.serialize_schema(types, source_id)
        data_table_name = dbms.get_table_name_with_insert_if_new_schema(client, store, source_id, types, serialized, schema_cache)
        dbms.insert_data(client, data_table_name, values)

        logger.debug(serialized)

    except Exception as e:
        logger.error(e, exc_info=e)
        logger.error("Consume failed '{}'. retrying...".format(method.routing_key))
        send_retry(channel, method, properties, body)

    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.debug("return basic_ack '{}({})'".format(method.routing_key, method.delivery_tag))


# initialize rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters(host=MQ_HOST, port=MQ_POST))
channel = connection.channel()

channel.queue_declare(queue=MQ_QNAME)
channel.basic_consume(MQ_QNAME, callback)
logger.info("RabbitMQ connected({}:{})".format(MQ_HOST, MQ_POST))

# initialize clickhouse
client = Client(DB_HOST, DB_PORT)
logger.info("Clickhouse connected({}:{})".format(DB_HOST, DB_PORT))

# Load Schema
if SCHEMA_STORE == 'rdb':
    store = SchemaStoreClickhouse(client)
else:
    schema_file = os.path.join(SCHEMA_DIR, f"schema_db_{DB_HOST}_{DB_PORT}.yml")
    store = SchemaStoreYAML(schema_file)

schema_cache = store.load_all_schemas()
logger.info(f"Load {len(schema_cache)} schemas from {SCHEMA_STORE}")
logger.info(f"Schemas: {[s for s in schema_cache.values()]}")

# start cousuming
logger.info("Consuming ...")
channel.start_consuming()
