#!/usr/bin/env python

import pika
import yaml
import os

from clickhouse_driver import Client
from lakeweed import clickhouse as d2c

from br2dl import logger
from br2dl import parse_args as pa
from br2dl import dbms_clickhouse as dbms
from br2dl.schema_store_yaml import SchemaStoreYAML
from br2dl.schema_store_clickhouse import SchemaStoreClickhouse


args = pa.parse_args()
MQ_QNAME = args.queue_name
MQ_HOST = args.mh
MQ_POST = args.mp
DB_HOST = args.dh
DB_PORT = args.dp
RETRY_MAX = args.retry_max_count

SCHEMA_STORE = args.schema_store
SCHEMA_DIR = args.local_schema_dir

TZ_STR = args.tz

# Logger initialize
logger = logger.init_logger(args.log_level, args.log_format, args.log_file_size, args.log_file_count, args.log_file, "Grebe")
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
    global schema_cache
    global store

    logger.debug("receive '{}({})'".format(method.routing_key, method.delivery_tag))
    try:
        # replace delimiter in topic mqtt/amqp.
        topic = method.routing_key
        source_id = topic.replace("/", "_").replace(".", "_")
        payload = str(body.decode('utf-8'))

        if source_id in specified_types.keys():
            spec_types = specified_types[source_id]
        else:
            spec_types = {}

        (columns, types, values_list) = d2c.data_string2type_value(payload, specified_types=spec_types, tz_str=TZ_STR)

        serialized = dbms.serialize_schema(columns, types, source_id)
        data_table_name = dbms.get_table_name_with_insert_if_new_schema(client, store, source_id, columns, types, serialized, schema_cache)
        dbms.insert_data(client, data_table_name, columns, values_list)

        logger.debug(serialized)

    except Exception as e:
        logger.error(e, exc_info=e)
        logger.error("Consume failed '{}'. retrying...".format(method.routing_key))

        import re
        m = re.search("DB::Exception: Table (.+) doesn't exist..", e.message)
        if m:
            logger.error(f"Table '{ m.group(1) }' is renamed? Update schema table cache.")
            schema_cache = store.load_all_schemas()

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

# Loading specified type file
specified_types = {}
if args.type_file:
    try:
        with open(args.type_file, 'r') as tf:
            specified_types = yaml.load(tf)
            logger.info(f"Load specified types. {specified_types}")
    except (FileNotFoundError, yaml.parser.ParserError, yaml.scanner.ScannerError) as e:
        logger.warning(f"type-file load failed. ('{args.type_file}') ignored specified types. ({e})")

# start cousuming
logger.info("Consuming ...")
channel.start_consuming()
