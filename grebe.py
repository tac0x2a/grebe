#!/usr/bin/env python

import pika
import yaml
import os


from lakeweed import clickhouse as d2c

from br2dl import logger
from br2dl import parse_args as pa
from br2dl import dbms_clickhouse as dbms
from br2dl.dbms_clickhouse import dbms_client, TableNotFoundException
from br2dl.schema_store_yaml import SchemaStoreYAML
from br2dl.schema_store_clickhouse import SchemaStoreClickhouse


class Grebe:
    def __init__(self, client, schema_store, specified_types, resend_exchange_name, retry_max: int, tz_str: str, logger):
        self.client = client
        self.schema_store = schema_store
        self.specified_types = specified_types
        self.resend_exchange_name = resend_exchange_name
        self.retry_max = retry_max
        self.tz_str = tz_str
        self.logger = logger

        self.schema_cache = self.schema_store.load_all_schemas()
        self.logger.info(f"Load {len(self.schema_cache)} schemas from {self.schema_store}")
        self.logger.info(f"Schemas: {[s for s in self.schema_cache.values()]}")

    def callback(self, channel, method, properties, body):
        self.logger.debug("receive '{}({})'".format(method.routing_key, method.delivery_tag))

        try:
            Grebe.insert_data(
                method, body,
                self.client,
                self.schema_store,
                self.schema_cache,
                self.specified_types,
                self.tz_str,
                self.logger
            )
        except TableNotFoundException as e:
            self.logger.error(f"Table '{ e.table_name }' is renamed? Update schema table cache.")
            self.schema_cache = self.schema_store.load_all_schemas()

            self.logger.error("Consume failed '{}'. retrying...".format(method.routing_key))
            Grebe.send_retry(
                channel, method, properties, body,
                self.retry_max,
                self.resend_exchange_name,
                self.logger
            )

        except Exception as e:
            self.logger.error(e, exc_info=e)
            self.logger.error("Consume failed '{}'. retrying...".format(method.routing_key))
            Grebe.send_retry(
                channel, method, properties, body,
                self.retry_max,
                self.resend_exchange_name,
                self.logger
            )

        finally:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.debug("return basic_ack '{}({})'".format(method.routing_key, method.delivery_tag))

    @classmethod
    def insert_data(cls, method, body, client, schema_store, schema_cache, specified_types, tz_str, logger):
        # replace delimiter in topic mqtt/amqp.
        topic = method.routing_key
        source_id = topic.replace("/", "_").replace(".", "_")
        payload = str(body.decode('utf-8'))

        if source_id in specified_types.keys():
            spec_types = specified_types[source_id]
        else:
            spec_types = {}

        (columns, types, values_list) = d2c.data_string2type_value(payload, specified_types=spec_types, tz_str=tz_str)

        serialized = dbms.serialize_schema(columns, types, source_id)
        data_table_name = dbms.get_table_name_with_insert_if_new_schema(client, schema_store, source_id, columns, types, serialized, schema_cache)
        dbms.insert_data(client, data_table_name, columns, values_list)

        logger.debug(serialized)

    @classmethod
    def send_retry(cls, channel, method, properties, body, retry_max, resend_exchange_name, logger):
        RetryCountKey = "x-grebe-retry-count"

        current_retry_count = 0

        if properties.headers and properties.headers.get(RetryCountKey):
            current_retry_count = int(properties.headers.get(RetryCountKey))

        if current_retry_count >= retry_max:
            logger.error("Retry count exceeded!!({}). Discard Message = [exchange={}, routing_key={}, body={}]".format(retry_max, resend_exchange_name, method.routing_key, body))
            return

        props = pika.BasicProperties(
            headers={RetryCountKey: current_retry_count + 1}
        )
        logger.debug("Re-sending [exchange={}, routing_key={}, props={}, body={}]".format(resend_exchange_name, method.routing_key, props, body))
        channel.basic_publish(exchange=resend_exchange_name, routing_key=method.routing_key, properties=props, body=body)
        logger.warning("Re-send complete. ({})".format(current_retry_count + 1))


# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
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

    # initialize rabbitmq
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=MQ_HOST, port=MQ_POST))
    channel = connection.channel()

    channel.queue_declare(queue=MQ_QNAME)
    logger.info("RabbitMQ connected({}:{})".format(MQ_HOST, MQ_POST))

    # initialize clickhouse
    client = dbms_client(DB_HOST, DB_PORT)
    logger.info("Clickhouse connected({}:{})".format(DB_HOST, DB_PORT))

    # Loading specified type file
    specified_types = {}
    if args.type_file:
        try:
            with open(args.type_file, 'r') as tf:
                specified_types = yaml.load(tf)
                logger.info(f"Load specified types. {specified_types}")
        except (FileNotFoundError, yaml.parser.ParserError, yaml.scanner.ScannerError) as e:
            logger.warning(f"type-file load failed. ('{args.type_file}') ignored specified types. ({e})")

    # Load Schema
    if SCHEMA_STORE == 'rdb':
        store = SchemaStoreClickhouse(client)
    else:
        schema_file = os.path.join(SCHEMA_DIR, f"schema_db_{DB_HOST}_{DB_PORT}.yml")
        store = SchemaStoreYAML(schema_file)

    grebe = Grebe(client, store, specified_types, MQ_QNAME, RETRY_MAX, TZ_STR, logger)

    def callback(channel, method, properties, body):
        grebe.callback(channel, method, properties, body)

    # start cousuming
    logger.info("Consuming ...")
    channel.basic_consume(MQ_QNAME, callback)
    channel.start_consuming()
