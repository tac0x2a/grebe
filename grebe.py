#!/usr/bin/env python

import pika
import yaml
import os

from concurrent import futures

from grebe.grebe import Grebe
from grebe import logger
from grebe import parse_args as pa
from grebe.dbms_clickhouse import dbms_client
from grebe.schema_store_yaml import SchemaStoreYAML
from grebe.schema_store_clickhouse import SchemaStoreClickhouse
from grebe import api

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    args = pa.parse_args()
    MQ_QNAME = args.queue_name
    MQ_HOST = args.mh
    MQ_POST = args.mp
    DB_HOST = args.dh
    DB_PORT = args.dp
    RETRY_MAX = args.retry_max_count
    API_PORT = args.api_port

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
    logger.info(f"RabbitMQ connected({MQ_HOST}:{MQ_POST})")

    # initialize clickhouse
    client = dbms_client(DB_HOST, DB_PORT)
    logger.info(f"Clickhouse connected({DB_HOST}:{DB_PORT})")

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
        schema_store_client = dbms_client(DB_HOST, DB_PORT)
        logger.info(f"Clickhouse connected({DB_HOST}:{DB_PORT}) for schema store")
        store = SchemaStoreClickhouse(schema_store_client)
    else:
        schema_file = os.path.join(SCHEMA_DIR, f"schema_db_{DB_HOST}_{DB_PORT}.yml")
        store = SchemaStoreYAML(schema_file)

    grebe = Grebe(client, store, specified_types, MQ_QNAME, RETRY_MAX, TZ_STR, logger)

    def callback(channel, method, properties, body):
        grebe.callback(channel, method, properties, body)

    # start cousuming
    def run_webapi():
        if API_PORT:
            logger.info("Web API is enabled.")
            api._args = vars(args)
            api._grebe = grebe
            api.app.run(host='0.0.0.0', port=API_PORT)
        else:
            logger.info("Web API is disabled.")

    def start_consuming():
        logger.info("Consuming ...")
        channel.basic_consume(MQ_QNAME, callback)
        channel.start_consuming()

    with futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_list = [
            executor.submit(run_webapi),
            executor.submit(start_consuming)
        ]
        futures.as_completed(future_list)
