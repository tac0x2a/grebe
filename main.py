import datetime
from datetime import datetime,timezone,timedelta

from clickhouse_driver import Client

from br2dl import dbms_clickhouse


def serialize_schema(types, source_id):
    """
    serialize schema to string
    """
    serialized = str(sorted([[k,v] for k,v in types.items()]))
    return source_id + "_" + serialized

def generate_new_table_name(source_id, schema_cache):
    tables = [ t for t in schema_cache.values() if t.startswith(source_id) ]
    next_idx = 1
    if len(tables) > 0:
        least_table = sorted(tables)[-1]
        next_idx = int(least_table[-3:]) + 1

    next_number_idx = str(next_idx).zfill(3)
    return source_id + "_" + next_number_idx


def select_all_schemas(client):
    query = dbms_clickhouse.query_get_schema_table_all()
    tables = client.execute(query) # [(serialized_schema, table_name, source_id),]
    return { s:n for (s,n,i) in tables }

def create_schema_table(client):
    query = dbms_clickhouse.query_create_schema_table()
    client.execute(query)

def insert_schema(client, source_id, data_table_name, serialized_schema):
    query = dbms_clickhouse.query_insert_schema_table_without_value()
    client.execute(query, [{"source_id": source_id, "schema": serialized_schema, "table_name": data_table_name}])

def create_data_table(types, new_table_name):
    query = dbms_clickhouse.query_create_data_table(types, new_table_name)
    client.execute(query)


def get_table_name_with_insert_if_new_schema(client, source_id, types, serialized, schema_cache):
    if serialized in schema_cache.keys():
        return schema_cache[serialized]

    # new format data received.
    new_schemas = select_all_schemas(client)
    schema_cache.update(new_schemas)

    if serialized in schema_cache.keys():
        return schema_cache[serialized]

    # it is true new schema !!
    new_table_name = generate_new_table_name(source_id, schema_cache)
    insert_schema(client, source_id, new_table_name, serialized)
    create_data_table(types, new_table_name)
    print("created new table {}".format(new_table_name))

    return new_table_name

def insert_data(client, data_table_name, values):
    query = dbms_clickhouse.query_insert_data_table_without_value(values.keys(), data_table_name)
    client.execute(query, [values])

# ------------------------------------------------

# source_id = "sosu"

# types, values = dbms_clickhouse.json2lcickhouse('{"hoge":128, "w":{"orld":"world", "ow":[1,2,3]}}')
# serialized = serialize_schema(types, source_id)
# print(types)
# print(values)
# print(serialized)

# client = Client('192.168.11.200', 19000)
# create_schema_table(client) # init

# schema_cache = select_all_schemas(client) # {serialize_schema : table_name}

# data_table_name = get_table_name_with_insert_if_new_schema(client, source_id, types, serialized, schema_cache)
# insert_data(client, data_table_name, values)

# ------------------------------------------

import pika
import os
rabbitmq_host = os.environ.get('RABBITMQ_HOST') or "192.168.11.200" # for debug
rabbitmq_port = os.environ.get('RABBITMQ_PORT') or "5672"

print(rabbitmq_host)
print(rabbitmq_port)

def callback(channel, method, properties, body):
    topic = method.routing_key.replace("/", "__")
    payload = str(body.decode('utf-8'))

    # print(topic)
    # print(payload)

    source_id = topic.replace('/', '_')
    types, values = dbms_clickhouse.json2lcickhouse(payload)
    # print(source_id)

    # print(types)
    # print(values)

    serialized = serialize_schema(types, source_id)
    data_table_name = get_table_name_with_insert_if_new_schema(client, source_id, types, serialized, schema_cache)
    print(data_table_name)
    print(serialized)

    insert_data(client, data_table_name, values)

# initialize rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

channel.queue_declare(queue='devic')
channel.basic_consume('devic', callback)

# initialize clickhouse
client = Client('192.168.11.200', 19000)
create_schema_table(client) # init
schema_cache = select_all_schemas(client)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()