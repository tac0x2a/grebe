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

def get_schemas(client):
    query = dbms_clickhouse.query_get_schema_table_all()
    tables = client.execute(query) # [(serialized_schema, table_name, source_id),]
    return { s:n for (s,n,i) in tables }

def create_schema_table(client):
    query = dbms_clickhouse.query_create_schema_table()
    client.execute(query)

def insert_schema(client, source_id, data_table_name, serialized_schema):
    query = dbms_clickhouse.query_insert_schema_table_without_value()
    client.execute(query, [{"source_id": source_id, "schema": serialized_schema, "table_name": data_table_name}])


def get_new_table_name(source_id, schema_cache):
    tables = [ t for t in schema_cache.values() if t.startswith(source_id) ]
    least_table = sorted(tables)[-1]
    next_number = int(least_table[-3:]) + 1
    next_number_idx = str(next_number).zfill(3)
    return source_id + "_" + next_number_idx

def get_table_name_with_insert_if_new_schema(client, source_id, serialized, schema_cache):

    if serialized in schema_cache.keys():
        return schema_cache[serialized]

    # new format data received.
    new_schemas = get_schemas(client)
    schema_cache.update(new_schemas)

    if serialized in schema_cache.keys():
        return schema_cache[serialized]

    # it is true new schema !!
    new_table_name = get_new_table_name(source_id, schema_cache)
    insert_schema(client, source_id, new_table_name, serialized)

    return new_table_name

# ------------------------------------------------

source_id = "sosu"

types, values = dbms_clickhouse.json2lcickhouse('{"helln":128, "w":{"orld":"world", "ow":[1,2,3]}}')
serialized = serialize_schema(types, source_id)
print(types)
print(values)
print(serialized)

client = Client('192.168.11.200', 19000)
create_schema_table(client) # init

schema_cache = get_schemas(client) # {serialize_schema : table_name}

new_table_name = get_table_name_with_insert_if_new_schema(client, source_id, serialized, schema_cache)
print(new_table_name)

# insert_schema(client, source_id, source_id + "_" + "042", serialized)
print(get_schemas(client))

# client = Client('192.168.11.200', 19000)
# print(client.execute('SHOW TABLES'))
# client.execute(
#     dbms_clickhouse.query_create_data_table(types, 'sample_001')
# )

# JST = timezone(timedelta(hours=+9), 'JST')
# client.execute(
#      'INSERT INTO {} (__uid, __collected_at, sample_f, sample_b) VALUES'.format("sample_data_table_001"),
#      [{'__uid': 100, "__collected_at": datetime.now(JST), "sample_f":42.195, "sample_b": False}]
#  )

# import pika
# import dbms_clickhouse

# import os
# rabbitmq_host = os.environ.get('RABBITMQ_HOST') or "192.168.11.200" # for debug
# rabbitmq_port = os.environ.get('RABBITMQ_PORT') or "5672"

# print(rabbitmq_host)
# print(rabbitmq_port)

# def callback(channel, method, properties, body):
#     topic = method.routing_key.replace("/", "__")
#     payload = str(body.decode('utf-8'))

#     print(topic)
#     print(payload)

# connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
# channel = connection.channel()

# channel.queue_declare(queue='devic')
# channel.basic_consume('devic', callback)

# print(' [*] Waiting for messages. To exit press CTRL+C')
# channel.start_consuming()