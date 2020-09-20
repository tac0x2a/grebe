from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException, ErrorCodes

from collections import OrderedDict
import re
import json
import logging
logger = logging.getLogger("dbms_clickhouse")


class TableNotFoundException(Exception):
    def __init__(self, message, table_name, nested=None):
        self.message = message
        self.table_name = table_name
        self.nested = nested


def dbms_client(db_host, db_port):
    return Client(db_host, db_port)


# ---------------------------------------------------------------------
def query_create_data_table(columns, types, data_table_name):
    column_types_map = {c: t for c, t in zip(columns, types)}

    # Column is already Nullable with out array.
    for c, t in column_types_map.items():
        if t.startswith("Array"):
            pattern = re.compile(r"Array\((.+)\)", re.IGNORECASE)
            inner_type_m = pattern.match(t)
            inner_type = inner_type_m.group(1)
            column_types_map[c] = f"Array(Nullable({inner_type}))"
        else:
            column_types_map[c] = "Nullable({})".format(t)

    columns_def_string = ", ".join([f"`{escape_symbol(c)}` {t}" for c, t in column_types_map.items()])
    return f"CREATE TABLE IF NOT EXISTS `{escape_symbol(data_table_name)}` ({columns_def_string}, __create_at DateTime64(3) DEFAULT now64(3), __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)"


def query_insert_data_table_without_value(column_names, data_table_name):
    columns_str = ", ".join([f"`{escape_symbol(c)}`" for c in column_names])
    return f"INSERT INTO `{escape_symbol(data_table_name)}` ({columns_str}) VALUES"


# ---------------------------------------------------------------------
def serialize_schema(columns: tuple, types: tuple, source_id: str) -> str:
    """
    serialize schema to string
    """
    d = OrderedDict([("source", source_id)])
    schema = OrderedDict(sorted([(k, v) for k, v in zip(columns, types)]))
    d["schema"] = schema
    return json.dumps(d, separators=(',', ':'))


def generate_new_table_name(source_id, schema_cache):
    related_tables = [t for t in schema_cache.values() if t.startswith(source_id + "_")]
    if len(related_tables) <= 0:
        return f"{source_id}_001"

    next_idx = 1
    table_name_candidate = f"{source_id}_{str(next_idx).zfill(3)}"

    while table_name_candidate in related_tables:
        next_idx = next_idx + 1
        table_name_candidate = f"{source_id}_{str(next_idx).zfill(3)}"

    return table_name_candidate


def create_data_table(client, columns, types, new_table_name):
    query = query_create_data_table(columns, types, new_table_name)
    client.execute(query)


def get_table_name_with_insert_if_new_schema(client, store, source_id, columns, types, serialized, schema_cache, max_sleep_sec=60):
    if serialized in schema_cache.keys():
        return schema_cache[serialized]

    # if multi grebe working, need to weak-consistency.
    # import random
    # from time import sleep
    # sleep_sec = random.randint(0, max_sleep_sec)
    # logger.info("Detected new schema '{}'. Random waiting {} sec ...".format(source_id, sleep_sec))
    # sleep(sleep_sec)

    # new format data received.
    new_schemas = store.load_all_schemas()
    schema_cache.update(new_schemas)
    if serialized in schema_cache.keys():
        logger.info("Schema is already created as '{}'. Keep going!".format(schema_cache[serialized]))
        return schema_cache[serialized]

    # it is true new schema !!
    new_table_name = generate_new_table_name(source_id, schema_cache)
    create_data_table(client, columns, types, new_table_name)
    store.store_schema(source_id, new_table_name, serialized)
    schema_cache[serialized] = new_table_name
    logger.info("Create new schema '{}' as '{}'".format(serialized, new_table_name))

    return new_table_name


def insert_data(client, data_table_name, columns, values_list):
    logger.debug(f"Try to insert into {data_table_name}")
    logger.debug(f"Columns {columns}")
    logger.debug(f"Values {values_list}")

    query = query_insert_data_table_without_value(columns, data_table_name)

    try:
        client.execute(query, values_list)
    except ServerException as e:
        if e.code == ErrorCodes.UNKNOWN_TABLE:
            raise TableNotFoundException("Table is not found", data_table_name, nested=e)
        else:
            raise e


def escape_symbol(symbol: str):
    return symbol.replace('`', '\\`')
