
import logging
logger = logging.getLogger("dbms_clickhouse")

# ---------------------------------------------------------------------


def query_create_data_table(column_types_map, data_table_name):
    columns_def_string = ", ".join(["\"{}\" {}".format(c, t) for c, t in column_types_map.items()])
    return "CREATE TABLE IF NOT EXISTS {} ({}, __create_at DateTime DEFAULT now(), __collected_at DateTime, __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)".format(data_table_name, columns_def_string)


def query_insert_data_table_without_value(column_names, data_table_name):
    columns_str = ", ".join(['"' + c + '"' for c in column_names])
    return "INSERT INTO {} ({}) VALUES".format(data_table_name, columns_str)


# ---------------------------------------------------------------------
def serialize_schema(types, source_id):
    """
    serialize schema to string
    """
    serialized = str(sorted([[k, v] for k, v in types.items()]))
    return source_id + "_" + serialized


def generate_new_table_name(source_id, schema_cache):
    tables = [t for t in schema_cache.values() if t.startswith(source_id)]
    next_idx = 1
    if len(tables) > 0:
        least_table = sorted(tables)[-1]
        next_idx = int(least_table[-3:]) + 1

    next_number_idx = str(next_idx).zfill(3)
    return source_id + "_" + next_number_idx


def create_data_table(client, types, new_table_name):
    query = query_create_data_table(types, new_table_name)
    client.execute(query)


def get_table_name_with_insert_if_new_schema(client, store, source_id, types, serialized, schema_cache, max_sleep_sec=60):
    if serialized in schema_cache.keys():
        return schema_cache[serialized]

    # if multi grebe working, need to weak-consistency.
    import random
    from time import sleep
    sleep_sec = random.randint(0, max_sleep_sec)
    logger.info("Detected new schema '{}'. Random waiting {} sec ...".format(source_id, sleep_sec))
    sleep(sleep_sec)

    # new format data received.
    new_schemas = store.load_all_schemas()
    schema_cache.update(new_schemas)

    if serialized in schema_cache.keys():
        logger.info("Schema is already created as '{}'. Keep going!".format(schema_cache[serialized]))
        return schema_cache[serialized]

    # it is true new schema !!
    new_table_name = generate_new_table_name(source_id, schema_cache)
    create_data_table(client, types, new_table_name)
    store.store_schema(source_id, new_table_name, serialized)
    schema_cache[serialized] = new_table_name
    logger.info("Create new schema '{}' as '{}'".format(serialized, new_table_name))

    return new_table_name


def insert_data(client, data_table_name, values):
    query = query_insert_data_table_without_value(values.keys(), data_table_name)
    client.execute(query, [values])
