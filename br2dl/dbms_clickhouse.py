
from . import time_parser

import json
import datetime
from dateutil.tz import tzutc

def json2clickhouse_sub_list(key, list, types, values):
    items = []
    items_ns = []
    types[key] = "Array(String)"
    for idx_i, v in enumerate(list):
        if isinstance(v, type(list)) or (type(v) is dict):
            items.append(json.dumps(v))
            continue

        idx = str(idx_i)
        temp_type = {}
        temp_value = {}

        json2lcickhouse_sub(idx, v, temp_type, temp_value)
        items.append(temp_value[idx])

        types[key] = "Array({})".format(temp_type[idx])

        # for DateTime Array
        if (idx + "_ns") in temp_type.keys():
            items_ns.append(temp_value[idx + "_ns"])
            types[key + "_ns"] = "Array({})".format(temp_type[idx + "_ns"])

    values[key] = items

    # for DateTime Array
    if len(items_ns) > 0:
        values[key + "_ns"] = items_ns

    return


def json2lcickhouse_sub(key, body, types, values):
    if type(body) is dict:
        for child_key, child_value in body.items():
            json2lcickhouse_sub(key + "__" + child_key, child_value, types, values)
        return

    if type(body) is list:
        json2clickhouse_sub_list(key, body, types, values)
        return

    # is atomic type.
    value = body

    if type(value) is float:
        values[key] = float(value)
        types[key] = "Float64"
        return
    if type(value) is int:
        values[key] = int(value)
        types[key] = "Float64"
        return
    if type(value) is bool:
        values[key] = 1 if value else 0
        types[key] = "UInt8"
        return

    # is string. try to parse as datetime.
    try:
        [dt, ns] = time_parser.elastic_time_parse(value)
        values[key] = dt
        types[key] = "DateTime"
        # Clickhouse can NOT contain ms in DateTime column.
        values[key + "_ns"] = ns
        types[key + "_ns"] = "UInt32"
        return
    except ValueError as e:
        pass

    values[key] = str(value)
    types[key] = "String"

    return

def json2lcickhouse(src_json_str, logger = None):
    """Convert json string to python dict with data types for Clickhouse"""
    body = json.loads(src_json_str)

    types = {}
    values = {}

    for key, value in body.items():
        json2lcickhouse_sub(key, value, types, values)

    # convert as string for clickhouse query.
    values_as_string = {}
    # for k,v in values.items():
    #     if type(v) is str:
    #         v = "'" + v + "'"
    #     values_as_string[k] = str(v)
    # return [types, values_as_string]
    return [types, values]

# ---------------------------------------------------------------------
def query_create_schema_table(schema_table_name = "schema_table"):
    return "CREATE TABLE IF NOT EXISTS {} (__create_at DateTime DEFAULT now(), source_id String, schema String, table_name String) ENGINE = MergeTree PARTITION BY source_id ORDER BY (source_id, schema)".format(
        schema_table_name
    )

def query_get_schema_table_all(schema_table_name = "schema_table"):
    return "SELECT schema, table_name, source_id FROM {} ORDER BY table_name".format(
        schema_table_name
    )

def query_get_schema_table_by_source_id(source_id, schema_table_name = "schema_table"):
    return "SELECT schema, table_name FROM {} where source_id = '{}' ORDER BY table_name".format(
        schema_table_name,
        source_id
    )

def query_create_data_table(column_types_map, data_table_name):
    columns_def_string = ", ".join([ "\"{}\" {}".format(c,t) for c,t in column_types_map.items() ])
    return "CREATE TABLE IF NOT EXISTS {} ({}, __create_at DateTime DEFAULT now(), __collected_at DateTime, __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)".format(
        data_table_name,
        columns_def_string
    )