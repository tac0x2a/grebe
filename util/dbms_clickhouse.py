
from util import time_parser

import json
import datetime
from dateutil.tz import tzutc

def json2clickhouse_sub_list(key, list, types, values):
    items = []
    items_ns = []
    for idx_i, v in enumerate(list):
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

    values[key] = str(items)
    if len(items_ns) > 0:
        values[key + "_ns"] = str(items_ns)

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
        values[key] = dt.astimezone(tz=tzutc()).strftime("%Y-%m-%d %H:%M:%S")
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
    values = { k : str(v) for k,v in values.items() }

    return [types, values]

