
from util import time_parser

import json
import datetime
from dateutil.tz import tzutc

def guess_type(value):
    # Number?
    if type(value) is float:
        return ["Float64", float(value)]
    if type(value) is int:
        return ["Float64", int(value)]
    if type(value) is bool:
        v = 1 if value else 0
        return ["UInt8", v]

    return guess_type_str(value)

def guess_type_str(str_value):
    value = time_parser.elastic_time_parse(str_value)
    if type(value) is datetime.datetime:
        return ["DateTime", value]

    return ["String", value]

def json2lcickhouse_sub(key, body, types, values):
    # if type(body) is map:
    #     for child_key, child_value in body.items():
    #         json2lcickhouse_sub(child_key, child_value, types, values)

    # is atomic type.
    value = body

    if type(value) is float:
        values[key] = str(float(value))
        types[key] = "Float64"
        return
    if type(value) is int:
        values[key] = str(int(value))
        types[key] = "Float64"
        return
    if type(value) is bool:
        values[key] = '1' if value else '0'
        types[key] = "UInt8"
        return

    # is string. try to parse as datetime.
    try:
        [dt, ns] = time_parser.elastic_time_parse(value)
        values[key] = dt.astimezone(tz=tzutc()).strftime("%Y-%m-%d %H:%M:%S")
        types[key] = "DateTime"
        # Clickhouse can NOT contain ms in DateTime column.
        values[key + "_ns"] = str(ns)
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

    return [types, values]

