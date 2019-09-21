
from util import time_parser

import json
import datetime

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

def json2lcickhouse_sub(key, body, values, types):
    if type(body) is map:
        for child_key, child_value in body.items():
            json2lcickhouse_sub(child_key, child_value, values, types)

    # Number?
    if type(body) is float:
        values[key] = float(value)
        types[key] = "Float64"
    if type(body) is int:
        return ["Float64", int(body)]
    if type(body) is bool:
        v = 1 if body else 0
        return ["UInt8", v]

    return

def json2lcickhouse(src_json_str, logger = None):
    """Convert json string to python dict with data types for Clickhouse"""
    body = json.loads(src_json_str)

    types = {}
    values = {}

    for key, value in body.items():
        json2lcickhouse_sub(key, body, types, values)

    return [types, values]

