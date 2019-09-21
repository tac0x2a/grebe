import json

import re

NumberPattern = re.compile('^\d+\.\d+$')

def guess_type(value):
    # Number?
    if type(value) is float:
        return ["Float64", float(value)]
    if type(value) is int:
        return ["Float64", int(value)]
    if type(value) is bool:
        v = 1 if value else 0
        return ["UInt8", v]

    return ["String", str(value)]


def json2lcickhouse_sub(body, values, types):
    for key, value in body.items():
        (v, t) = guess_type(value)
        values[key] = v
        types[key] = t
    return

def json2lcickhouse(src_json_str, logger = None):
    """Convert json string to python dict with data types for Clickhouse"""
    body = json.loads(src_json_str)

    types = {}
    values = {}

    json2lcickhouse_sub(body, types, values)

    return [types, values]

