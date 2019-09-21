#!/usr/bin/env python

import pytest
from util import dbms_clickhouse

from datetime import datetime, timezone, timedelta

def test_return_empty_set():
    src = "{}"
    expected = [{},{}]
    res = dbms_clickhouse.json2lcickhouse(src)
    assert res == expected

def test_return_basic_type_and_values():
    src = """
    { "hello" : 42, "world" : 128.4, "bool" : true, "str" : "Hello,World" }
    """

    expected = [
      {"hello" : "Float64", "world" : "Float64", "bool" : "UInt8", "str" : "String"},
      {"hello" : 42,        "world" : 128.4,     "bool" : 1      , "str" : "Hello,World"}
    ]
    res = dbms_clickhouse.json2lcickhouse(src)
    assert expected == res

if __name__ == '__main__':
    pytest.main(['-v', __file__])