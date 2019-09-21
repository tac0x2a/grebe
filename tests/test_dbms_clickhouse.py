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
      {"hello" : "42",      "world" : "128.4",   "bool" : "1"    , "str" : "Hello,World"}
    ]
    res = dbms_clickhouse.json2lcickhouse(src)
    assert expected == res

def test_return_DateTime_and_UInt32_type_if_DateTime_like_string_provided():
    src = """
    { "hello" : "2018/11/14", "world" : "2018/11/15 11:22:33.123456789", "hoge" : "2018/13/15 11:22:33"}
    """
    expected = [
      {
        "hello" : "DateTime", "hello_ns" : "UInt32",
        "world" : "DateTime", "world_ns" : "UInt32",
        "hoge" : "String"
      },
      {
        "hello" : datetime(2018, 11, 14,  0,  0,  0, 0, timezone(timedelta(hours=0))), "hello_ns" : "0",
        "world" : datetime(2018, 11, 15, 11, 22, 33, 0, timezone(timedelta(hours=0))), "world_ns" : "123456789", # ignnore naoo order in python.
        "hoge" : "2018/13/15 11:22:33"
      }
    ]
    res = dbms_clickhouse.json2lcickhouse(src)
    assert expected == res

if __name__ == '__main__':
    pytest.main(['-v', __file__])