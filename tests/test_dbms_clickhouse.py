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
      {"hello" : "42",      "world" : "128.4",   "bool" : "1"    , "str" : "'Hello,World'"}
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
      # "'#{v.getlocal('+00:00').strftime("%F %T")}'"
        "hello" : "'2018-11-14 00:00:00'", "hello_ns" : "0",
        "world" : "'2018-11-15 11:22:33'", "world_ns" : "123456789", # ignnore naoo order in python.
        "hoge"  : "'2018/13/15 11:22:33'"
      }
    ]
    res = dbms_clickhouse.json2lcickhouse(src)
    assert expected == res

def test_return_nested_values_splited_by__():
  src = """
    { "hello" : 42, "world" : { "value" : 128.4, "bool" : true, "deep" : {"str" : "Hello,World" } } }
    """

  expected = [
      {"hello" : "Float64", "world__value" : "Float64", "world__bool" : "UInt8", "world__deep__str" : "String"},
      {"hello" : "42",      "world__value" : "128.4",   "world__bool" : "1",     "world__deep__str" : "'Hello,World'"}
    ]
  res = dbms_clickhouse.json2lcickhouse(src)
  assert expected == res

def test_return_array_values():
  src = """
    { "hello" : [42, -84, 128], "world" : [128.4, -255.3], "bool" : [true, false, true, false],  "str" : ["Hello", "World", "Hoge"]}
    """

  expected = [
      {"hello" : "Array(Float64)", "world" : "Array(Float64)", "bool" : "Array(UInt8)", "str" : "Array(String)"},
      {"hello" : "[42, -84, 128]",   "world" : "[128.4, -255.3]",  "bool" : "[1, 0, 1, 0]",   "str" : "['Hello', 'World', 'Hoge']"}
    ]
  res = dbms_clickhouse.json2lcickhouse(src)
  assert expected == res

def test_return_String_array_values_if_DateTime_like_strings():
  src = """
    {"hello" : ["2018/11/14", "2018/11/15 11:22:33.123456789"]}
    """

  expected = [{
        "hello"    : "Array(DateTime)",
        "hello_ns" : "Array(UInt32)"
      },
      {
        "hello"    : "['2018-11-14 00:00:00', '2018-11-15 11:22:33']",
        "hello_ns" : "[0, 123456789]"
      }]
  res = dbms_clickhouse.json2lcickhouse(src)
  assert expected == res

def test_return_array_under_object():
  src = """
  { "hello" : 42, "world" : { "value" : [128.4, -255.3] } }
  """

  expected = [
      {"hello" : "Float64", "world__value" : "Array(Float64)"},
      {"hello" : "42",      "world__value" : "[128.4, -255.3]"}
    ]
  res = dbms_clickhouse.json2lcickhouse(src)
  assert expected == res

def test_return_string_Array_if_empyt_array():
  src = """
  { "empty" : [], "nested" : [[]]}
  """

  expected = [
      {"empty" : "Array(String)", "nested" : "Array(String)"},
      {"empty" : "[]",            "nested" : "['[]']" }
    ]
  res = dbms_clickhouse.json2lcickhouse(src)
  assert expected == res

def test_return_String_nested_array():
  src = """
  {
      "hello" : [[1.1, 2.2], [3.3, 4.4]],
      "world" : { "value" : [[1,2], [3,4]]},
      "hoge"  : [{"v": 1}, {"v": 2}]
  }
  """

  expected = [
      {"hello" : "Array(String)", "world__value" : "Array(String)", "hoge" : "Array(String)"},
      {
        "hello"        : "['[1.1, 2.2]', '[3.3, 4.4]']",
        "world__value" : "['[1, 2]', '[3, 4]']",
        "hoge"         : "['{\"v\": 1}', '{\"v\": 2}']",
      }
    ]
  res = dbms_clickhouse.json2lcickhouse(src)
  assert expected == res

def test_return_values_as_string_for_clickhouse_query():
  src = """
    {
      "array" : [1,2,3],
      "hello" : [[1.1, 2.2], [3.3, 4.4]],
      "world" : {"value" : [[1,2], [3,4]]},
      "hoge"  : [{"v":1}, {"v":2}],
      "dates" : ["2019/09/15 14:50:03.101 +0900", "2019/09/15 14:50:03.202 +0900"],
      "date"  : "2019/09/15 14:50:03.042042043 +0900",
      "str"   : "Hello String"
    }
  """
  expected = {
      "array" : "[1, 2, 3]",
      "hello" : "['[1.1, 2.2]', '[3.3, 4.4]']",
      "world__value" : "['[1, 2]', '[3, 4]']",
      "hoge"  : "['{\"v\": 1}', '{\"v\": 2}']",
      "dates" : "['2019-09-15 05:50:03', '2019-09-15 05:50:03']",
      "dates_ns": "[101000000, 202000000]",
      "date"  : "'2019-09-15 05:50:03'",
      "date_ns": "42042043",
      "str"   : "'Hello String'"
    }
  res = dbms_clickhouse.json2lcickhouse(src)
  assert expected == res[1]

if __name__ == '__main__':
    pytest.main(['-v', __file__])