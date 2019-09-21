#!/usr/bin/env python

import pytest
from util import time_parser


from datetime import datetime, timezone, timedelta

def assertConverted(src, expected):
    res = time_parser.elastic_time_parse(src)
    assert res == expected

def assertNotValid(src):
    assert src == time_parser.elastic_time_parse(src)

def test_return_time_value_if_string_is_ISO_8601_format():
    assertConverted("2018-12-03T12:34:56-12:00",
                    datetime(2018, 12, 3, 12, 34, 56, 0, timezone(timedelta(hours=-12)))
                    )

def test_return_original_string_Time_value_if_NOT_string_is_NOT_datetime_format():
    assertNotValid("2018-12-03T12:34:56-12:00 hoge")

if __name__ == '__main__':
    pytest.main(['-v', __file__])