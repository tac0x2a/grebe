#!/usr/bin/env python

import pytest
from util import time_parser


from datetime import datetime, timezone, timedelta

def test_return_time_value_if_string_is_ISO_8601_format():
    """'return Time value if string is ISO 8601 format"""
    src = "2018-12-03T12:34:56-12:00"
    expected = datetime(2018, 12, 3, 12, 34, 56, 0, timezone(timedelta(hours=-12)))
    res = time_parser.elastic_time_parse(src)
    assert res == expected

if __name__ == '__main__':
    pytest.main(['-v', __file__])