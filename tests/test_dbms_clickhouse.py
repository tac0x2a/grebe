#!/usr/bin/env python

import pytest
from util import dbms_clickhouse

from datetime import datetime, timezone, timedelta

def test_return_empty_set():
    src = "{}"
    expected = [{},{}]
    res = dbms_clickhouse.json2lcickhouse(src)
    assert res == expected

if __name__ == '__main__':
    pytest.main(['-v', __file__])