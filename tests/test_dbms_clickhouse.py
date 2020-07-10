#!/usr/bin/env python

import pytest
from br2dl import dbms_clickhouse
from datetime import datetime, timezone, timedelta


def test_return_query_create_data_table():
    expected = dbms_clickhouse.query_create_data_table({"sample_f": "Float64", "sample_b": "UInt8"}, "sample_data_table_001")
    actual = "CREATE TABLE IF NOT EXISTS sample_data_table_001 (\"sample_f\" Nullable(Float64), \"sample_b\" Nullable(UInt8), __create_at DateTime DEFAULT now(), __collected_at DateTime, __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)"
    assert expected == actual


def test_return_query_insert_data_table():
    expected = dbms_clickhouse.query_insert_data_table_without_value({"sample_f": 42.195, "sample_b": 1}.keys(), "sample_data_table_001")
    actual = 'INSERT INTO sample_data_table_001 ("sample_f", "sample_b") VALUES'
    assert expected == actual


if __name__ == '__main__':
    pytest.main(['-v', __file__])
