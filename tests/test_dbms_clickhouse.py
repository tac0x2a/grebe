from br2dl import dbms_clickhouse


def test_return_query_create_data_table():
    expected = 'CREATE TABLE IF NOT EXISTS sample_data_table_001 (`sample_f` Nullable(Float64), `sample_b` Nullable(UInt8), __create_at DateTime64(3) DEFAULT now64(3), __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)'
    actual = dbms_clickhouse.query_create_data_table(("sample_f", "sample_b"), ("Float64", "UInt8"), "sample_data_table_001")
    assert expected == actual


def test_return_query_create_data_table_arrays():
    expected = 'CREATE TABLE IF NOT EXISTS sample_data_table_002 (`sample_f` Array(Nullable(Float64)), `sample_s` Array(Nullable(String)), `sample_d` Array(Nullable(DateTime64(6))), __create_at DateTime64(3) DEFAULT now64(3), __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)'
    actual = dbms_clickhouse.query_create_data_table(("sample_f", "sample_s", "sample_d"), ("Array(Float64)", "Array(String)", "Array(DateTime64(6))"), "sample_data_table_002")
    assert expected == actual


def test_return_query_insert_data_table():
    expected = 'INSERT INTO sample_data_table_001 (`sample_f`, `sample_b`) VALUES'
    actual = dbms_clickhouse.query_insert_data_table_without_value(("sample_f", "sample_b"), "sample_data_table_001")
    assert expected == actual
