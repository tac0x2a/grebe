from grebe import dbms_clickhouse


def test_return_query_create_data_table():
    expected = 'CREATE TABLE IF NOT EXISTS `sample_data_table_001` (`sample_f` Nullable(Float64), `sample_b` Nullable(UInt8), __create_at DateTime64(3) DEFAULT now64(3), __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)'
    actual = dbms_clickhouse.query_create_data_table(("sample_f", "sample_b"), ("Float64", "UInt8"), "sample_data_table_001")
    assert expected == actual


def test_return_query_create_data_table_escape():
    expected = 'CREATE TABLE IF NOT EXISTS `hello\\`sample_data\\`_table\\`_001` (`sample_f\\`test` Nullable(Float64), `test\\`sample\\`_b` Nullable(UInt8), __create_at DateTime64(3) DEFAULT now64(3), __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)'
    actual = dbms_clickhouse.query_create_data_table(("sample_f`test", "test`sample`_b"), ("Float64", "UInt8"), "hello`sample_data`_table`_001")
    assert expected == actual


def test_return_query_create_data_table_arrays():
    expected = 'CREATE TABLE IF NOT EXISTS `sample_data_table_002` (`sample_f` Array(Nullable(Float64)), `sample_s` Array(Nullable(String)), `sample_d` Array(Nullable(DateTime64(6))), __create_at DateTime64(3) DEFAULT now64(3), __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY (__create_at)'
    actual = dbms_clickhouse.query_create_data_table(("sample_f", "sample_s", "sample_d"), ("Array(Float64)", "Array(String)", "Array(DateTime64(6))"), "sample_data_table_002")
    assert expected == actual


def test_return_query_insert_data_table():
    expected = 'INSERT INTO `sample_data_table_001` (`sample_f`, `sample_b`) VALUES'
    actual = dbms_clickhouse.query_insert_data_table_without_value(("sample_f", "sample_b"), "sample_data_table_001")
    assert expected == actual


def test_return_query_insert_data_table_escape():
    expected = 'INSERT INTO `sample_data\\`_table_001` (`sample_f\\`test`, `test\\`sample_b`) VALUES'
    actual = dbms_clickhouse.query_insert_data_table_without_value(("sample_f`test", "test`sample_b"), "sample_data`_table_001")
    assert expected == actual


def test_serialize_schema_simple():
    expected = '{"source":"test_source","schema":{"f":"Nullable(Float64)"}}'
    actual = dbms_clickhouse.serialize_schema(('f', ), ('Nullable(Float64)', ), "test_source")
    assert expected == actual


def test_serialize_schema_simple_without_escape():
    expected = '{"source":"test_`source","schema":{"f`sample":"Nullable(Float64)"}}'
    actual = dbms_clickhouse.serialize_schema(('f`sample', ), ('Nullable(Float64)', ), "test_`source")
    assert expected == actual


def test_serialize_schema_sorted_order():
    expected = '{"source":"test_source","schema":{"a":"Nullable(Float64)","b":"Nullable(Int64)"}}'
    actual = dbms_clickhouse.serialize_schema(('b', 'a'), ('Nullable(Int64)', 'Nullable(Float64)'), "test_source")
    assert expected == actual


def test_escape_symbol():
    expected = 'Hello\\`World'
    actual = dbms_clickhouse.escape_symbol('Hello`World')
    assert expected == actual


def test_generate_new_table_name():
    schema_cache = {
        '{"source":"source","schema":{col":"Float64"}}': 'source_001',
        '{"source":"source","schema":{"col":"Int64"}}': 'source_002'
    }
    source_id = 'source'
    expected = 'source_003'
    actual = dbms_clickhouse.generate_new_table_name(source_id, schema_cache)
    assert expected == actual


def test_generate_new_table_name_new_table_cache_is_empty():
    schema_cache = {}
    source_id = 'source'
    expected = 'source_001'
    actual = dbms_clickhouse.generate_new_table_name(source_id, schema_cache)
    assert expected == actual


def test_generate_new_table_name_new_table_isolated_other_table():
    schema_cache = {
        '{"test":"source","schema":{col":"Float64"}}': 'test_001',
        '{"test":"source","schema":{"col":"Int64"}}': 'test_002'
    }
    source_id = 'source'
    expected = 'source_001'
    actual = dbms_clickhouse.generate_new_table_name(source_id, schema_cache)
    assert expected == actual


def test_generate_new_table_name_use_unused_number():
    schema_cache = {
        '{"source":"source","schema":{col":"Float64"}}': 'source_001',
        '{"source":"source","schema":{"col":"Int64"}}': 'source_003'
    }
    source_id = 'source'
    expected = 'source_002'
    actual = dbms_clickhouse.generate_new_table_name(source_id, schema_cache)
    assert expected == actual


def test_generate_new_table_name_isolated_other_tables():
    schema_cache = {
        '{"source_hello":"source","schema":{col":"Float64"}}': 'source_hello_001',
        '{"source_hello":"source","schema":{"col":"Int64"}}': 'source_hello_002',
        '{"source":"source","schema":{"col":"Int64"}}': 'source_001'
    }
    source_id = 'source'
    expected = 'source_002'
    actual = dbms_clickhouse.generate_new_table_name(source_id, schema_cache)
    assert expected == actual

