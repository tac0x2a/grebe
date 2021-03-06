from grebe.schema_store_clickhouse import SchemaStoreClickhouse as sc

# ---------------------------------------------------------------------


def test_return_query_create_schema_table():
    expected = "CREATE TABLE IF NOT EXISTS `schema_table` (__create_at DateTime64(3) DEFAULT now64(3), source_id String, schema String, table_name String) ENGINE = MergeTree PARTITION BY source_id ORDER BY (source_id, schema)"
    actual = sc.query_create_schema_table(schema_table_name="schema_table")
    assert expected == actual


def test_return_query_create_schema_table_escape():
    expected = "CREATE TABLE IF NOT EXISTS `schema_table\\`sample` (__create_at DateTime64(3) DEFAULT now64(3), source_id String, schema String, table_name String) ENGINE = MergeTree PARTITION BY source_id ORDER BY (source_id, schema)"
    actual = sc.query_create_schema_table(schema_table_name="schema_table`sample")
    assert expected == actual


def test_return_query_get_schema_table_all():
    expected = "SELECT schema, table_name, source_id FROM `__schema` ORDER BY table_name"
    actual = sc.query_get_schema_table_all("__schema")
    assert expected == actual


def test_return_query_get_schema_table_all_sample():
    expected = "SELECT schema, table_name, source_id FROM `__schema\\`test` ORDER BY table_name"
    actual = sc.query_get_schema_table_all("__schema`test")
    assert expected == actual


def test_return_query_insert_schema_table_without_value():
    expected = "INSERT INTO `__schema` (source_id, schema, table_name) VALUES"
    actual = sc.query_insert_schema_table_without_value("__schema")
    assert expected == actual


def test_return_query_insert_schema_table_without_value_escape():
    expected = "INSERT INTO `__schema\\`sample` (source_id, schema, table_name) VALUES"
    actual = sc.query_insert_schema_table_without_value("__schema`sample")
    assert expected == actual
