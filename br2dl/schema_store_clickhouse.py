from . import time_parser

import json
import datetime
from dateutil.tz import tzutc

import logging
logger = logging.getLogger("schema_store_clickhouse")


class SchemaStoreClickhouse():
    def __init__(self, client, schema_table_name="schema_table"):
        self.client = client
        self.schema_table_name = schema_table_name
        self.__create_schema_table(client)

    def load_all_schemas(self):
        return self.__select_all_schemas(self.client)

    def store_schema(self, source_id, new_table_name, schema):
        return self.__insert_schema(self.client, source_id, new_table_name, schema)

    # ----------------------------------------------------
    def __create_schema_table(self, client):
        query = self.query_create_schema_table()
        client.execute(query)

    def __select_all_schemas(self, client):
        query = self.query_get_schema_table_all()
        tables = client.execute(query)  # [(schema, table_name, source_id),]
        return {s: n for (s, n, i) in tables}

    def __insert_schema(self, client, source_id, data_table_name, schema):
        query = self.query_insert_schema_table_without_value()
        client.execute(query, [{"source_id": source_id, "schema": schema, "table_name": data_table_name}])

    # ----------------------------------------------------
    @classmethod
    def query_create_schema_table(self, schema_table_name="schema_table"):
        return "CREATE TABLE IF NOT EXISTS {} (__create_at DateTime DEFAULT now(), source_id String, schema String, table_name String) ENGINE = MergeTree PARTITION BY source_id ORDER BY (source_id, schema)".format(schema_table_name)

    @classmethod
    def query_get_schema_table_all(self, schema_table_name="schema_table"):
        return "SELECT schema, table_name, source_id FROM {} ORDER BY table_name".format(schema_table_name)

    @classmethod
    def query_insert_schema_table_without_value(self, schema_table_name="schema_table"):
        return "INSERT INTO {} (source_id, schema, table_name) VALUES".format(schema_table_name)
