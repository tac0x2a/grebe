import json
from . import dbms_clickhouse as ch


class SourceSettingStoreClickhouse():
    def __init__(self, client, ss_table_name, logger):
        self.client = client
        self.ss_table_name = ss_table_name
        self.__create_table(client)

    def load_all_source_settings(self):
        select_result = self.__select_all(self.client)
        return {src: json.loads(setting_str) for src, setting_str in select_result.items()}

    def store_source_setting(self, source_id, source_setting):
        # Todo upsert.
        return self.__insert(self.client, source_id, source_setting)

    # ----------------------------------------------------
    def __create_table(self, client):
        query = self.query_create_table(self.ss_table_name)
        client.execute(query)

    def __select_all(self, client):
        query = self.query_get_all(self.ss_table_name)
        tables = client.execute(query)  # [(source_id, source_setting),]
        return {s: m for (s, m) in tables}

    def __insert(self, client, source_id, source_setting):
        query = self.query_insert_without_value(self.ss_table_name)
        client.execute(query, [{"source_id": source_id, "source_setting": source_setting}])

    def __update(self, client, source_id, source_setting):
        query = self.query_update_without_value(self.ss_table_name)
        client.execute(query, [{"source_id": source_id, "source_setting": source_setting}])

    # ----------------------------------------------------
    @classmethod
    def query_create_table(self, table_name):
        return f"CREATE TABLE IF NOT EXISTS `{ch.escape_symbol(table_name)}` (source_id String, source_setting String, __create_at DateTime64(3) DEFAULT now64(3)) ENGINE = MergeTree PARTITION BY source_id ORDER BY source_id"

    @classmethod
    def query_get_all(self, table_name):
        return f"SELECT source_id, source_setting FROM `{ch.escape_symbol(table_name)}` ORDER BY source_id"

    @classmethod
    def query_insert_without_value(self, table_name):
        return f"INSERT INTO `{ch.escape_symbol(table_name)}` (source_id, source_setting) VALUES"

    @classmethod
    def query_update_without_value(self, table_name):
        return f"ALTER TABLE `{ch.escape_symbol(table_name)}` UPDATE source_setting = %(source_setting)s WHERE source_id = %(source_id)s"
