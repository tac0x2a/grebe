from . import dbms_clickhouse as ch


class MetaStoreClickhouse():
    def __init__(self, client, metastore_table_name, logger):
        self.client = client
        self.metastore_table_name = metastore_table_name
        self.__create_table(client)

    def load_all_meta_data(self):
        return self.__select_all(self.client)

    def store_source_meta_data(self, source_id, source_metadata):
        # Todo upsert.
        return self.__insert(self.client, source_id, source_metadata)

    # ----------------------------------------------------
    def __create_table(self, client):
        query = self.query_create_table(self.metastore_table_name)
        client.execute(query)

    def __select_all(self, client):
        query = self.query_get_all(self.metastore_table_name)
        tables = client.execute(query)  # [(source_id, metadata),]
        return {s: m for (s, m) in tables}

    def __insert(self, client, source_id, metadata):
        query = self.query_insert_without_value(self.metastore_table_name)
        client.execute(query, [{"source_id": source_id, "metadata": metadata}])

    def __update(self, client, source_id, metadata):
        query = self.query_update_without_value(self.metastore_table_name)
        client.execute(query, [{"source_id": source_id, "metadata": metadata}])

    # ----------------------------------------------------
    @classmethod
    def query_create_table(self, table_name):
        return f"CREATE TABLE IF NOT EXISTS `{ch.escape_symbol(table_name)}` (source_id String, metadata String, __create_at DateTime64(3) DEFAULT now64(3)) ENGINE = MergeTree PARTITION BY source_id ORDER BY source_id"

    @classmethod
    def query_get_all(self, table_name):
        return f"SELECT source_id, metadata FROM `{ch.escape_symbol(table_name)}` ORDER BY source_id"

    @classmethod
    def query_insert_without_value(self, table_name):
        return f"INSERT INTO `{ch.escape_symbol(table_name)}` (source_id, metadata) VALUES"

    @classmethod
    def query_update_without_value(self, table_name):
        return f"ALTER TABLE `{ch.escape_symbol(table_name)}` UPDATE metadata = %(metadata)s WHERE source_id = %(source_id)s"
