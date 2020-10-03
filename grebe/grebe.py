import pika

from lakeweed import clickhouse as d2c

from . import dbms_clickhouse as dbms
from .dbms_clickhouse import TableNotFoundException


class Grebe:
    def __init__(self, client, schema_store, meta_store, resend_exchange_name, retry_max: int, tz_str: str, logger):
        self.client = client
        self.schema_store = schema_store
        self.meta_store = meta_store
        self.resend_exchange_name = resend_exchange_name
        self.retry_max = retry_max
        self.tz_str = tz_str
        self.logger = logger

        self.reload_schema()
        self.logger.info(f"Schemas: {[s for s in self.schema_cache.values()]}")

        meta = self.meta_store.load_all_meta_data()
        logger.info(f"Loaded meta data: {meta}")

        self.specified_types_cache = {source_id: body['types'] for source_id, body in meta.items() if 'types' in body}
        logger.info(f"Specified Types cache: {self.specified_types_cache}")

        for s, b in meta.items():
            logger.info(f"{s}:{b}")

    def callback(self, channel, method, properties, body):
        self.logger.debug("receive '{}({})'".format(method.routing_key, method.delivery_tag))

        try:
            Grebe.insert_data(
                method, body,
                self.client,
                self.schema_store,
                self.schema_cache,
                self.specified_types_cache,
                self.tz_str,
                self.logger
            )
        except TableNotFoundException as e:
            self.logger.error(f"Table '{ e.table_name }' is renamed? Update schema table cache.")
            self.reload_schema()

            self.logger.error("Consume failed '{}'. retrying...".format(method.routing_key))
            Grebe.send_retry(
                channel, method, properties, body,
                self.retry_max,
                self.resend_exchange_name,
                self.logger
            )

        except Exception as e:
            self.logger.error(e, exc_info=e)
            self.logger.error("Consume failed '{}'. retrying...".format(method.routing_key))
            Grebe.send_retry(
                channel, method, properties, body,
                self.retry_max,
                self.resend_exchange_name,
                self.logger
            )

        finally:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.debug("return basic_ack '{}({})'".format(method.routing_key, method.delivery_tag))

    def reload_schema(self):
        self.schema_cache = self.schema_store.load_all_schemas()
        self.logger.info(f"Load {len(self.schema_cache)} schemas from {self.schema_store}")
        return {'schema_count': len(self.schema_cache), 'store': str(type(self.schema_store))}

    @classmethod
    def insert_data(cls, method, body, client, schema_store, schema_cache, specified_types, tz_str, logger):
        # replace delimiter in topic mqtt/amqp.
        topic = method.routing_key
        source_id = topic.replace("/", "_").replace(".", "_")
        payload = str(body.decode('utf-8'))

        if source_id in specified_types.keys():
            spec_types = specified_types[source_id]
        else:
            spec_types = {}

        (columns, types, values_list) = d2c.data_string2type_value(payload, specified_types=spec_types, tz_str=tz_str)

        serialized = dbms.serialize_schema(columns, types, source_id)
        data_table_name = dbms.get_table_name_with_insert_if_new_schema(client, schema_store, source_id, columns, types, serialized, schema_cache)
        dbms.insert_data(client, data_table_name, columns, values_list)

        logger.debug(serialized)

    @classmethod
    def send_retry(cls, channel, method, properties, body, retry_max, resend_exchange_name, logger):
        RetryCountKey = "x-grebe-retry-count"

        current_retry_count = 0

        if properties.headers and properties.headers.get(RetryCountKey):
            current_retry_count = int(properties.headers.get(RetryCountKey))

        if current_retry_count >= retry_max:
            logger.error("Retry count exceeded!!({}). Discard Message = [exchange={}, routing_key={}, body={}]".format(retry_max, resend_exchange_name, method.routing_key, body))
            return

        props = pika.BasicProperties(
            headers={RetryCountKey: current_retry_count + 1}
        )
        logger.debug("Re-sending [exchange={}, routing_key={}, props={}, body={}]".format(resend_exchange_name, method.routing_key, props, body))
        channel.basic_publish(exchange=resend_exchange_name, routing_key=method.routing_key, properties=props, body=body)
        logger.warning("Re-send complete. ({})".format(current_retry_count + 1))
