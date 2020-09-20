
import pytest
from unittest.mock import MagicMock, patch

from grebe import Grebe


@pytest.fixture(scope='function', autouse=True)
def mocked_grebe():
    client = MagicMock(name='client')
    schema_store = MagicMock(name='schema_store')
    logger = MagicMock(name='logger')
    return (client, schema_store, logger)


@pytest.fixture(scope='function', autouse=True)
def mocked_callback_args():
    channel = MagicMock(name='channnel')
    method = MagicMock(name='method')
    properties = MagicMock(name='properties')
    body = MagicMock(name='body')
    return (channel, method, properties, body)


@pytest.mark.usefixtures("mocked_grebe", "mocked_callback_args")
class TestCallBack():
    def test_insert_data_is_called(self, mocked_grebe, mocked_callback_args):
        (client, schema_store, logger) = mocked_grebe
        schema_store.load_all_schemas.return_value = {}
        grebe = Grebe(client, schema_store, {}, 'exchange_name', 3, "UTC", logger)

        with patch('grebe.Grebe.insert_data') as mock_insert_data:
            channel, method, properties, body = mocked_callback_args
            grebe.callback(channel, method, properties, body)
            mock_insert_data.assert_called_once_with(method, body, client, schema_store, {}, {}, "UTC", logger)

    def test_send_retry_is_called(self, mocked_grebe, mocked_callback_args):
        (client, schema_store, logger) = mocked_grebe
        schema_store.load_all_schemas.return_value = {}
        grebe = Grebe(client, schema_store, {}, 'exchange_name', 3, "UTC", logger)

        with patch('grebe.Grebe.insert_data') as mock_insert_data:
            mock_insert_data.side_effect = Exception('Unknown Exception!!')

            with patch('grebe.Grebe.send_retry') as mock_send_retry:
                channel, method, properties, body = mocked_callback_args
                grebe.callback(channel, method, properties, body)
                mock_send_retry.assert_called_once_with(channel, method, properties, body, 3, 'exchange_name', logger)

    def test_dbms_insert_data_is_called(self, mocked_grebe, mocked_callback_args):
        (client, schema_store, logger) = mocked_grebe
        grebe = Grebe(client, schema_store, {}, 'exchange_name', 3, "UTC", logger)
        grebe.__insert_data = MagicMock(name='__insert_data')

        channel, method, properties, body = mocked_callback_args
        body.decode('utf-8').return_value = '{"Hello": "World"}'
        method.routing_key = "source"

        with patch('lakeweed.clickhouse.data_string2type_value') as lw:
            lw.return_value = (['Hello'], ['String'], [('World',)])

            with patch('br2dl.dbms_clickhouse.insert_data') as insert_data:
                insert_data.return_value
                grebe.callback(channel, method, properties, body)
                insert_data.assert_called_once_with(client, "source_001", ['Hello'], [('World',)])
