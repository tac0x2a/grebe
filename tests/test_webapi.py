import pytest
import json

from unittest.mock import Mock

from grebe import api


@pytest.fixture
def client():
    api.app.config['TESTING'] = True
    with api.app.test_client() as client:
        yield client


def test_web_api_simple(client):
    result = client.get('/')
    assert b'Grebe is running.' == result.data


def test_web_app_show_arguments(client):
    args = {"queue_name": "nayco"}
    api._args = args
    result = client.get('/args')
    assert args == json.loads(result.data)


def test_web_app_show_schema_cache_empty(client):
    grebe = Mock()
    grebe.schema_cache = {}

    api._grebe = grebe
    result = client.get('/schema_cache')
    assert [] == json.loads(result.data)


def test_web_app_show_schema_cache(client):
    grebe = Mock()
    grebe.schema_cache = {
        """
        {
            "source": "city",
            "schema": {\"City\": \"String\", \"Latitude\": \"Float64\", \"Longitude\": \"Float64\"}
        }
        """: "city_place",
        """
        {
            "source": "city",
            "schema": {\"City\": \"String\", \"Pref\": \"String\"}
        }
        """: "city_pref"
    }
    api._grebe = grebe
    result = client.get('/schema_cache')
    expected = [
        {
            "schema": {"City": "String", "Latitude": "Float64", "Longitude": "Float64"},
            "source": "city",
            "table": "city_place"
        },
        {
            "schema": {"City": "String", "Pref": "String"},
            "source": "city",
            "table": "city_pref"
        }
    ]
    assert expected == json.loads(result.data)


def test_web_app_reload_schema_cache(client):
    grebe = Mock()
    grebe.reload_schema.return_value = {'schema_count': 0, 'store': "<class 'grebe.schema_store_clickhouse.SchemaStoreClickhouse'>"}
    api._grebe = grebe
    result = client.get('/schema_cache/reload')
    grebe.reload_schema.assert_called_with()

    expected = {
        'result': 'Success',
        'schema_count': 0,
        'store': "<class 'grebe.schema_store_clickhouse.SchemaStoreClickhouse'>"
    }
    assert expected == json.loads(result.data)


def test_web_app_reload_schema_cache_failed(client):
    grebe = Mock()
    grebe.reload_schema.side_effect = Exception("Unknown Exception!!")
    api._grebe = grebe
    result = client.get('/schema_cache/reload')
    grebe.reload_schema.assert_called_with()

    expected = {
        'result': 'Failed',
        'type': 'Exception',
        'messages': ['Unknown Exception!!']
    }
    assert expected == json.loads(result.data)
    assert 500 == result.status_code


def test_web_app_show_source_settings_cache_empty(client):
    grebe = Mock()
    grebe.source_settings_cache = {}

    api._grebe = grebe
    result = client.get('/source_settings_cache')
    assert [] == json.loads(result.data)


def test_web_app_show_source_settings_cache(client):
    grebe = Mock()
    grebe.source_settings_cache = {
        "city": {"Latitude": "Float64", "Longitude": "Float64"},
        "weather": {"RainFall": "Float64"}
    }

    api._grebe = grebe
    result = client.get('/source_settings_cache')
    expected = [
        {"source_id": "city", "source_settings": {"Latitude": "Float64", "Longitude": "Float64"}},
        {"source_id": "weather", "source_settings": {"RainFall": "Float64"}}
    ]
    print(json.loads(result.data))
    assert expected == json.loads(result.data)


def test_web_app_reload_source_settings_cache(client):
    grebe = Mock()
    grebe.reload_source_settings.return_value = {'store': "<class 'grebe.schema_store_clickhouse.SchemaStoreClickhouse'>"}
    api._grebe = grebe
    result = client.get('/source_settings_cache/reload')
    grebe.reload_source_settings.assert_called_with()

    expected = {
        'result': 'Success',
        'store': "<class 'grebe.schema_store_clickhouse.SchemaStoreClickhouse'>"
    }
    assert expected == json.loads(result.data)


def test_web_app_reload_source_settings_cache_failed(client):
    grebe = Mock()
    grebe.reload_source_settings.side_effect = Exception("Unknown Exception!!")
    api._grebe = grebe
    result = client.get('/source_settings_cache/reload')
    grebe.reload_source_settings.assert_called_with()

    expected = {
        'result': 'Failed',
        'type': 'Exception',
        'messages': ['Unknown Exception!!']
    }
    assert expected == json.loads(result.data)
    assert 500 == result.status_code
