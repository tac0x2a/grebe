import pytest

from br2dl.webapi import app


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


def test_web_api_simple(client):
    result = client.get('/')
    assert b'{"hello":"grebe"}\n' == result.data
