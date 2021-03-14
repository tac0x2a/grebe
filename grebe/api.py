import json
from fastapi import FastAPI, Response, status

app = FastAPI()

_args = None
_grebe = None


@app.get('/', status_code=status.HTTP_200_OK)
def hello_world():
    return 'Grebe is running.'


@app.get('/args', status_code=status.HTTP_200_OK)
def arguments():
    return _args


@app.get('/schema_cache', status_code=status.HTTP_200_OK)
def schema_cache():
    res = [{'source': json.loads(s)['source'], 'schema': json.loads(s)['schema'], 'table': t} for s, t in _grebe.schema_cache.items()]
    return res


@app.get('/schema_cache/reload', status_code=status.HTTP_200_OK)
def schema_cache_reload(response: Response):
    try:
        result = _grebe.reload_schema()
        result['result'] = 'Success'
        return result

    except Exception as e:
        result = {
            'result': 'Failed',
            'type': type(e).__name__,
            'messages': e.args
        }
        response.status_code = 500
        return result


@app.get('/source_settings_cache', status_code=status.HTTP_200_OK)
def source_settings__cache():
    res = [{'source_id': s, 'source_settings': m} for s, m in _grebe.source_settings_cache.items()]
    return res


@app.get('/source_settings_cache/reload', status_code=status.HTTP_200_OK)
def source_settings_cache_reload(response: Response):
    try:
        result = _grebe.reload_source_settings()
        result['result'] = 'Success'
        return result

    except Exception as e:
        result = {
            'result': 'Failed',
            'type': type(e).__name__,
            'messages': e.args
        }
        response.status_code = 500
        return result
