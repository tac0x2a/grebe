import json
import traceback

from flask import Flask, jsonify
app = Flask(__name__)

_args = None
_grebe = None

@app.route('/')
def hello_world():
    return 'Grebe is running.', 200


@app.route('/args')
def arguments():
    return jsonify(_args), 200


@app.route('/schema_cache')
def schema_cache():
    res = [{'source': json.loads(s)['source'], 'schema': json.loads(s)['schema'], 'table': t} for s, t in _grebe.schema_cache.items()]
    return jsonify(res), 200


@app.route('/schema_cache/reload')
def schema_cache_reload():
    try:
        result = _grebe.reload_schema()
        result['result'] = 'Success'
        return jsonify(result), 200

    except Exception:
        result = {
            'result': 'Failed',
            'stack_trace': traceback.format_exc()
        }
        return jsonify(result), 500


@app.route('/source_settings_cache')
def source_settings__cache():
    res = [{'source_id': s, 'source_settings': m} for s, m in _grebe.source_settings_cache.items()]
    return jsonify(res), 200


@app.route('/source_settings_cache/reload')
def source_settings_cache_reload():
    try:
        result = _grebe.reload_source_settings()
        result['result'] = 'Success'
        return jsonify(result), 200

    except Exception:
        result = {
            'result': 'Failed',
            'stack_trace': traceback.format_exc()
        }
        return jsonify(result), 500
