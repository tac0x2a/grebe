import json

from flask import Flask
app = Flask(__name__)

_args = None
_grebe = None

@app.route('/')
def hello_world():
    return 'Grebe is running.'

@app.route('/args')
def arguments():
    return str(json.dumps(_args))
