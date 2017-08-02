import json
import time
import logging
from flask import Flask, request

app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.ERROR)


def log(fields):
    print(json.dumps({'Timestamp': int(time.time() * 1e9), 'Fields': fields}))


def flatten(fields):
    result = {}
    if type(fields) is dict:
        for key, value in fields.items():
            if type(value) in [list, dict]:
                result.update({(key + '.' + k): v for k, v in flatten(value).items()})
            else:
                result[key] = value
    elif type(fields) is list:
        for index, value in enumerate(fields):
            if type(value) in [list, dict]:
                result.update({(str(index) + '.' + k): v for k, v in flatten(value).items()})
            else:
                result[str(index)] = value
    else:
        raise TypeError()
    return result


def normalize(fields):
    if type(fields) is not dict:
        raise TypeError()
    return flatten(fields)


@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@app.route('/<path:path>', methods=['GET', 'POST'])
def catchall(path):
    data = request.get_json(force=True)
    if type(data) is list:
        try:
            logs = [normalize(item) for item in data]
            for item in logs:
                log(item)
        except TypeError:
            return '', 400
    elif type(data) is dict:
        log(normalize(data))
    else:
        return '', 400
    return '', 204


@app.route('/__lbheartbeat__')
def lbheartbeat():
    return '', 200


@app.route('/__heartbeat__')
def heartbeat():
    return 'OK\n', 200


try:
    with open('version.json') as o:
        version_json = o.read()
except FileNotFoundError:
    version_json = '{"version":"notfound"}'


@app.route('/__version__')
def version():
    return version_json, 200