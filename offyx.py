import json
import time
import logging
from flask import Flask, request

app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.ERROR)


def log(fields):
    line = json.dumps({'Timestamp': int(time.time() * 1e9), 'Fields': fields})
    if len(line) > 1024e3:
        raise ValueError('body too long')
    print(line)


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
        raise TypeError('could not flatten fields')
    return result


def normalize(fields):
    if type(fields) is not dict:
        raise TypeError('fields is not a dict')
    return flatten(fields)


@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@app.route('/<path:path>', methods=['GET', 'POST'])
def catchall(path):
    meta = {
        'path': path,
        'agent': request.headers.get('User-Agent'),
        'remoteAddressChain': request.headers.get('X-Forwarded-For'),
        'method': request.method,
    }
    data = request.get_json(force=True)
    try:
        if type(data) is list:
            logs = [normalize(item) for item in data]
            for item in logs:
                log({**item, **meta})
        # elif dict type bulk api call:
        #     logs = [normalize(item) for item in data['bulk']]
        #     for item in logs:
        #         log({**item, **meta})
        elif type(data) is dict:
            log(normalize({**data, **meta}))
        else:
            return '', 400
        return '', 204
    except (TypeError, ValueError):
        return '', 400


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
