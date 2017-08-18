from datetime import datetime
from flask import Flask, request
from sys import stdout
from ua import ua_parse
import json
import logging

app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.ERROR)
schemas = {
  None: {
    'Timestamp': str,
    'Fields': {
      # default fields, trusted
      'agent': str,
      'method': str,
      'path': str,
      'user_agent_browser': str,
      'user_agent_os': str,
      'user_agent_version': int,
    },
  },
  'view': {
    'Timestamp': str,
    'Fields': {
      'view': int,
      'locale': str,
      'tiles': [{
        'id': int,
        'pin': bool,
        'pos': int,
        'score': int,
        'url': str,
      }],
      # default fields, trusted
      'agent': str,
      'method': str,
      'path': str,
      'user_agent_browser': str,
      'user_agent_os': str,
      'user_agent_version': int,
    },
  },
  'click': {
    'Timestamp': str,
    'Fields': {
      'click': int,
      'block': int,
      'pin': int,
      'unpin': int,
      'sponsored': int,
      'sponsored_link': int,
      'locale': str,
      'tiles': [{
        'id': int,
        'pin': bool,
        'pos': int,
        'score': int,
        'url': str,
      }],
      # default fields, trusted
      'agent': str,
      'method': str,
      'path': str,
      'user_agent_browser': str,
      'user_agent_os': str,
      'user_agent_version': int,
    },
  },
  'ping-centre': {
    'Timestamp': str,
    'Fields': {
      'topic': str,
      'client_id': str,
      'object': str,
      'client_time': int,
      'variants': str,
      'addon_id': str
      'addon_version': str,
      'firefox_version': str,
      'os_name': str,
      'os_version': str,
      'locale': str,
      # from onyx docs not found in ping-centre repo
      'tab_id': int,
      'load_reason': str,
      'source': str,
      'search': int,
      'max_scroll_depth': int,
      'click_position': int,
      'total_bookmarks': int,
      'total_history_size': int,
      'session_duration': int,
      'unload_reason': str,
      # default fields, trusted
      'agent': str,
      'method': str,
      'path': str,
      'user_agent_browser': str,
      'user_agent_os': str,
      'user_agent_version': int,
    },
  },
  'activity-stream': {
    'Timestamp': str,
    'Fields': {
      'action': str,
      'client_id': str,
      'addon_version': str,
      'tab_id': int,
      'load_reason': str,
      'source': str,
      'search': int,
      'max_scroll_depth': int,
      'click_position': int,
      'total_bookmarks': int,
      'total_history_size': int,
      'session_duration': int,
      'unload_reason': str,
      # default fields, trusted
      'agent': str,
      'method': str,
      'path': str,
      'user_agent_browser': str,
      'user_agent_os': str,
      'user_agent_version': int,
    },
  },
}


def get_meta():
    meta = {
        'agent': request.headers.get('User-Agent'),
        'remoteAddressChain': request.headers.get('X-Forwarded-For'),
        'method': request.method,
    }
    ua_parse(meta, 'agent')
    return meta, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def validate(schema, data):
    if type(schema) is dict:
        assert type(data) is dict, 'invalid payload'
        return {key: validate(schema[key], data[key]) for key in schema if key in data}
    elif type(schema) is list:
        assert type(data) is list, 'invalid payload'
        return [validate(schema[0], element) for element in data]
    else:
        return schema(data)


def transform(schema, messages, extra={}):
    meta, time = get_meta()
    for message in messages:
        assert(type(message) is dict)
        for key in ['agent', 'user_agent_browser', 'user_agent_os', 'user_agent_version']:
            if key in message:
                del message[key]
        message.update(meta)
        message.update(extra)
        yield validate(schemas[schema], {'Timestamp': time, 'Fields': message})


def log(schema, messages):
    lines = []
    for message in messages:
        line = '%s:%s\n' % (schema, json.dumps([body, meta], separators=(',', ':')))
        if len(line) > 1024e3:
            raise ValueError('message too long')
        lines.append(line)
    for line in lines:
        stdout.write(line)
    stdout.flush()


@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@app.route('/<path:path>', methods=['GET', 'POST'])
def catchall(path):
    try:
        payload = request.get_json(force=True)
        assert type(payload) in [dict, list], 'invalid payload'
        if type(payload) is dict:
            messages = [payload]
        elif type(body) is list:
            messages = payload
        log(None, transform(None, messages, {'path': path}))
        return '', 204
    except Exception as e:
        return str(e), 400


@app.route('/v2/links/view', methods=['POST'])
@app.route('/v3/links/view', methods=['POST'])
def view():
    try:
        payload = request.get_json(force=True)
        log('view', transform('view', [payload]))
        return '', 204
    except Exception as e:
        return str(e), 400


@app.route('/v2/links/click', methods=['POST'])
@app.route('/v3/links/click', methods=['POST'])
def view():
    try:
        payload = request.get_json(force=True)
        log(transform('click', [payload]))
        return '', 204
    except Exception as e:
        return str(e), 400


@app.route('/v3/links/ping-centre', methods=['POST'])
def view():
    try:
        payload = request.get_json(force=True)
        log(transform('ping-centre', [payload]))
        return '', 204
    except Exception as e:
        return str(e), 400


@app.route('/v3/links/activity-stream', methods=['POST'])
@app.route('/v4/links/activity-stream', methods=['POST'])
def view():
    try:
        payload = request.get_json(force=True)
        log(transform('activity-stream', [payload]))
        return '', 204
    except Exception as e:
        return str(e), 400


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
