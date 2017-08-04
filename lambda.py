import os
import json
import re
import datetime
import boto3
import urllib
import gzip


def ua_basic(ua):
    parts = ua.rsplit('/', 1)
    if len(parts) == 2:
        match = re.match('^(\d+)', parts[1])
        if match:
            return int(match.group()), None, None
    return None, None, None


def ua_keyword(kw, browser=None, os=None):
    def f(ua):
        i = ua.find(kw)
        if i > -1:
            i += len(kw)
            match = re.match('^(\d+)', ua[i:])
            if match:
                return int(match.group()), browser, os
        return None, browser, os
    return f


ua_os_matchers = [
    ('iPod', 'iPod'),
    ('iPad', 'iPad'),
    ('iPhone', 'iPhone'),
    ('Android', 'Android'),
    ('BlackBerry', 'BlackBerry'),
    ('Linux', 'Linux'),
    ('Macintosh', 'Macintosh'),
    ('Mozilla/5.0 (Mobile;', 'FirefoxOS'),
    ('Windows NT 10.0', 'Windows 10'),
    ('Windows NT 6.3', 'Windows 8.1'),
    ('Windows NT 6.2', 'Windows 8'),
    ('Windows NT 6.2', 'Windows 7'),
    ('Windows NT 6.0', 'Windows Vista'),
    ('Windows NT 5.1', 'Windows XP'),
    ('Windows NT 5.0', 'Windows 2000'),
]

ua_browser_matchers = [
    ('Edge', ua_keyword('Edge/')),
    ('Chrome', ua_keyword('Chrome/')),
    ('Opera Mini', ua_basic),
    ('Opera Mobi', ua_basic),
    ('Opera', ua_basic),
    ('MSIE', ua_keyword('MSIE ')),
    ('Trident/7.0', lambda ua: (11, 'MSIE', None)),
    ('Safari', ua_basic),
    (
        'Firefox AndroidSync',
        ua_keyword('Firefox AndroidSync ', 'FxSync', 'Android')
    ),
    ('Firefox-iOS-Sync', ua_keyword('Firefox-iOS-Sync/', 'FxSync', 'iOS')),
    ('Firefox', ua_keyword('Firefox/')),
]


def ua_parse(
    log,
    field,
    prefix='user_agent_',
    ua_os_matchers=ua_os_matchers,
    ua_browser_matchers=ua_browser_matchers,
):
    if field not in log:
        return
    ua, browser, version, os = log[field], None, None, None
    for key, f in ua_browser_matchers:
        if key in ua:
            version, browser, os = f(ua)
            if browser is None:
                browser = key
            break
    if os is None:
        for key, value in ua_os_matchers:
            if key in ua:
                os = value
                break
    if browser is not None:
        del log[field]
        log[prefix+'browser'] = browser
    if version is not None:
        log[prefix+'version'] = version
    if os is not None:
        log[prefix+'os'] = os


def validate(schema, log):
    if type(schema) is dict:
        if type(log) is not dict:
            raise ValueError('invalid log')
        return {key: validate(schema[key], log[key]) for key in schema if key in log}
    elif type(schema) is list:
        if type(log) is not list:
            raise ValueError('invalid log')
        return [validate(schema[0], element) for element in log]
    elif type(schema) is type:
        try:
            return schema(log)
        except:
            raise ValueError('invalid log')
    else:
        raise TypeError('invalid schema')


path_schema_map = {}
schemas = {
    None: {
        'Timestamp': str,
        'Fields': {
            'agent': str,
            'method': str,
            'path': str,
#           'remoteAddressChain': str,
        },
    },
}


def transform(line):
    if prefix_delimiter is not None:
        line = line.split(prefix_delimiter, 1).pop()
    try:
        log = json.loads(line)
    except:
        return None, None
    assert(type(log) is dict)
    log['Timestamp'] = datetime.datetime.fromtimestamp(
        log['Timestamp']/1e9
    ).strftime('%Y-%m-%d %H:%M:%S')
    if 'Fields' not in log:
        return None, log
    fields = log['Fields']
    for field in ['email', 'uid', 'remoteAddressChain', 'ip']:
        if field in fields:
            del fields[field]
    ua_parse(fields, 'agent')
    schema = path_schema_map[fields.get('path')]
    try:
        log = validate(schemas[schema], log)
    except ValueError:
        return None, None


s3 = boto3.client('s3')
schemas


def handle(key, bucket):
    print('{"input":"s3://%s/%s"}' % (bucket, key))
    base, _, log, date, hour = re.match(
        r'^.*/((logging.s3.)?(.*)-[0-9]+-(20[0-9]{2}-[0-9]{2}-[0-9]{2})-([0-9]{2})-.*.gz)$',
        key,
    ).groups()
    app = log.split('.', 1)[0]
    src_path, err_path, dst_paths = '/tmp/'+base, '/tmp/err+'+base, []
    out, err = False, False
    key_suffix = '/date=%s/hour=%s/%s' % (date, hour, base)
    key_prefix = '/%s/log=%s' % (app, log)
    err_key = 'lambda/transform-errors' % (app, log, date, hour, base)
    if key.endswith('.gz'):
        open = gzip.open
    s3.download_file(bucket, key, src_path)
    with open(src_path, 'r') as s, open(err_path, 'w') as e:
        for l in s:
            try:
                schema, log = transform(l)
                if log is not None:
                    if schema not in dst_paths:
                        path = '/tmp/dst+%s+%s' % (schema, base)
                        dst_paths[schema] = [path, open(path, 'w')]
                    dst_paths[schema][1].write(json.dumps(log, separators=(',', ':')) + '\n')
            except:
                e.write(l)
                err = True
    if err:
        err_key = 'lambda/transform-errors/%s/log=%s/date=%s/hour=%s/%s' % (app, log, date, hour, base)
        s3.upload_file(err_path, bucket, err_key)
        print('{"errors":"s3://%s/%s"}' % (bucket, err_key))
    for schema in dst_paths:
        path, fp = dst_paths[schema]
        fp.close()
        dst_key = 'json/%s/log=%s/schema=%s/date=%s/hour=%s/%s' % (app, schema, log, date, hour, base)
        if schema is None:
            dst_key = 'json/%s/log=%s/date=%s/hour=%s/%s' % (app, log, date, hour, base)
        s3.upload_file(path, bucket + '-parquet', dst_key)
        s3.upload_file(path, bucket, dst_key)
        print('{"output":"s3://%s/%s"}' % (bucket, dst_key))
    os.remove(src_path)
    os.remove(err_path)
    map(os.remove, [path for _, path in dst_paths.values()])


def lambda_handler(event, context):
    for record in event['Records']:
        if 'object' in record['s3']:
            handle(
                urllib.unquote(record['s3']['object']['key']),
                record['s3']['bucket']['name'],
            )
