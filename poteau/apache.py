#!/usr/bin/env python

import re
import os

os.environ["UA_PARSER_YAML"] = "./regexes.yaml"

from ua_parser import user_agent_parser


RE_COMMON = re.compile(r'(.*?) - (.*?) \[(.*?) [+-]\d{4}\] "(.*?) (.*?) (.*?)" (\d{3}) (\d+)', re.U)
RE_COMBINED = re.compile(r'(.*?) - (.*?) \[(.*?) [+-]\d{4}\] "(.*?) (.*?) (.*?)" (\d{3}) (.*?) "([^"]*)" "([^"]*)"', re.U)
RE_DATE = re.compile(r'(\d+)/(\w{3})/(\d+):(\d+:\d+:\d+)', re.U)


def parse_date(src):
    """
10/Oct/2000:13:55:36
2009-11-15T14:12:12
    """
    m = RE_DATE.match(src)
    MONTH = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    resp = '%(year)s-%(month)02d-%(day)02dT%(hms)s' % {
        'year': m.group(3),
        'month': MONTH.index(m.group(2)) + 1,
        'day': int(m.group(1)),
        'hms': m.group(4)
    }
    return resp


def intOrZero(a):
    if a == '-':
        return 0
    return int(a)


def unescape(txt):
    if txt is None:
        return None
    return "_".join(txt.split(" "))


def combined(reader):
    for line in reader:
        m = RE_COMBINED.match(line)
        ua = user_agent_parser.Parse(m.group(10))
        ua['os']['family'] = unescape(ua['os']['family'])
        ua['device']['family'] = unescape(ua['device']['family'])
        if m is not None:
            yield {
                'ip': m.group(1),
                'user': m.group(2),
                'date': parse_date(m.group(3)),
                'command': m.group(4),
                'url': m.group(5),
                'http': m.group(6),
                'code': int(m.group(7)),
                'size': intOrZero(m.group(8)),
                'referer': m.group(9),
                'user-agent': ua,
                'raw': line
            }


def documents_from_combined(logs):
    for log in logs:
        yield {
            '@source': 'stuff://',
            '@type': 'combined',
            '@tags': [],
            '@fields': log,
            '@timestamp': log['date'],
            '@message': log['raw']
        }


if __name__ == "__main__":
    import sys

    from pyelasticsearch import ElasticSearch

    from __init__ import Kibana

    # Instantiate it with an url
    es = ElasticSearch(sys.argv[1])
    # Kibana need this kind of name
    k = Kibana(es)
    logs = combined(sys.stdin)
    for n in k.index_documents('combined', documents_from_combined(logs)):
        print(n)
