#!/usr/bin/env python

import re
import os

os.environ["UA_PARSER_YAML"] = "./regexes.yaml"

from ua_parser import user_agent_parser

import pygeoip


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

GEOIP = None

def geo_ip(ip):
    global GEOIP
    if GEOIP is None:
        GEOIP = pygeoip.GeoIP('GeoLiteCity.dat')
    return GEOIP.record_by_addr(ip)

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
            geo = geo_ip(m.group(1))
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
                'raw': line,
                'country_name': unescape(geo['country_name']),
                'country_code': geo['country_code'],
                'city': geo['city'],
                'geo': [geo['latitude'], geo['longitude']]
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


