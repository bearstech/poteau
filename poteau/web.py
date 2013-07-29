#!/usr/bin/env python

# TODO domain referer
# TODO search engine query from referer
import re
import os
from urlparse import urlparse, parse_qs

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
    try:
        return int(a)
    except Exception as e:
        return 0


def unescape(txt):
    if txt is None:
        return None
    return "_".join(txt.split(" "))


def combined(reader, user_agent=True, geo_ip=True):
    for line in reader:
        m = RE_COMBINED.match(line)
        if m is not None:
            log = {
                'ip': m.group(1),
                'user': m.group(2),
                'date': parse_date(m.group(3)),
                'command': m.group(4),
                'url': m.group(5),
                'http': m.group(6),
                'code': int(m.group(7)),
                'size': intOrZero(m.group(8)),
                'referer': m.group(9),
                'raw': line,
            }
            if user_agent:
                ua = user_agent_parser.Parse(m.group(10))
                ua['os']['family'] = unescape(ua['os']['family'])
                ua['device']['family'] = unescape(ua['device']['family'])
                log['user-agent'] = ua
            else:
                log['user-agent'] = [ {'string': m.group(10)} ]
            if geo_ip:
                geo = geo_ip(m.group(1))
                log['country_name'] = unescape(geo['country_name']),
                log['country_code'] = geo['country_code'],
                log['city'] = geo['city'],
                log['geo'] = [geo['latitude'], geo['longitude']]
            ref = urlparse(m.group(9))
            log['referer_domain'] = ref.netloc
            query = None
            if ref.query:
                qq = parse_qs(ref.query)
                if 'q' in qq:
                    query = qq['q'][0]
            log['query'] = query
            yield log


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


def sessions(logs):
    sessions = {}
    for log in logs:
        ip = "%s %s" % (log['ip'], log['user-agent'][0]['string'])
        if ip not in sessions:
            sessions[ip] = 0
        sessions[ip] += 1
    sessions = sessions.items()
    sessions.sort(lambda a, b: cmp(a[1], b[1]))
    return sessions

if __name__ == "__main__":
    import sys
    with open(sys.argv[1], 'r') as f:
        sessions = sessions(combined(f, user_agent=False, geo_ip=False))
        print sessions[:-2]
