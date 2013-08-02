#!/usr/bin/env python

# TODO domain referer
# TODO search engine query from referer
import re
import os
import time
from urlparse import urlparse, parse_qs

os.environ["UA_PARSER_YAML"] = "./regexes.yaml"

from ua_parser import user_agent_parser

import pygeoip


RE_COMMON = re.compile(r'(.*?) - (.*?) \[(.*?) [+-]\d{4}\] "(.*?) (.*?) (.*?)" (\d{3}) (\d+)', re.U)
RE_COMBINED = re.compile(r'(.*?) - (.*?) \[(.*?) [+-]\d{4}\] "(.*?) (.*?) (.*?)" (\d{3}) (.*?) "([^"]*)" "([^"]*)"', re.U)
RE_DATE = re.compile(r'(\d+)/(\w{3})/(\d+):(\d+:\d+:\d+)', re.U)

MONTH = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
         'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def parse_date(src):
    """
10/Oct/2000:13:55:36
2009-11-15T14:12:12
    """
    m = RE_DATE.match(src)
    resp = '%(year)s-%(month)02d-%(day)02dT%(hms)s' % {
        'year': m.group(3),
        'month': MONTH.index(m.group(2)) + 1,
        'day': int(m.group(1)),
        'hms': m.group(4)
    }
    return resp


def parse_time(src):
    return int(time.mktime(time.strptime(src, "%d/%b/%Y:%H:%M:%S")))

GEOIP = None


def geo_ip(ip):
    global GEOIP
    if GEOIP is None:
        GEOIP = pygeoip.GeoIP('GeoLiteCity.dat')
    return GEOIP.record_by_addr(ip)


def intOrZero(a):
    try:
        return int(a)
    except Exception:
        return 0


def unescape(txt):
    if txt is None:
        return None
    return "_".join(txt.split(" "))


def combined(reader, user_agent=True, geo_ip=True, date=parse_date):
    for line in reader:
        m = RE_COMBINED.match(line)
        if m is not None:
            log = {
                'ip': m.group(1),
                'user': m.group(2),
                'date': date(m.group(3)),
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
                log['user-agent'] = [{'string': m.group(10)}]
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


def ts_to_date(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts))


class Session(object):
    def __init__(self, key):
        self.ip = key
        self.ts = {}
        self._sum = 0
        self.last = None

    def append(self, log):
        d = log['date']
        self.last = d
        if d not in self.ts:
            self.ts[d] = 0
        self.ts[d] += 1
        self._sum += 1

    def filter(self, minima=2):
        for k, v in self.ts.items():
            if v < minima:
                self._sum -= self.ts[k]
                del self.ts[k]

    def to_es(self, source='localhost'):
        d = self.ts.keys()
        d.sort()
        #2009-11-1'T14:12:12
        first = ts_to_date(d[0])
        age = d[-1] - d[0]
        ip, user_agent = self.ip
        log = {'sum': self.sum(),
               'max': self.max(),
               'size': len(self),
               'med': self.median(),
               'age': age,
               'ip': ip,
               }
        geo = geo_ip(ip)
        if geo:
            log['country_name'] = unescape(geo['country_name']),
            log['country_code'] = geo['country_code'],
            log['city'] = unescape(geo['city']),
            log['geo'] = [geo['latitude'], geo['longitude']]
        ua = user_agent_parser.Parse(user_agent)
        ua['os']['family'] = unescape(ua['os']['family'])
        ua['device']['family'] = unescape(ua['device']['family'])
        log['user-agent'] = ua
        return {'@type': 'session',
                '@timestamp': first,
                '@message': '',
                '@source': source,
                '@fields': log
                }

    def sum(self):
        return self._sum

    def median(self):
        v = self.ts.values()
        v.sort()
        return v[int(len(v) / 2)]

    def max(self):
        return max(self.ts.values())

    def __cmp__(self, other):
        return cmp(self._sum, other._sum)

    def __repr__(self):
        return "<Session %s>" % self.ts

    def __len__(self):
        return len(self.ts)


class Sessions(object):
    def __init__(self, time_delta):
        self.sessions = {}


def filter_session(s, minima):
    for k, v in s:
        v.filter(minima)
        if len(v.ts):
            yield k, v


def sessions(logs, max_age=900):
    sessions = {}
    for log in logs:
        key = (log['ip'], log['user-agent'][0]['string'])
        if key not in sessions:
            sessions[key] = Session(key)
        elif (log['date'] - sessions[key].last) > max_age:
            yield sessions[key]
            sessions[key] = Session(key)
        sessions[key].append(log)
    for session in sessions.values():
        yield session


def asset_filter(logs):
    for log in logs:
        if log['command'] != 'GET':
            yield log
            continue
        u = urlparse(log['url'])
        uu = u.path.split('.')
        if len(uu) == 1:
            yield log
            continue
        if uu[-1] not in ['css', 'js', 'ico', 'woff', 'png', 'gif', 'jpg']:
            yield log


def documents_from_session(sessions):
    for session in sessions:
        yield session.to_es()


if __name__ == "__main__":
    import sys
    from poteau import Kibana
    from pyelasticsearch import ElasticSearch

    idx = len(sys.argv) > 2
    if idx:
        es = ElasticSearch(sys.argv[2], timeout=240, max_retries=10)
        k = Kibana(es)
    with open(sys.argv[1], 'r') as f:
        s = sessions(asset_filter(combined(f, user_agent=False, geo_ip=False,
                              date=parse_time)))
        if idx:
            for day, size in k.index_documents('session', documents_from_session(s)):
                print("[%s] #%i" % (day, size))
        else:
            for d in documents_from_session(s):
                print d
