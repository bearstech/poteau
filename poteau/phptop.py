import re

from web import geo_ip, MONTH, unescape
from urlparse import urlparse, parse_qs

PATTERN = re.compile(r"\[(.*?)\] \[(.*?)\] \[(.*?)\] .*? stderr: phptop (.*?) time:(.*?) user:(.*?) sys:(.*?) mem:(\d+)")


def parse_date(txt):
    #2009-11-15T14:12:12
    m = txt.split(" ")
    return "%(year)s-%(month)s-%(day)sT%(time)s" % {
        'year': m[4],
        'month': MONTH.index(m[1]) + 1,
        'day': m[2],
        'time': m[3]
    }


def phpstat(raw):
    for line in raw:
        m = PATTERN.match(line)
        if m is None:
            continue
        date = m.group(1)
        source = m.group(3)
        log = {
            'ip': source.split(' ')[-1],
            'level': m.group(2),
            'time': float(m.group(5)),
            'user': float(m.group(6)),
            'sys': float(m.group(7)),
            'mem': int(m.group(8)),
        }
        url = m.group(4)
        uu = urlparse(url)
        log['domain'] = uu.netloc
        log['path'] = uu.path
        log['query'] = uu.query
        geo = geo_ip(log['ip'])
        if geo is not None:
            log['country_name'] = unescape(geo['country_name']),
            log['country_code'] = geo['country_code'],
            log['city'] = unescape(geo['city']),
            log['geo'] = [geo['latitude'], geo['longitude']]
        #print parse_date(date), time, user, sys, mem, url
        #print geo_ip(ip)
        yield {
            'date': parse_date(date),
            'raw': line,
            'fields': log
        }


def documents_from_phpstat(stats):
    for stat in stats:
        yield {
            '@source': 'stuff://',
            '@type': 'combined',
            '@tags': [],
            '@fields': stat['fields'],
            '@timestamp': stat['date'],
            '@message': stat['raw']
        }


if __name__ == "__main__":
    import sys
    from poteau import Kibana
    from logging import DEBUG, basicConfig
    basicConfig(filename='poteau.log', level=DEBUG)
    from pyelasticsearch import ElasticSearch

    es = ElasticSearch(sys.argv[1], timeout=240, max_retries=10)
    k = Kibana(es)
    k.mapping['@fields']['properties']['path'] = {
        'type': 'string',
        'analyzer': 'path'}
    k.mapping['@fields']['properties']['query'] = {'type': 'string'}
    k.mapping['@fields']['properties']['ip'] = {'type': 'ip'}
    k.mapping['@fields']['properties']['geo'] = {'type': 'geo_point'}
    for day, size in k.index_documents('page',
                                       documents_from_phpstat(phpstat(sys.stdin))):
        print("[%s] #%i" % (day, size))
