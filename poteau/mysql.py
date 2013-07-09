from pyelasticsearch import ElasticSearch
from myslow import MySlow
import time


def documents(lines):
    for ts, header, command in lines:
        yield {
            '@type': 'myslow',
            '@timestamp': time.strftime("%Y-%m-%dT%H:%M:%S", ts.timetuple()),
            '@message': command,
            '@fields': header
        }


if __name__ == '__main__':
    import sys
    from poteau import Kibana
    # Instantiate it with an url
    es = ElasticSearch(sys.argv[1], timeout=240, max_retries=10)
    k = Kibana(es)
    for day, size in k.index_documents('myslow', documents(MySlow(sys.stdin))):
        print("[%s] #%i" % (day, size))
