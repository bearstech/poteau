#!/usr/bin/env python

import time

from pyelasticsearch import ElasticSearch

from mbox import Mbox


def documents_from_mails(mails):
    """Build document from mail"""
    for ts, mail in mails:
        if ts[3] < 0: # This bug is audacious
            ts = list(ts)
            ts[3] = 12
            ts = tuple(ts)
        yield {
            '@source': 'stuff://',
            '@type': 'mailadmin',
            '@tags': [mail.headers['From']],
            '@fields': mail.headers,
            '@timestamp': time.strftime("%Y-%m-%dT%H:%M:%S", ts),
            '@message': mail.body,
            'id': mail.headers['Message-Id']
        }

if __name__ == '__main__':
    import sys
    from poteau import Kibana
    # Instantiate it with an url
    es = ElasticSearch(sys.argv[1], timeout=240, max_retries=10)
    k = Kibana(es)
    # Kibana need this kind of name
    emails = Mbox(sys.argv[2])
    for day, size in k.index_documents('email', documents_from_mails(emails)):
        print("[%s] #%i" % (day, size))
