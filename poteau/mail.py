#!/usr/bin/env python


# Lamson is an application, but also the best way to read email without
# struggling with "battery include" libraries.
from lamson.encoding import from_string as parse_mail

from pyelasticsearch import ElasticSearch


def parse_date(txt):
    """Mails use this format :
Fri, 10 Feb 2012 08:48:52 +0100 (CET)
Elastic Search need this one :
2009-11-15T14:12:12
Just use a naive translation.
    """
    MONTH = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    parts = txt.encode('ASCII').split(',', 2)[-1].strip().split(' ')
    resp = '%(year)s-%(month)02d-%(day)02dT%(hms)s' % {
        'year': parts[2],
        'month': MONTH.index(parts[1]) + 1,
        'day': int(parts[0]),
        'hms': parts[3]}
    return resp


def mbox(path):
    """My Thunderbird mail crash the email.mbox library.
    Here is a violent way to read mbox format : yielding mail as text and
    parsing them with lamson.
    """
    buff = None
    with open(path, 'r') as box:
        for line in box:
            if line.startswith('From '):
                if buff is None:
                    buff = []
                else:
                    yield parse_mail("".join(buff))
                    buff = []
            buff.append(line)
        yield parse_mail("".join(buff))


def documents_from_mails(mails):
    """Build document from mail"""
    for mail in mails:
        if 'Date' in mail.headers:  # Some mails seem broken.
            yield {
                '@source': 'stuff://',
                '@type': 'mailadmin',
                '@tags': [mail.headers['From']],
                '@fields': mail.headers,
                '@timestamp': parse_date(mail.headers['Date']),
                '@message': mail.body,
                'id': mail.headers['Message-Id']
            }

if __name__ == '__main__':
    import sys
    from __init__ import Kibana
    # Instantiate it with an url
    es = ElasticSearch(sys.argv[1], timeout=120, max_retries=10)
    k = Kibana(es)
    # Kibana need this kind of name
    emails = mbox(sys.argv[2])
    for n in k.index_documents('email', documents_from_mails(emails)):
        print(n)
