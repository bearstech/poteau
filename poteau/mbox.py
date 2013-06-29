#!/usr/bin/env python

"""My Thunderbird mail crash the email.mbox library.
Here is a violent way to read mbox format : yielding mail as text and
parsing them with lamson.
"""

import mmap
from email.utils import parsedate
# Lamson is an application, but also the best way to read email without
# struggling with "battery include" libraries.
from lamson.encoding import from_string as parse_mail


class Mbox(object):
    def __init__(self, path, sort=True):
        f = open(path, 'r+b')
        self.mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        self._index = None
        if sort:
            self.sort()

    def dates(self):
        self.mm.seek(0)
        state = None
        start = None
        end = None
        date = None
        for line in iter(self.mm.readline, ""):
            if line.startswith('From '):
                state = 'header'
                end = self.mm.tell() - len(line)
                if date is not None:
                    yield (start, end), date
                start = end
                date = None
            if state == 'header':
                if line == "\n":
                    state = 'body'
                elif line.startswith('Date: '):
                    date = parsedate(line[6:-1])
        end = self.mm.tell()
        yield (start, end), date

    def sort(self):
        self._index = list(self.dates())
        self._index.sort(lambda x, y: cmp(x[1], y[1]))

    def __iter__(self):
        if self._index is not None:
            mails = self._index
        else:
            mails = self.dates()
        for (start, end), ts in mails:
            self.mm.seek(start)
            yield ts, parse_mail(self.mm.read(end - start))


if __name__ == '__main__':
    import sys
    m = Mbox(sys.argv[1])
    for ts, mail in m:
        print ts, mail['Subject']
