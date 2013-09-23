import re

from web import geo_ip, MONTH

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

if __name__ == "__main__":
    import sys
    for line in sys.stdin:
        m = PATTERN.match(line)
        if m is None:
            continue
        date = m.group(1)
        level = m.group(2)
        source = m.group(3)
        ip = source.split(' ')[-1]
        url = m.group(4)
        time = float(m.group(5))
        user = float(m.group(6))
        sys = m.group(7)
        mem = m.group(8)
        print parse_date(date), time, user, sys, mem, url
        print geo_ip(ip)
