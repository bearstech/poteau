#!/usr/bin/env python

import re
import os

os.environ["UA_PARSER_YAML"] = "./regexes.yaml"

from ua_parser import user_agent_parser


RE_COMMON = re.compile(r'(.*?) - (.*?) \[(.*?) [+-]\d{4}\] "(.*?) (.*?) (.*?)" (\d{3}) (\d+)', re.U)
RE_COMBINED = re.compile('(.*?) - (.*?) \[(.*?) [+-]\d{4}\] "(.*?) (.*?) (.*?)" (\d{3}) (.*?) "([^"]*)" "([^"]*)"', re.U)


def parse_date(src):
    pass


def intOrZero(a):
    if a == '-':
        return 0
    return int(a)


def combined(reader):
    for line in reader:
        m = RE_COMBINED.match(line)
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
                'user-agent': user_agent_parser.Parse(m.group(10))
            }


if __name__ == "__main__":
    import sys
    for line in combined(sys.stdin):
        print line
