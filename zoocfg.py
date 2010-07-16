#! /usr/bin/env python
#
#  Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from StringIO import StringIO
from optparse import OptionParser

class ZooCfg(object):

    _defaults = {
        'globalOutstandingLimit': 1000,
        'preAllocSize': '64M',
        'snapCount': 100000,
        'maxClientCnxns': 10,
        'minSessionTimeout': 2,
        'maxSessionTimeout': 20,
        'electionAlg': 3,
        'leaderServers': 'yes'
    }

    @classmethod
    def from_file(cls, file_name):
        return cls(open(file_name).read())
    
    def __init__(self, content=''):
        self._data = dict(self._defaults)
        self._data.update(self._parse(content))

        self._errors = []
        self._warnings = []

    def _parse(self, content):
        h = StringIO(content)
        result = {}
        for line in h.readlines():
            if not line.strip() or line[0] == '#':
                continue
            result.update(self._parse_line(line))
        return result

    def _parse_line(self, line):
        try:
            key, value = map(str.strip, line.split('='))

            try:
                value = int(value)
            except (TypeError, ValueError):
                pass

            return {key: value}
        except ValueError:
            return {} # just skip broken line

    def __getattr__(self, name):
        return self._data[name]

    def has_errors(self):
        return bool(self._errors)

    def has_warnings(self):
        return bool(self._warnings)

    @property
    def errors(self): return tuple(self._errors)

    @property
    def warnings(self): return tuple(self._warnings)


def main(argv):
    parser = OptionParser()

    parser.add_option('-f', '--file', dest='filename', 
        help="ZooKeeper config FILE", metavar="FILE")

    parser.add_option('-w', '--warnings', dest='warnings',
        default=False, action='store_true', 
        help='show warnings. defaults to false')

    (opts, args) = parser.parse_args(argv)

    if opts.filename is None:
        print >>sys.stderr, "Config file name is mandatory."

        parser.print_help()
        return 1

    cfg = ZooCfg.from_file(opts.filename)
    if cfg.has_errors():
        # print errors
        pass

    if cfg.has_warnings() and opts.warnings is True:
        # print warnings
        pass

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

