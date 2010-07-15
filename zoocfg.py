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

from StringIO import StringIO

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
    
    def __init__(self, content=''):
        self._data = self._parse(content)
        self._set_defaults()


    def _set_defaults(self):
        for k, v in self._defaults.iteritems():
            if k not in self._data:
                self._data[k] = v

    def _parse(self, content):
        h = StringIO(content)
        result = {}
        for line in h.readlines():
            if not line.strip(): continue
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


