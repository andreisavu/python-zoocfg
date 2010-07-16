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

class dotdict(dict):
    """ Extend the standard dict to allow dot syntax """
    def __getattr__(self, name):
        return self[name]

class ZooCfg(dotdict):

    _defaults = dotdict({
        'globalOutstandingLimit': 1000,
        'preAllocSize': '64M',
        'snapCount': 100000,
        'maxClientCnxns': 10,
        'minSessionTimeout': 2,
        'maxSessionTimeout': 20,
        'electionAlg': 3,
        'leaderServers': 'yes'
    })

    @classmethod
    def from_file(cls, file_name):
        return cls(open(file_name).read())
    
    def __init__(self, content=''):
        super(ZooCfg, self).__init__()

        self.update(self._defaults)
        self.update(self._parse(content))
        if 'dataLogDir' not in self and 'dataDir' in self:
            self['dataLogDir'] = self['dataDir']

        self._warnings, self._errors = Rules.check_all(self)

    def _parse(self, content):
        h = StringIO(content)
        result = {}
        for line in h.readlines():

            try:
                line = line[:line.index('#')]
            except ValueError:
                pass

            if not line.strip():
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

    def has_errors(self):
        return bool(self._errors)

    def has_warnings(self):
        return bool(self._warnings)

    @property
    def errors(self): 
        return tuple(self._errors)

    @property
    def warnings(self): 
        return tuple(self._warnings)


class Rules(object):
    """ ZooKeeper config validation rules """

    @classmethod
    def check_all(cls, cfg):
        """ Check all configuration rules """
        warnings, errors = [], []

        for name, ref in Rules.__dict__.items():
            try:
                if hasattr(ref, 'mro') and Rules.BaseRule in ref.mro() and ref != Rules.BaseRule:
                    w, e = ref.check(cfg)

                    warnings.extend(w)
                    errors.extend(e)
            except Exception, e:
                errors.append('`%s` rule check failed: %s' % (name, e))

        return warnings, errors

    class BaseRule(object):
        """ Inherit from this class when defining a new validation rule """
        @classmethod
        def check(cls, cfg):
            pass

    class ClientPort(BaseRule):
        """ A valid TCP/IP port >1024 """

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'clientPort' not in cfg:
                errors.append('No `clientPort` found in config file.')

            elif not isinstance(cfg.clientPort, int) or cfg.clientPort < 0 or cfg.clientPort > 65535:
                errors.append('`clientPort` should be a valid TCP/IP port number.')

            return warnings, errors

    class TickTime(BaseRule):
        """ The length of a single tick, which is the basic time unit used by ZooKeeper, measured in milliseconds"""

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'tickTime' not in cfg:
                errors.append('No `tickTime` found in config file.')

            elif not isinstance(cfg.tickTime, int) or cfg.tickTime <= 0:
                errors.append('`tickTime` should be a positive number measured in milliseconds')

            return warnings, errors


    class AbsoluteDataDir(BaseRule):
        """ The dataDir should be absolute because ZooKeeper runs as a daemon """

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'dataDir' not in cfg:
                errors.append('No `dataDir` found in config file.')

            elif cfg.dataDir[0] != '/':
                warnings.append('`dataDir` contains a relative path. '\
                    'This could be a problem if ZooKeeper is running as daemon.')

            return warnings, errors

    class DataLogDirOnAnotherPartition(BaseRule):
        """ Warn that dataLogDir should be on another partition """

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'dataLogDir' not in cfg:
                errors.append('No `dataLogDir` found in config file.')

            elif cfg.dataLogDir == cfg.dataDir:
                warnings.append('The `dataLogDir` should not use the same partition as `dataDir` '\
                    'in order to avoid competition between logging and snapshots. Having a '\
                    'dedicated log device has a large impact on throughput and stable latencies.')

            return warnings, errors            

    class ElectionAlgorithm(BaseRule):
        """ Check the selected election algorithm """

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'electionAlg' not in cfg:
                errors.append('No `electionAlg` found in config file.')

            elif cfg.electionAlg not in (0, 1, 2, 3):
                errors.append('Unknown `electionAlg`. Valid values: 0, 1, 2, 3.')

            elif cfg.electionAlg in (1, 2):
                warnings.append('Election algorithm implementation 1 and 2 are no longer supported.')

            return warnings, errors

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
        return -1

    cfg = ZooCfg.from_file(opts.filename)

    ret = 0
    if cfg.has_warnings() and opts.warnings is True:
        print 'Warnings:'
        for warning in cfg.warnings: print '* %s\n' % warning
        ret = 1

    if cfg.has_errors():
        print 'Errors:'
        for error in cfg.errors: print '* %s\n' % error
        ret = 2

    return ret

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

