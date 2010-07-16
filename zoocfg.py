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
        'preAllocSize': 65536, # 64M in kilobytes
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

class RulesResult(object):
    """ A result obtained by checking all the config rules """

    def __init__(self, warnings, errors):
        self.warnings = tuple(warnings)
        self.errors = tuple(errors)

    def has_errors(self):
        return bool(self.errors)

    def has_warnings(self):
        return bool(self.warnings)

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

        return RulesResult(warnings, errors)

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

            elif cfg.clientPort < 1024:
                warnings.append('`clientPort` < 1024. You should not run ZooKeeper using the root account')

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


    class DataDir(BaseRule):
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

    class DataLogDir(BaseRule):
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

    class GlobalOutstandingLimit(BaseRule):
        
        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'globalOutstandingLimit' not in cfg:
                errors.append('No `globalOutstandingLimit` found in config file.')

            elif not isinstance(cfg.globalOutstandingLimit, int) or cfg.globalOutstandingLimit < 0:
                errors.append('`globalOutstandingLimit` should be a positive integer')

            return warnings, errors

    class PreAllocSize(BaseRule):
        """ Transaction log block prealloc size """

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'preAllocSize' not in cfg:
                errors.append('No `preAllocSize` found in config file.')

            elif not isinstance(cfg.preAllocSize, int) or cfg.preAllocSize < 0:
                errors.append('`preAllocSize` should be a positive number of kilobytes.')

            return warnings, errors

    class SnapCount(BaseRule):
        """ The number of transaction processed before a snapshot is generated """

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'snapCount' not in cfg:
                errors.append('No `snapCount` found in config file.')

            elif not isinstance(cfg.snapCount, int) or cfg.snapCount < 0:
                errors.append('`snapCount` should be a positive integer.')

            elif cfg.snapCount < 5000:
                warnings.append('Settings `snapCount` to low may hurt server performance.')

            return warnings, errors

    class TraceFile(BaseRule):
        """ Enable the tracefile. Useful for debugging but this will impact performance. """

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'traceFile' in cfg:
                warnings.append('Enabling the tracefile will impact system performance')

            return warnings, errors

    class MaxClientCnxns(BaseRule):
        """ Limit the total number of concurrent connections handle by a member of the ensemble """

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'maxClientCnxns' not in cfg:
                errors.append('`No `maxClientCnxns` found in config file.')

            elif not isinstance(cfg.maxClientCnxns, int) or cfg.maxClientCnxns < 0:
                errors.append('`maxClientCnxns` should be a positive integer or 0.')

            elif cfg.maxClientCnxns == 0:
                warnings.append('`maxClientCnxns` is set to 0. The server is vulnerable to DOS attacks.')

            return warnings, errors

    class SessionTimeout(BaseRule):

        @classmethod
        def check(cls, cfg):
            warnings, errors = [], []

            if 'minSessionTimeout' not in cfg:
                errors.append('No `minSessionTimeout` found in config file.')

            elif not isinstance(cfg.minSessionTimeout, int) or cfg.minSessionTimeout < 0:
                errors.append('`minSessionTimeout` should be a positive integer.')

            if 'maxSessionTimeout' not in cfg:
                errors.append('No `maxSessionTimeout` found in config file.')

            elif not isinstance(cfg.maxSessionTimeout, int) or cfg.maxSessionTimeout < 0:
                errors.append('`maxSessionTimeout` should be a positive integer.')

            if not errors:
                if cfg.minSessionTimeout > cfg.maxSessionTimeout:
                    errors.append('`minSessionTimeout` > `maxSessionTimeout`')

            return warnings, errors

    class ElectionAlg(BaseRule):
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
    check = Rules.check_all(cfg)

    ret = 0
    if check.has_warnings() and opts.warnings is True:
        print 'Warnings:'
        for warning in check.warnings: print '* %s\n' % warning
        ret = 1

    if check.has_errors():
        print 'Errors:'
        for error in check.errors: print '* %s\n' % error
        ret = 2

    return ret

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

