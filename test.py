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

import unittest
import sys
import os
from StringIO import StringIO

import zoocfg
from zoocfg import ZooCfg, dotdict

def abspath(*args):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(current_dir, *args)

class TestZooCfg(unittest.TestCase):
    
    def test_parse_two_lines(self):
        cfg = ZooCfg('a=23\nb=asd')

        assert cfg.a == 23
        assert cfg.b == 'asd'

    def test_skip_broken_lines(self):
        cfg = ZooCfg('broken-line\na=3')

        assert cfg.a == 3

    def test_default_values(self):
        cfg = ZooCfg()

        assert cfg.snapCount == 100000
        assert cfg.electionAlg == 3

    def test_overwrite_default_value(self):
        cfg = ZooCfg('electionAlg=4')

        assert cfg.electionAlg == 4

    def test_load_from_file(self):
        cfg = ZooCfg.from_file(abspath('samples/standalone-zoo.cfg'))

        assert cfg.dataDir == '/var/zookeeper/data/'

    def test_ignore_comments(self):
        cfg = ZooCfg('#a=5\nb=6')

        assert hasattr(cfg, '#a') is False
        assert cfg.b == 6

    def test_ignore_comments_at_the_end_of_the_line(self):
        cfg = ZooCfg('a=5 # ignored\nb=6')

        assert cfg.a == 5 and cfg.b == 6

class CapturingTestCase(unittest.TestCase):

    def _in_memory_buffer(self, stream, *args):
        if args:
            map(self._in_memory_buffer, args)
        try: 
            getattr(sys, "_%s" % stream)
        except: 
            setattr(sys, "_%s" % stream, getattr(sys, stream))
        setattr(sys, stream, StringIO())

    def _restore(self, stream, *args):
        if args:
            map(self._restore, args)
        setattr(sys, stream, getattr(sys, '_%s' % stream))

    def setUp(self):
        self._in_memory_buffer('stdout', 'stderr')

    def tearDown(self):
        self._restore('stdout', 'stderr')

    def stdout(self):
        sys.stdout.seek(0)
        return sys.stdout.read()

    def stderr(self):
        sys.stderr.seek(0)
        return sys.stderr.read()

class TestZooCfg_CommandLine_Interface(CapturingTestCase):

    def test_file_param_is_mandatory(self):
        r = zoocfg.main([])
        assert r == -1 
        assert self.stderr() == 'Config file name is mandatory.\n'

class TestRules(unittest.TestCase):

    def test_absolute_dataDir(self):
        w, e = zoocfg.Rules.AbsoluteDataDir.check(
            dotdict(dataDir='./relative-path'))

        assert len(w) == 1 and len(e) == 0

if __name__ == '__main__':
    unittest.main()

