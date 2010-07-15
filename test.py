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

from zoocfg import ZooCfg

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

if __name__ == '__main__':
    unittest.main()

