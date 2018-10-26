"""
For fragment tests, we are using the CalRecycle model to provide test cases
"""

import unittest
from math import floor

from ... import LcCatalog
from ...data_sources.local import make_config, check_enabled, CATALOG_ROOT

if check_enabled('calrecycle'):
    cat = LcCatalog(CATALOG_ROOT)
    crc = make_config('calrecycle')
    fg = crc.foreground(cat)


@unittest.skipUnless(check_enabled('calrecycle'), 'CalRecycle not enabled')
class FragmentTests(unittest.TestCase):

    def test_autoconsumption(self):
        pass  # no autoconsumption fragments in CalRecycle :(


if __name__ == '__main__':
    unittest.main()
