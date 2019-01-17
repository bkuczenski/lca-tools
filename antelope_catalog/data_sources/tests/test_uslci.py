import unittest
from collections import namedtuple
import os

from ... import LcCatalog
from ..local import CATALOG_ROOT, make_config, check_enabled

etypes = ('quantity', 'flow', 'process')

_debug = True

if __name__ == '__main__':
    _run_uslci = check_enabled('uslci')
else:
    _run_uslci = check_enabled('uslci') or _debug


if _run_uslci:
    cat = LcCatalog(CATALOG_ROOT)
    cfg = make_config('uslci')


class UsLciTestContainer(object):

    class UsLciTestBase(unittest.TestCase):
        """
        Base class Mixin contains tests common to both ecospold and olca implemenations.  We hide it inside a container
        so that it does not get run automatically by unittest.main().  Then we can add implementation-specific tests in
        the subclasses.

        Thanks SO! https://stackoverflow.com/questions/1323455

        """
        _atype = None
        _initial_count = (0, 0, 0)
        _bg_len = None

        @property
        def reference(self):
            return '.'.join([cfg.prefix, self._atype])

        @property
        def inx_reference(self):
            return '.'.join([self.reference, 'index'])

        @property
        def query(self):
            return cat.query(self.reference)

        def test_00_resources_exist(self):
            self.assertIn(self.reference, cat.references)

        def test_01_initial_count(self):
            ar = cat.get_archive(self.reference, strict=True)
            for i, k in enumerate(etypes):
                self.assertEqual(ar.count_by_type(k), self._initial_count[i])

        def test_10_index(self):
            inx_ref = cat.index_ref(self.reference)
            self.assertTrue(inx_ref.startswith(self.inx_reference))
            self.assertIn(inx_ref, cat.references)

        def test_20_bg_gen(self):
            self.assertTrue(self.query.check_bg())

        def test_21_bg_length(self):
            self.assertEqual(len([k for k in self.query.background_flows()]), self._bg_len)


class UsLciEcospoldTest(UsLciTestContainer.UsLciTestBase):

    _atype = 'ecospold'
    _initial_count = (5, 97, 5)
    _bg_len = 38

    def test_get_by_id(self):
        f = cat.query(self.reference).get(2176)  # this flow was loaded via the config mechanism
        self.assertGreaterEqual(len(f.profile()), 2)


class UsLciOlcaTest(UsLciTestContainer.UsLciTestBase):

    _atype = 'olca'
    _initial_count = (4, 71, 3)
    _bg_len = 36



if __name__ == '__main__':
    unittest.main()
