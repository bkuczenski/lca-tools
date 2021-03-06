import unittest

from ... import LcCatalog
from ..local import CATALOG_ROOT, make_config, check_enabled
from lcatools.interfaces import UnknownOrigin

etypes = ('quantity', 'flow', 'process')

_debug = True

if __name__ == '__main__':
    _run_uslci = check_enabled('uslci')
else:
    _run_uslci = check_enabled('uslci') or _debug


if _run_uslci:
    cat = LcCatalog(CATALOG_ROOT)
    cfg = make_config('uslci')
    try:
        gwp = next(cat.query('lcia.ipcc').lcia_methods())
    except UnknownOrigin:
        gwp = False


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
        _test_case_lcia = 0.0

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

        def test_20_inventory(self):
            p = next(self.query.processes(Name='^Petroleum refining, at'))
            rx = [x for x in p.references()]
            inv = [x for x in p.inventory()]
            self.assertEqual(len(rx), 9)
            self.assertEqual(len(inv), 51)

        def _get_fg_test_case_rx(self):
            p = next(self.query.processes(Name='Seedlings, at greenhouse, US PNW'))
            return p.reference()

        def _get_fg_test_case_lci(self):
            rx = self._get_fg_test_case_rx()
            return [x for x in self.query.lci(rx.process.external_ref, rx.flow.external_ref)]

        def test_21_exchange_relation(self):
            rx = self._get_fg_test_case_rx()
            k = next(self.query.flows(Name='CUTOFF Potassium fertilizer, production mix, at plant'))
            v = self.query.exchange_relation(rx.process.external_ref, rx.flow.external_ref, k.external_ref, 'Input')
            self.assertEqual(v, 0.000175)

        def test_30_bg_gen(self):
            self.assertTrue(self.query.check_bg())

        def test_31_bg_length(self):
            self.assertEqual(len([k for k in self.query.background_flows()]), self._bg_len)

        def test_32_lci_fg(self):
            lci = self._get_fg_test_case_lci()
            self.assertEqual(len(lci), 298 - self._bg_len)  # this works because the bg discrepancy shows up as cutoffs
            lead_vals = {1.5e-09, 2.3e-09, 0.0}
            self.assertSetEqual({round(x.value, 10) for x in lci if x.flow.name.startswith('Lead')}, lead_vals)

        def test_40_lcia_fg(self):
            if gwp:
                lci = self._get_fg_test_case_lci()
                res = gwp.do_lcia(lci)
                self.assertAlmostEqual(res.total(), self._test_case_lcia)


class UsLciEcospoldTest(UsLciTestContainer.UsLciTestBase):

    _atype = 'ecospold'
    _initial_count = (5, 97, 5)
    _bg_len = 38
    _test_case_lcia = 0.0415466  # more robust bc of ocean freight??

    def test_get_by_id(self):
        f = cat.query(self.reference).get(2176)  # this flow was loaded via the config mechanism
        self.assertGreaterEqual(len(f.profile()), 2)


class UsLciOlcaTest(UsLciTestContainer.UsLciTestBase):

    _atype = 'olca'
    _initial_count = (4, 71, 3)
    _bg_len = 36
    _test_case_lcia = .04110577



if __name__ == '__main__':
    unittest.main()
