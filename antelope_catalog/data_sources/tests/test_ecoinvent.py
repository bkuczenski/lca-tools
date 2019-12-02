import unittest
from collections import namedtuple

from ... import LcCatalog
from ..local import CATALOG_ROOT, make_config, check_enabled

RefStats = namedtuple('RefStats', ('proc', 'elem', 'flowables'))

test_refs = {'local.ecoinvent.3.4.apos': RefStats(13290, 4078, 1184)}

_debug = True

if __name__ == '__main__':
    _run_ecoinvent = check_enabled('ecoinvent')
else:
    _run_ecoinvent = check_enabled('ecoinvent') or _debug

if _run_ecoinvent:
    cat = LcCatalog(CATALOG_ROOT)
    cfg = make_config('ecoinvent')


class EcoinventDataSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ar = []
        for ref in test_refs.keys():
            res = next(cfg.make_resources(ref))
            if ref not in cat.references:
                cat.add_resource(res)
            res.check(cat)
            res.archive.load_flows()
            ar.append(res.archive)
        cls.ea = tuple(ar)

    def test_nproc(self):
        for ar in self.ea:
            stats = test_refs[ar.ref]
            self.assertEqual(ar.count_by_type('process'), stats.proc, ar.ref)

    def test_nelem(self):
        for ar in self.ea:
            stats = test_refs[ar.ref]
            self.assertEqual(ar.count_by_type('flow'), stats.elem, ar.ref)

    def test_flowables(self):
        for ar in self.ea:
            stats = test_refs[ar.ref]
            self.assertEqual(len([k for k in ar.tm.flowables()]), stats.flowables, ar.ref)


if __name__ == '__main__':
    unittest.main()
