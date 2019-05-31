"""
This file deals with tests for auto-loading and masquerading of LCIA methods
"""
import unittest

from .. import LcCatalog
from ...data_sources.local import CATALOG_ROOT, make_config


cat = LcCatalog(CATALOG_ROOT)
cfg = make_config('ipcc2007')
ref = next(cfg.references)


def setUpModule():
    if ref not in cat.references:
        cat.add_resource(next(cfg.make_resources(ref)))


class QuantityRefTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gwp = next(cat.query(ref).lcia_methods(Name='Global Warming'))  # canonical
        cls.gwp_ref = cat._qdb[cls.gwp.external_ref]  # original ref
        cls.gwp_true = cat.get_archive(cls.gwp_ref.origin).get(cls.gwp_ref.external_ref)  # authentic entity

    def test_origins(self):
        self.assertEqual(self.gwp.origin, ref)
        self.assertEqual(self.gwp_ref.origin, ref)
        res = cat.get_resource(ref)
        self.assertEqual(self.gwp_true.origin, res.archive.names[res.source])

    def test_factors(self):
        self.assertEqual(len([k for k in self.gwp.factors()]), 91)
        self.assertEqual(len([k for k in self.gwp_ref.factors()]), 91)
        self.assertEqual(len([k for k in self.gwp_true.factors()]), 91)

    def test_properties(self):
        self.assertTrue(self.gwp.has_property('indicator'))
        self.assertTrue(self.gwp_ref.has_property('indicator'))
        self.assertTrue(self.gwp_true.has_property('indicator'))

