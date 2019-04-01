import unittest


from lcatools.archives import BasicArchive, Qdb
from .. import IPCC_2007_GWP
from lcatools.entities.tests.base_testclass import BasicEntityTest
from lcatools.exchanges import ExchangeValue

mass_uuid = '93a60a56-a3c8-11da-a746-0800200b9a66'


class QdbTestCase(BasicEntityTest):
    """
    lots to test here
    """
    @classmethod
    def setUpClass(cls):
        super(QdbTestCase, cls).setUpClass()
        cls._qdb = Qdb()
        cls._I = BasicArchive.from_file(IPCC_2007_GWP)

    def setUp(self):
        self.qi = self._qdb.make_interface('quantity')

    def test_mass(self):
        self.assertEqual(self.qi.get_canonical('mass').uuid, mass_uuid)

    def test_canonical(self):
        m = self.qi.get_canonical('mass')
        self.assertEqual(m['Name'], 'Mass')

    def test_kwh_mj_conversion(self):
        elec = next(self._qdb.search('flow', Name='electricity'))
        ncv = self.qi.get_canonical('net calorific value')
        self.assertEqual(elec.cf(ncv), 3.6)

    def _exch_gen(self):
        p = object()
        p.origin = 'test.null'
        p.uuid = '1234567'
        yield ExchangeValue(p, 'carbon dioxide', 'Output', 34.7)
        yield ExchangeValue(p, 'methane', 'Output', 16)

    def test_lcia(self):
        gwp = self._I['Global Warming Air']
        res = gwp.do_lcia(self._exch_gen())
        self.assertEqual(res.total(), 34.7 + 16*25)


if __name__ == '__main__':
    unittest.main()
