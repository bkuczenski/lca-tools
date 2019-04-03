import unittest


from lcatools.archives import Qdb
from lcatools.entities.tests.base_testclass import BasicEntityTest

mass_uuid = '93a60a56-a3c8-11da-a746-0800200b9a66'


class QdbTestCase(BasicEntityTest):
    """
    lots to test here
    """
    @classmethod
    def setUpClass(cls):
        super(QdbTestCase, cls).setUpClass()
        cls._qdb = Qdb()

    def setUp(self):
        self.qi = self._qdb.make_interface('quantity')

    def test_qdb(self):
        self.assertEqual(self._qdb.count_by_type('quantity'), 26)

    def test_mass(self):
        self.assertEqual(self.qi.get_canonical('mass').uuid, mass_uuid)

    def test_canonical(self):
        m = self.qi.get_canonical('mass')
        self.assertEqual(m['Name'], 'Mass')

    def test_kwh_mj_conversion(self):
        elec = next(self._qdb.search('flow', Name='electricity'))
        ncv = self.qi.get_canonical('net calorific value')
        self.assertEqual(elec.cf(ncv), 3.6)


if __name__ == '__main__':
    unittest.main()
