import unittest


from .qdb import Qdb

mass_uuid = '93a60a56-a3c8-11da-a746-0800200b9a66'


class QdbTestCase(unittest.TestCase):
    """
    lots to test here
    """
    @classmethod
    def setUpClass(cls):
        cls._qdb = Qdb()

    def setUp(self):
        self._qi = self._qdb.make_interface('quantity')

    def test_mass(self):
        self.assertEqual(self._qi.get_canonical('mass').uuid, mass_uuid)


if __name__ == '__main__':
    unittest.main()
