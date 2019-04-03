import unittest
from lcatools.archives import BasicArchive
from .. import IPCC_2007_GWP
from lcatools.exchanges import ExchangeValue


def _exch_gen():
    p = object()
    p.origin = 'test.null'
    p.uuid = '1234567'
    yield ExchangeValue(p, 'carbon dioxide', 'Output', 34.7)
    yield ExchangeValue(p, 'methane', 'Output', 16)


class IpccTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._I = BasicArchive.from_file(IPCC_2007_GWP)

    def test_lcia(self):
        gwp = self._I['Global Warming Air']
        res = gwp.do_lcia(_exch_gen())
        self.assertEqual(res.total(), 34.7 + 16*25)


if __name__ == '__main__':
    unittest.main()
