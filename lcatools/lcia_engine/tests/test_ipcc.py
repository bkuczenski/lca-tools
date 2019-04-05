import unittest
from lcatools.archives import BasicArchive
from .. import IPCC_2007_GWP
from lcatools.exchanges import ExchangeValue
from lcatools.entities.flows import new_flow

from collections import namedtuple

DummyEntity = namedtuple('DummyEntity', ('origin', 'uuid', 'external_ref'))


I = BasicArchive.from_file(IPCC_2007_GWP)
mass = I['Mass']
air = I.tm['air']

def _exch_gen():
    p = DummyEntity('test.null', '1234567', 'dummy process')
    co2 = new_flow('carbon dioxide', mass, origin='test')
    ch4 = new_flow('methane', mass, origin='test')
    yield ExchangeValue(p, co2, 'Output', termination=air, value=34.7)
    yield ExchangeValue(p, ch4, 'Output', termination=air, value=16)


class IpccTestCase(unittest.TestCase):
    def test_lcia(self):
        gwp = I['Global Warming Air']
        res = gwp.do_lcia(_exch_gen())
        self.assertEqual(res.total(), 34.7 + 16*25)


if __name__ == '__main__':
    unittest.main()
