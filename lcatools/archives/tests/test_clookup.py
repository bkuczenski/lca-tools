from ..clookup import CLookup, SCLookup, FactorCollision, QuantityMismatch
from ...entities import LcFlow, LcQuantity
from ...characterizations import Characterization
from synonym_dict.example_compartments import Context
import unittest


q1 = LcQuantity.new('A quantity', 'kg')
q2 = LcQuantity.new('Another quantity', 'MJ ncv')
q3 = LcQuantity.new('A third quantity', 'MJ gcv')

f1 = LcFlow.new('Massive flow', q1, Compartment=['emissions', 'to air'])
f1d = LcFlow.new('Massive flow', q1, Compartment=['emissions', 'to air'])
f2 = LcFlow.new('Energetic flow', q2, Compartment=['resources', 'from ground'])

cx_air = Context('to air')
cx_ua = Context('to urban air', parent=cx_air)
cx_ra = Context('to rural air', parent=cx_air)
cx_r = Context('resources')
cx_rg = Context('from ground', parent=cx_r)

cf = Characterization(f1, q2, context=cx_air, value=47)
cfd = Characterization(f1d, q2, context=cx_air, value=84)
cfua = Characterization(f1, q2, context=cx_ua, value=58)
cfra = Characterization(f1, q2, context=cx_ra, value=51)

cg = Characterization(f2, q2, context=cx_r, value=123)
ch = Characterization(f2, q3, context=Context.null(), value=1.023)


class SingleCfTest(unittest.TestCase):
    def test_create_clookup(self):
        g = CLookup()
        g.add(cf)
        self.assertIs(g.find_first(cx_air, dist=0), cf)

    def test_strict(self):
        g = CLookup()
        gs = SCLookup()
        g.add(cf)
        gs.add(cf)
        g.add(cfd)
        with self.assertRaises(FactorCollision):
            gs.add(cfd)

    def test_quantity_mismatch(self):
        g = CLookup()
        g.add(cg)
        with self.assertRaises(QuantityMismatch):
            g.add(ch)

    def test_find_1(self):
        g = CLookup()
        g.add(cfua)
        g.add(cfra)
        self.assertSetEqual(g.find(cx_air, dist=0), set())
        self.assertSetEqual({x.value for x in g.find(cx_air, dist=1, return_first=False)}, {51, 58})

    def test_find_2(self):
        g = CLookup()
        g.add(cg)
        self.assertSetEqual(g.find(cx_rg, dist=0), set())
        self.assertSetEqual(g.find(cx_rg, dist=1), set())
        self.assertSetEqual(g.find(cx_rg, dist=2), {cg})


if __name__ == '__main__':
    unittest.main()