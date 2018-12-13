"""
For fragment tests, we are using the CalRecycle model to provide test cases
"""

import unittest
# from math import floor

from ..fragment_editor import FragmentEditor
from lcatools.entities.editor import FlowEditor
from lcatools.qdb import Qdb
from lcatools.interfaces import CONTEXT_STATUS_

origin = 'test.origin'

qdb = Qdb()
flow_ed = FlowEditor(qdb)
frag_ed = FragmentEditor()

mass = qdb.get_canonical('mass')
ncv = qdb.get_canonical('Net calorific value')
vol = qdb.get_canonical('volume')


def new_flow(name, ref_quantity, context=None, **kwargs):
    """

    :param name:
    :param ref_quantity:
    :param context: [None] pending context refactor
    :param kwargs:
    :return:
    """
    if CONTEXT_STATUS_ == 'compat':
        if context is not None and 'compartment' not in kwargs:
            kwargs['compartment'] = str(context)
    ref_q = qdb.get_canonical(ref_quantity)
    f = flow_ed.new_flow(name=name, quantity=ref_q, origin=origin, **kwargs)
    # self._archive.add_entity_and_children(f)
    return f


def new_fragment(*args, **kwargs):
    """

    :param args: flow, direction (w.r.t. parent)
    :param kwargs: uuid=None, parent=None, comment=None, value=None, units=None, balance=False;
      **kwargs passed to LcFragment
    :return:
    """
    frag = frag_ed.create_fragment(*args, origin=origin, **kwargs)
    # self._archive.add_entity_and_children(frag)
    return frag

f4_mj_kg = 35

f1 = new_flow('My first flow', 'mass')
f2 = new_flow('My second flow', 'volume')
f3 = new_flow('My third flow', 'net calorific value')
f3w = new_flow('A waste energy flow', 'net calorific value')
f4 = new_flow('Another mass flow', 'mass')
f4.add_characterization(qdb.get_canonical('net calorific value'), value=f4_mj_kg)
f5 = new_flow('yet another mass flow', 'mass')
f6 = new_flow('An energetic conservation flow', 'net calorific value')
f7 = new_flow('An ancillary flow', 'number of items')

a1_vol = 10
a1_mj_in = 19
a1_addl = 1.2

a2_kwh = 10
a2_waste_heat = 5
a2_item = 0.183

ac_mj_in = 4

a2_mj = a2_kwh * 3.6  # kWh converted to MJ


class FragmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a1 = new_fragment(f1, 'Output', Name='A Production Process')
        cls.a2 = new_fragment(f3, 'Output', value=a2_kwh, units='kWh', Name='A conserving energy conversion process')
        cls.af = new_fragment(f4, 'Output', Name='A fuel supply process')
        cls.aa = new_fragment(f7, 'Output', Name='An auto-consumption process')

        afg = new_fragment(f4, 'Input', parent=cls.af, balance=True, Name='Internal fuel supply')
        afg.to_foreground()  # in the wild, this would be terminated and not show up as an IO-- fg emulates that

        new_fragment(f2, 'Input', parent=cls.a1, value=a1_vol)
        a3 = new_fragment(f3, 'Input', parent=cls.a1, value=a1_mj_in)
        new_fragment(f5, 'Input', parent=cls.a1, balance=True)
        new_fragment(f4, 'Input', parent=cls.a1, value=a1_addl)
        a3.terminate(cls.a2, descend=False)
        new_fragment(f7, 'Input', parent=a3)

        '''a1
           -<--O   bfe35 [       1 kg] My first flow
            [   1 unit] My first flow
               | -<----: 01db0 (=      1 kg) yet another mass flow
               | -<----: 15923 (      10 m3) My second flow
               | -<--#:: d24f5 (     1.2 kg) Another mass flow
               x 
        (a1d is the same but with descend=False)
        '''

        new_fragment(f3w, 'Output', parent=cls.a2, value=a2_waste_heat)
        a6 = new_fragment(f6, 'Input', parent=cls.a2, balance=True)
        a6.terminate(cls.af, term_flow=f4)
        new_fragment(f7, 'Input', parent=cls.a2, value=a2_item)

        cls.a2.observe(accept_all=True, recurse=True)
        '''
           -<--O   9fbf7 [      36 MJ] My third flow
            [   1 unit] My third flow
               | =>=---: 2c6d4 [       5 MJ] My third flow
               | -<--#:: cbd5c (=      1 MJ) An energetic conservation flow
               x 
        '''

        # now make a clone to test both clone and descend
        cls.a1d = frag_ed.clone_fragment(cls.a1)

        a3d = next(c for c in cls.a1d.child_flows if c.flow is a3.flow)
        a3d.terminate(cls.a2, descend=True)

        ac = new_fragment(f3, 'Input', value=ac_mj_in, parent=cls.aa)
        ac.terminate(cls.a2)

    def _check_fragmentflows(self, ffs, flow, direction, *magnitudes):
        """
        A routine to test whether a set of traversal results includes the expected set of flows, directions, and
        magnitudes.

        :param ffs: a list of fragment flows
        :param flow: a flow entity to look for
        :param direction: the direction to look for
        :param magnitudes: a sequence of positional args indicating the magnitudes expected to be found in the traversal
        :return:
        """
        filt_list = [ff for ff in ffs if ff.fragment.flow == flow and ff.fragment.direction == direction]
        self.assertEqual(len(filt_list), len(magnitudes))
        self.assertSetEqual(set([f.magnitude for f in filt_list]), set(magnitudes))

    def test_af_traversal(self):
        ffs = self.af.traverse(None)
        self._check_fragmentflows(ffs, f4, 'Input', 1, 1)

    def test_af_unit_inventory(self):
        io, ff = self.af.unit_inventory()
        self._check_fragmentflows(io, f4, 'Output', 1)
        self.assertEqual(len(ff), 2)
        self.assertSetEqual({True}, {f.term.term_node is f.fragment for f in ff})

    def test_unit_conversion_on_fragment_creation(self):
        self.assertEqual(self.a2.exchange_value(), a2_mj)

    def test_unit_conversion_on_flow_termination(self):
        proper_kg = (a2_mj + a2_waste_heat) / f4_mj_kg
        ff = self.a2.traverse(None)
        mag = next(f.magnitude for f in ff if f.flow == f4)
        self.assertEqual(mag, proper_kg)

    def test_subfragment_child(self):
        pass

    def test_nonreference_subfragment(self):
        pass

    def test_inverted_fragment(self):
        pass

    def test_descend_traversal(self):
        pass

    def test_nondescend_traversal(self):
        pass

    def test_autoconsumption(self):
        pass


if __name__ == '__main__':
    unittest.main()
