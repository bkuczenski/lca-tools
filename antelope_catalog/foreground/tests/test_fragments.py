"""
There are a WHOLE LOT of test cases for fragment traversal because it's a very complex and subtle process.
This is a weakness, ultimately. But the test cases produced here should replace subtlety with clarity.

Here are the things the fragment traversal is supposed to accomplish / enable:

 * sequentially computed node weights, to be multiplied by unit LCIA scores for impact assessment (basic traversal)

 * observability of fragments: each fragment has a separate notion of the exchange value it was "born with" and the
   "observed" exchange value set by the modeler.

   Note: This is a bit of a "solution in search of a problem," but the notional problem is distinguishing between
   inventory data grabbed directly from a reference db and a data point that has been validated by the modeler.
   Traversal with observed=True will use the observed exchange values, which default to 0 so unobserved exchanges
   will be absent.  There is also a routine way to 'observe' a fragment, automatically accepting cached values and
   even recursing.

 * variability of both exchange values and fragment terminations by scenario specification
   (a) exchange value scenario
   (b) termination scenario
   (c) determine valid scenarios for a fragment and subfragments

 * Conservation of at most one quantity per fragment by computing the magnitude of a balance flow

 * the ability to nest fragments and
   (a) have the outcomes of nested traversals affect higher-level traversals (i.e. flow mapping between subfragment
       cutoff flows and superfragment child flows)
   (b) have subfragments driven from non-reference outputs (i.e. encapsulated traversal of sub-fragments)

 * the ability to control whether a subfragment is transparent or opaque (aggregated), a.k.a. 'descend' vs 'non-descend'

 * unit-matching flow magnitude conversions
   (a) when specifying a fragment
   (b) during traversal, between a fragment and its termination

 * the ability to drive fragments in reverse
   (a) by supplying a negative node weight
   (b) by invoking them with a flow whose direction is opposing the fragment's reference direction

Not yet implemented:
 = apply-scenario option for subfragment terminations. Build one electricity grid and traverse each instance differently
 = monte carlo analysis in traversal
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
f7_mass = 0.0123

f1 = new_flow('My first flow', 'mass')
f2 = new_flow('My second flow', 'volume')
f3 = new_flow('My third flow', 'net calorific value')
f3w = new_flow('A waste energy flow', 'net calorific value')
f4 = new_flow('Another mass flow', 'mass')
f4.add_characterization(qdb.get_canonical('net calorific value'), value=f4_mj_kg)
f5 = new_flow('yet another mass flow', 'mass')
f6 = new_flow('An energetic conservation flow', 'net calorific value')
f7 = new_flow('An ancillary flow', 'number of items')
f7.add_characterization(qdb.get_canonical('mass'), value=f7_mass)
f8 = new_flow('A freight flow', 'freight')

a1_vol = 10
a1_mj_in = 19
a1_mj_optimistic = a1_mj_in * 0.75
a1_addl = 0.88
a1_addl_alt = 1.11

a2_kwh = 10
a2_mj = a2_kwh * 3.6  # kWh converted to MJ
a2_waste_heat = 5
a2_item = 0.183
a2_eff_waste_heat = 3.5
a2_eff_item = 0.147

a2_alt_fuel = 1.1 / f4_mj_kg
a2_alt_tx = 2.6

ac_mj_in = 4


aa_in = 0.14


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
        a3.set_exchange_value('optimistic', a1_mj_optimistic)
        a3.terminate(cls.a2, descend=False)

        new_fragment(f7, 'Input', parent=a3)
        new_fragment(f5, 'Input', parent=cls.a1, balance=True).terminate(cls.af, term_flow=f4)  # to be run negated
        new_fragment(f4, 'Input', parent=cls.a1, value=a1_addl).set_exchange_value('surplus', a1_addl_alt)

        '''a1
           -<--O   70a01 [       1 kg] A Production Process
            [   1 unit] A Production Process
               | -<--#:: 3fe31 (=      1 kg) yet another mass flow
               | -<----: a37a3 (     1.2 kg) Another mass flow
               | -<----: a4a0c (      10 m3) My second flow
               | -<--#   397f5 (      19 MJ) My third flow
               |  [   1 unit] My third flow
               |     | -<----: c3f26 (       1 Item(s)) An ancillary flow
               |     x 
               x 
        (a1d is the same but with descend=True on "My third flow")
        '''

        a3w = new_fragment(f3w, 'Output', parent=cls.a2, value=a2_waste_heat)
        a3w.set_exchange_value('efficiency', a2_eff_waste_heat)
        a3w.to_foreground()

        a6 = new_fragment(f6, 'Input', parent=cls.a2, balance=True)
        a6.terminate(cls.af, term_flow=f4)
        new_fragment(f7, 'Input', parent=cls.a2, value=a2_item).set_exchange_value('efficiency', a2_eff_item)

        cls.a2.observe(accept_all=True, recurse=True)
        '''a2
           -<--O   a33fc [      36 MJ] A conserving energy conversion process
            [   1 unit] A conserving energy conversion process
               | -<----: 2b587 [   0.183 Item(s)] An ancillary flow
               | =>=---: b1951 [       5 MJ] A waste energy flow
               | -<--#:: b09e3 (=      1 MJ) An energetic conservation flow
               x 
        '''
        # make an alternate energy supply fragment to test scenario terminations
        cls.a2_alt = new_fragment(f3, 'Output', Name='An alternate energy conversion process')
        new_fragment(f4, 'Input', parent=cls.a2_alt, value=a2_alt_fuel)
        new_fragment(f8, 'Input', parent=cls.a2_alt, value=a2_alt_tx)
        '''a2_alt
           -<--O   db333 [       1 MJ] An alternate energy conversion process
            [   1 unit] An alternate energy conversion process
               | -<----: 99b68 (  0.0314 kg) Another mass flow
               | -<----: beda8 (     2.6 t*km) A freight flow
               x 
        This a2_alt is supplied as an alternate termination scenario. it lacks the 'ancillary item' flow so that
        child flow should be found to be zero and should not appear in the traversal.  At the same time, it contains
        a 'freight' flow which SHOULD be visible as long as descend=True.
        '''

        a3.terminate(cls.a2_alt, scenario='improvement', descend=True)

        new_fragment(f5, 'Input', parent=cls.aa, value=aa_in)
        ac = new_fragment(f3, 'Input', value=ac_mj_in, parent=cls.aa)
        new_fragment(f4, 'Output', parent=cls.aa, balance=True).terminate(cls.af)  # to be run in reverse
        ac.terminate(cls.a2)
        '''aa
           -<--O   b0565 [       1 Item(s)] An auto-consumption process
            [   1 unit] An auto-consumption process
               | -<----: dbda7 (    0.14 kg) yet another mass flow
               | -<--#:: 332f7 (       4 MJ) My third flow
               | =>=-#:: 48149 (=      1 kg) Another mass flow
               x 
               
        This process produces ancillary items, but uses fragment a2 which consumes ancillary items. This process
        also produces a fuel co-product which is used to drive the fuel subfragment negative.
        '''

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
        """
        af is a very simple fragment with one reference flow and one foreground flow, which is a stand-in for an
        aggregated process inventory.
        :return:
        """
        ffs = self.af.traverse(None)
        self._check_fragmentflows(ffs, f4, 'Input', 1, 1)

    def test_af_unit_inventory(self):
        """
        A unit inventory reports two separate sets of fragment flows: cutoffs (cross system boundary) and nodes (within
        system boundary).  For af, there is one cutoff (the reference flow) and two internal, both foreground fragments.
        :return:
        """
        io, ff = self.af.unit_inventory()
        self._check_fragmentflows(io, f4, 'Output', 1)
        self.assertEqual(len(ff), 2)
        # foreground <=> term is self
        self.assertSetEqual({True}, {f.term.term_node is f.fragment for f in ff})

    def test_unit_conversion_on_fragment_creation(self):
        self.assertEqual(self.a2.exchange_value(), a2_mj)

    def test_unit_conversion_on_flow_termination(self):
        proper_kg = (a2_mj + a2_waste_heat) / f4_mj_kg
        ff = self.a2.traverse(None)
        mag = next(f.magnitude for f in ff if f.flow == f4)
        self.assertEqual(mag, proper_kg)

    def test_subfragment_child(self):
        """
        Child flows from a subfragment termination have their e.v.s computed based on the subfragment traversal
        self.a1 subfragment of self.a2 with child flow f7 'An ancillary flow'
        :return:
        """
        ancillary = a2_item * a1_mj_in / self.a2.exchange_value()
        self._check_fragmentflows(self.a2.traverse(None), f7, 'Input', a2_item)
        self._check_fragmentflows(self.a1.traverse(None), f7, 'Input', ancillary)

    def test_unobserved_traversal(self):
        """
        A simple unobserved traversal should just result in
        :return:
        """
        ffuobs = [ff for ff in self.a1.traverse() if ff.fragment.reference_entity is self.a1]
        for c in self.a1.child_flows:
            if c.balance_flow:
                continue
            self._check_fragmentflows(ffuobs, c.flow, c.direction, c.cached_ev)

    def test_observed_traversal(self):
        """
        An unobserved traversal should use the cached values (i.e. defined at creation) whereas an observed traversal
        should use the modeler's observed values, which default to zero.  Fragment a1 is not observed whereas fragment
        a2 is auto-observed to apply cached values as observed values throughout.
        :return:
        """
        ff1 = self.a1.traverse()
        ff1o = self.a1.traverse(observed=True)
        ff2 = self.a2.traverse()
        ff2o = self.a2.traverse(observed=True)
        self.assertEqual(ff2, ff2o)
        self.assertNotEqual(ff1, ff1o)

    def test_scenarios(self):
        self.assertSetEqual({k for k in self.a1.scenarios()}, {'surplus', 'optimistic', 'improvement'})

    def test_scenario_ev(self):
        pass

    def test_scenario_termination(self):
        pass

    def test_nonreference_subfragment(self):
        """
        A termination to a non-reference flow of a subfragment computes the proper node weight for the subfragment
        :return:
        """

    def test_negative_fragment(self):
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
