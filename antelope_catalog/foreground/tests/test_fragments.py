"""
There are a WHOLE LOT of test cases for fragment traversal because it's a very complex and subtle process.
This is a weakness, ultimately. But the test cases produced here should replace subtlety with clarity.

Here are the things the fragment traversal is supposed to accomplish / enable:

 * sequentially compute node weights, to be multiplied by unit LCIA scores for impact assessment (basic traversal)

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

 * Conservation of up to one quantity per fragment by computing the magnitude of a balance flow

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
 = FragmentFlows must specify which scenarios applied to both ev and termination (exactly 1 for each, maybe None)
   - challenge here is in propagating that info in GhostFragments and aggregations
 = apply-scenario option for subfragment terminations. Build one electricity grid and parameterize it for multiple
   locales; then traverse each instance differently as specified in the termination
 = stochastic exchange values
 = private subfragments can force nondescend?
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

mass = f1.reference_entity

# flows

f2 = new_flow('My second flow', 'volume')
f3 = new_flow('My third flow', 'net calorific value')
f3w = new_flow('A waste energy flow', 'net calorific value')
f4 = new_flow('Another mass flow', 'mass')
f4.add_characterization(qdb.get_canonical('net calorific value'), value=f4_mj_kg)
f5 = new_flow('yet another mass flow', 'mass')
f6 = new_flow('An energetic conservation flow', 'net calorific value')
f7 = new_flow('An ancillary flow', 'number of items')
f7.add_characterization(mass, value=f7_mass)
f8 = new_flow('A freight flow', 'freight')
fp = new_flow('A private flow', 'price')

# parameters

#a1
a1_vol = 10
a1_mj_in = 19
a1_addl = 0.88
# a1 scenarios
a1_mj_optimistic = a1_mj_in * 0.75
a1_surplus_addl = 1.11

# a2
a2_kwh = 10
a2_mj = a2_kwh * 3.6  # kWh converted to MJ
a2_waste_heat = 5
a2_private = 61
a2_item = 0.003
# a2 scenarios
a2_eff_waste_heat = 3.5
a2_eff_private = 49

#a2_alt
a2_alt_fuel = 1.1 / f4_mj_kg
a2_alt_tx = 2.6

#aa
aa_mj_in = 4
aa_in = 0.14


class FragmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Build the test fragments
        :return:
        """
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
        new_fragment(f4, 'Input', parent=cls.a1, value=a1_addl).set_exchange_value('surplus', a1_surplus_addl)

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
        a2p = new_fragment(fp, 'Input', parent=cls.a2, value=a2_private)
        a2p.set_exchange_value('efficiency', a2_eff_private)
        new_fragment(f7, 'Input', parent=a2p, value=a2_item)

        cls.a2.observe(accept_all=True, recurse=True)
        '''a2
           -<--O   a33fc [      36 MJ] A conserving energy conversion process
            [   1 unit] A conserving energy conversion process
               | -<--O   53279 [      61 EUR] A private flow
               |  [   1 unit] A private flow
               |     | -<----: be6fa [   0.003 Item(s)] An ancillary flow
               |     x 
               | =>=-O   b1951 [       5 MJ] A waste energy flow
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
        ac = new_fragment(f3, 'Input', value=aa_mj_in, parent=cls.aa)
        new_fragment(f4, 'Output', parent=cls.aa, balance=True).terminate(cls.af)  # to be run in reverse
        ac.terminate(cls.a2)
        ac.terminate(cls.a2, scenario='nondescend', descend=False)  # to be run in reverse
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
        mag = next(f.magnitude for f in ff if f.fragment.flow == f4)
        self.assertEqual(mag, proper_kg)

    def test_subfragment_child(self):
        """
        Child flows from a subfragment termination have their e.v.s computed based on the subfragment traversal
        self.a1 subfragment of self.a2 with child flow f7 'An ancillary flow'
        :return:
        """
        a2_ancillary = a2_item * a2_private
        a1_ancillary = a2_ancillary * a1_mj_in / self.a2.exchange_value()
        self._check_fragmentflows(self.a2.traverse(None), f7, 'Input', a2_ancillary)
        self._check_fragmentflows(self.a1.traverse(None), f7, 'Input', a1_ancillary)

    def test_unobserved_traversal(self):
        """
        A simple unobserved traversal should just result in cached evs for observable flows, properly computed balance
        :return:
        """
        ffuobs = [ff for ff in self.a1.traverse() if ff.fragment.reference_entity is self.a1]
        for c in self.a1.child_flows:
            if c.is_balance:
                bal = self.a1.exchange_value() - a1_addl
                self._check_fragmentflows(ffuobs, c.flow, c.direction, bal)
            else:
                self._check_fragmentflows(ffuobs, c.flow, c.direction, c.cached_ev)

    def test_unobservable_balance(self):
        for f in self.a1.child_flows:
            if f.is_balance:
                self.assertFalse(f.observable())
            else:
                self.assertTrue(f.observable())

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
        fbal = next(f for f in ff1o if f.fragment is self.a1.balance_flow)
        self.assertEqual(fbal.magnitude, self.a1.exchange_value())  # balance flow should get full amount

    def test_scenarios_detection(self):
        self.assertSetEqual({k for k in self.a1.scenarios()}, {'surplus', 'optimistic', 'improvement', 'efficiency'})
        self.assertSetEqual({k for k in self.a2.scenarios()}, {'efficiency'})
        self.assertSetEqual({k for k in self.aa.scenarios()}, {'efficiency', 'nondescend'})
        self.assertSetEqual({k for k in self.af.scenarios()}, set())

    def test_scenario_ev(self):
        """
        surplus
        :return:
        """
        default = self.a1.traverse()
        surplus = self.a1.traverse('surplus')
        self._check_fragmentflows(default, f4, 'Input', a1_addl, 1-a1_addl, 1-a1_addl)
        self._check_fragmentflows(surplus, f4, 'Input', a1_surplus_addl, 1 - a1_surplus_addl, 1 - a1_surplus_addl)

    def test_scenario_termination(self):
        default = self.a1.traverse()
        improved = self.a1.traverse('improvement')
        self.assertIn(self.a2_alt, [f.fragment for f in improved])
        self.assertNotIn(self.a2_alt, [f.fragment for f in default])
        self._check_fragmentflows(default, f8, 'Input')  # not present
        self._check_fragmentflows(improved, f8, 'Input', a1_mj_in * a2_alt_tx)  # present

    def test_autoconsumption(self):
        """
        fragment self.aa produces f7 "ancillary flows" in the amount of one unit.  but it also depends on fragment
        self.a2, which has a cutoff input requirement for f7.  The inventory results for self.aa should report only
        net production of f7
        :return:
        """
        f7_autoconsumption = aa_mj_in / self.a2.exchange_value() * a2_private * a2_item
        inv = self.aa.inventory()
        f7_out = next(f for f in inv if f.flow is f7)
        f5_in = next(f for f in inv if f.flow is f5)
        self.assertEqual((f7_out.direction, f7_out.value), ('Output', 1 - f7_autoconsumption))
        self.assertEqual((f5_in.direction, f5_in.value), ('Input', aa_in))

    def test_nonreference_subfragment(self):
        """
        A termination to a non-reference flow of a subfragment computes the proper node weight for the subfragment
        Here we build an ad hoc fragment that coproduces f5 'yet another mass flow'-- which is also
        required by the autoconsumption fragment self.aa.

        We are going to assume that the co-product drives additional production of f7 "ancillary flows" by terminating
        our coproduct flow to self.aa.  This should have the effect of driving the autoconsuming fragment in its
        forward direction and outputting f7.
        :return:
        """
        coproduct = 6
        fq = new_flow('transparent aluminum', 'area')
        at = new_fragment(fq, 'Output')
        new_fragment(f5, 'Output', parent=at, value=coproduct).terminate(self.aa, term_flow=f5)
        # node weight of driven node
        aa_nw = coproduct / aa_in
        # unit consumption of f7 by energy production node
        f7_autoconsumption = aa_mj_in / self.a2.exchange_value() * a2_private * a2_item
        net_f7_production = aa_nw * (1 - f7_autoconsumption)
        ffs = at.traverse()
        self._check_fragmentflows(ffs, f7, 'Output', net_f7_production)

    def test_negative_fragment(self):
        """
        self.a1 balancing flow is negative under the 'surplus' scenario.  self.a1 balancing flow terminates to self.af
        so if the driving amount is negative, the node weight of the subfragment should be negative.
        :return:
        """
        ffs = self.a1.traverse('surplus')
        self._check_fragmentflows(ffs, f5, 'Input', 1 - a1_surplus_addl)
        for f in ffs:
            if f.fragment.top() is self.af:
                self.assertTrue(f.node_weight < 0)

    def test_balance_nonreference_quantities(self):
        """
        self.aa reference product is characterized w.r.t. mass, so balance flow should account for that
        :return:
        """
        ffs = self.aa.traverse()
        net_balance = aa_in - f7.cf(mass)
        self._check_fragmentflows(ffs, f4, 'Output', net_balance)

    def test_inverted_fragment(self):
        """
        self.aa produces an output of f5 which is terminated to self.af, which also produces an output of af.  Thus
        when self.af is the subfragment its node weight should be negative.
        :return:
        """
        ffs = self.aa.traverse('nondescend')  # ensure that the subfragment's dependence on af is concealed
        for f in ffs:
            if f.fragment.top() is self.af:
                self.assertTrue(f.node_weight < 0, '%s' % f)

    def test_descend_equivalence(self):
        """
        Inventory results should not vary regardless of descend (this may require more robust testing)
        :return:
        """
        inv_d = self.aa.inventory()
        inv_nd = self.aa.inventory('nondescend')
        self.assertEqual(inv_d, inv_nd)

    def test_nondescend_privacy(self):
        """
        The internal fragment of fp in self.a2 should be concealed when descend=False, visible when descend=True
        :return:
        """
        ff_d = self.aa.traverse()
        ff_nd = self.aa.traverse('nondescend')
        expected_private = aa_mj_in / self.a2.exchange_value() * a2_private
        expected_waste = aa_mj_in / self.a2.exchange_value() * a2_waste_heat
        self._check_fragmentflows(ff_d, fp, 'Input', expected_private)
        self._check_fragmentflows(ff_nd, fp, 'Input')
        self._check_fragmentflows(ff_d, f3w, 'Output', expected_waste)
        self._check_fragmentflows(ff_nd, f3w, 'Output')

    def test_nonscenario(self):
        """
        A Scenarios specification is just an unordered collection of scenario names. Names for scenarios that are not
        found in the fragment should simply have no effect.
        :return:
        """
        self.assertNotIn('moriarty', [k for k in self.a1.scenarios()])
        ffs = self.a1.traverse()
        ffs_m = self.a1.traverse('moriarty')
        self.assertEqual(ffs, ffs_m)

    def test_multiple_scenarios(self):
        """
        Here we combine several different scenarios and look for signatures of each in the traversal results.
        'optimistic' reduces a1 energy requirements
        'efficiency' improves a2 performance
        'surplus' changes the amount of input received by a1
        'improvement' substitutes a2_alt for a2
        :return:
        """
        ff_o_e = self.a1.traverse({'optimistic', 'efficiency'})
        expected_item = a1_mj_optimistic / self.a2.exchange_value() * a2_eff_private * a2_item
        self._check_fragmentflows(ff_o_e, f7, 'Input', expected_item)
        self._check_fragmentflows(ff_o_e, f5, 'Input', 1-a1_addl)

        ff_o_s_e = self.a1.traverse({'optimistic', 'surplus', 'efficiency'})
        self._check_fragmentflows(ff_o_s_e, f7, 'Input', expected_item)
        self._check_fragmentflows(ff_o_s_e, f5, 'Input', 1 - a1_surplus_addl)

        ff_o_i_s = self.a1.traverse({'optimistic', 'improvement', 'surplus'})
        expected_another = a1_mj_optimistic / self.a2_alt.exchange_value() * a2_alt_fuel
        self._check_fragmentflows(ff_o_i_s, f7, 'Input')
        self._check_fragmentflows(ff_o_i_s, f4, 'Input',
                                  a1_surplus_addl, 1 - a1_surplus_addl, 1 - a1_surplus_addl, expected_another)


if __name__ == '__main__':
    unittest.main()
