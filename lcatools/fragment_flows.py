from lcatools.interfaces import comp_dir

from .terminations import FlowTermination, SubFragmentAggregation
from .characterizations import DuplicateCharacterizationError
from lcatools.lcia_results import LciaResult, DetailedLciaResult, SummaryLciaResult

from collections import defaultdict
from math import isclose

class CumulatingFlows(Exception):
    """
    when a fragment includes multiple instances of the reference flow having consistent (i.e. not complementary)
    directions. Not handled in subfragment traversal bc no valid test case
    """
    pass


class FragmentFlow(object):
    """
    A FragmentFlow is a an immutable record of a traversal query. essentially an enhanced NodeCache record which
    can be easily serialized to an antelope fragmentflow record.

    A fragment traversal generates an array of FragmentFlow objects.

    X    "fragmentID": 8, - added by antelope
    X    "fragmentStageID": 80,

    f    "fragmentFlowID": 167,
    f    "name": "UO Local Collection",
    f    "shortName": "Scenario",
    f    "flowID": 371,
    f    "direction": "Output",
    f    "parentFragmentFlowID": 168,
    f    "isBackground": false,

    w    "nodeWeight": 1.0,

    t    "nodeType": "Process",
    t    "processID": 62,

    *    "isConserved": true,
    *    "flowPropertyMagnitudes": [
      {
        "flowPropertyID": 23,
        "unit": "kg",
        "magnitude": 1.0
      }
    ]

    """
    @classmethod
    def from_antelope_v1(cls, j, query):
        """
        Need to:
         * create a termination
         * create a fragment ref
         * extract node weight
         * extract magnitude
         * extract is_conserved
        :param j: JSON-formatted fragmentflow, from a v1 .NET antelope instance.  Must be modified to include StageName
         instead of fragmentStageID
        :param query: an antelope v1 catalog query
        :return:
        """
        fpms = j['flowPropertyMagnitudes']
        ref_mag = fpms[0]
        magnitude = ref_mag['magnitude']
        flow = query.get('flows/%s' % j['flowID'])
        for fpm in fpms[1:]:
            mag_qty = query.get('flowproperties/%s' % fpm['flowPropertyID'])
            if fpm['magnitude'] == 0:
                val = 0
            else:
                val = fpm['magnitude'] / magnitude
            try:
                flow.characterize(mag_qty, value=val, compartment=None)
            except DuplicateCharacterizationError:
                if not isclose(mag_qty.cf(flow).value, val):
                    raise ValueError('Characterizations do not match: %g vs %g' % (mag_qty.cf(flow), val))

        dirn = j['direction']

        if 'parentFragmentFlowID' in j:
            parent = 'fragments/%s/fragmentflows/%s' % (j['fragmentID'], j['parentFragmentFlowID'])
            frag = GhostFragment(parent, flow, dirn)

        else:
            frag = query.get('fragments/%s' % j['fragmentID'])

        node_type = j['nodeType']
        nw = j['nodeWeight']
        if magnitude == 0:
            inbound_ev = 0
        else:
            inbound_ev = magnitude / nw

        if node_type == 'Process':
            term_node = query.get('processes/%s' % j['processID'])
            term = FlowTermination(frag, term_node, term_flow=flow, inbound_ev=inbound_ev)
        elif node_type == 'Fragment':
            term_node = query.get('fragments/%s' % j['subFragmentID'])
            term = FlowTermination(frag, term_node, term_flow=flow, inbound_ev=inbound_ev)
        else:
            term = FlowTermination.null(frag)
        if 'isConserved' in j:
            conserved = j['isConserved']
        else:
            conserved = False
        return cls(frag, magnitude, nw, term, conserved)

    @classmethod
    def ref_flow(cls, parent, use_ev):
        """

        :param parent:
        :param use_ev: required to create reference flows from fragment refs
        :return:
        """
        fragment = GhostFragment(parent, parent.flow, comp_dir(parent.direction))
        term = FlowTermination.null(fragment)
        return cls(fragment, use_ev, 1.0, term,
                   parent.is_conserved_parent)

    @classmethod
    def cutoff(cls, parent, flow, direction, magnitude, is_conserved=False):
        fragment = GhostFragment(parent, flow, direction)
        term = FlowTermination.null(fragment)
        return cls(fragment, magnitude, magnitude, term, is_conserved)

    def __init__(self, fragment, magnitude, node_weight, term, is_conserved):
        """

        :param fragment:
        :param magnitude:
        :param node_weight:
        :param term:
        :param is_conserved:
        """
        # TODO: figure out how to cache + propagate scenario applications through aggregation ops
        self.fragment = fragment
        self.magnitude = magnitude
        self.node_weight = node_weight
        self.term = term
        self.is_conserved = is_conserved
        self._subfrags_params = None

    @property
    def subfragments(self):
        if self.term.is_subfrag and (self.term.descend is False):
            return self._subfrags_params[0]
        return []

    @property
    def subfragment_params(self):
        return {'scenario': self._subfrags_params[1],
                'observed': self._subfrags_params[2]}

    def aggregate_subfragments(self, subfrags, scenario=None, observed=False):
        self._subfrags_params = (subfrags, scenario, observed)

    def scale(self, x):
        self.node_weight *= x
        self.magnitude *= x

    def __str__(self):
        if self.term.is_null:
            term = '--:'
            name = self.fragment['Name']
        else:
            term = '-# '
            name = self.term.term_node.name
        return '%.5s  %10.3g [%6s] %s %s' % (self.fragment.uuid, self.node_weight, self.fragment.direction,
                                             term, name)

    def __add__(self, other):
        if isinstance(other, FragmentFlow):
            if other.fragment.uuid != self.fragment.uuid:
                raise ValueError('Fragment flows do not belong to the same fragment')
            mag = other.magnitude
            nw = other.node_weight
            if not self.term == other.term:
                raise ValueError('These fragment flows are differently terminated')

            if mag * self.node_weight / (self.magnitude * nw) != 1.0:
                raise ValueError('These fragment flows cannot be combined because their implicit evs do not match')
            conserved = self.is_conserved and other.is_conserved
        elif isinstance(other, DetailedLciaResult):
            print('DEPRECATED: adding FragmentFlow to DetailedLciaResult')
            if other.exchange.process is not self.fragment:
                raise ValueError('FragmentFlow and DetailedLciaResult do not belong to the same fragment')
            nw = other.exchange.value
            mag = nw
            conserved = False
        elif isinstance(other, SummaryLciaResult):
            print('DEPRECATED: adding FragmentFlow to SummaryLciaResult')
            if other.entity is not self.fragment:
                raise ValueError('FragmentFlow and SummaryLciaResult do not belong to the same fragment')
            nw = other.node_weight
            mag = nw
            conserved = False
        else:
            raise TypeError("Don't know how to add type %s to FragmentFlow\n %s\n to %s" % (type(other), other, self))
        # don't check unit scores-- ?????
        new = FragmentFlow(self.fragment, self.magnitude + mag, self.node_weight + nw,
                           self.term, conserved)
        return new

    def __eq__(self, other):
        """
        FragmentFlows are equal if they have the same fragment, termination, and magnitude
        :param other:
        :return:
        """
        if not isinstance(other, FragmentFlow):
            return False
        return self.fragment == other.fragment and self.term == other.term  and self.magnitude == other.magnitude

    def __hash__(self):
        return hash(self.fragment)

    def to_antelope(self, fragmentID, stageID):
        pass


def group_ios(parent, ffs, include_ref_flow=True):
    """
    Utility function for dealing with a traversal result (list of FragmentFlows)
    Creates a list of cutoff flows from the inputs and outputs from a fragment traversal.
    ios is a list of FragmentFlows
    :param parent: the node generating the cutoffs
    :param ffs: a list of fragment flows resulting from a traversal of the parent
    :param include_ref_flow: [True] whether to include the reference fragment and adjust for autoconsumption
    :return: [list of grouped IO flows], [list of internal non-null flows]
    """
    out = defaultdict(float)
    internal = []
    external = []
    for ff in ffs:
        if ff.term.is_null:
            # accumulate IO flows under the convention that inflows are positive, outflows are negative
            if ff.fragment.direction == 'Input':
                magnitude = ff.magnitude
            else:
                magnitude = -ff.magnitude
            out[ff.fragment.flow] += magnitude
        else:
            internal.append(ff)

    # now deal with reference flow-- trivial fragment should wind up with two equal-and-opposite [pass-through] flows
    if include_ref_flow:
        ref_frag = parent.top()
        ref_mag = ffs[0].magnitude
        if ref_frag.flow in out:  # either pass through or autoconsumption
            ref_frag.dbg_print('either pass through or autoconsumption')
            val = out[ref_frag.flow]
            if val < 0:
                auto_dirn = 'Output'
            else:
                auto_dirn = 'Input'
            """
            Default is autoconsumption, which is fine as long as 
             (a) directions are complementary [meaning equal since ref flow dirn is w.r.t. parent] and 
             (b) magnitude of autoconsumption is smaller
            """
            if auto_dirn == ref_frag.direction:
                if abs(val) < ref_mag:
                    ref_frag.dbg_print('autoconsumption %g %g' % (val, ref_mag))
                    # autoconsumption, the direction sense of the autoconsumed flow should switch
                    if auto_dirn == 'Output':
                        out[ref_frag.flow] += ref_mag
                    else:
                        out[ref_frag.flow] -= ref_mag

                else:
                    ref_frag.dbg_print('pass thru no effect %g %g' % (val, ref_mag))
                    # pass-thru: pre-initialize external with the reference flow, having the opposite direction
                    external.append(FragmentFlow.cutoff(parent, ref_frag.flow, comp_dir(auto_dirn), ref_mag))
            else:
                ref_frag.dbg_print('cumulation! %g %g' % (val, ref_mag))
                # cumulation: the directions are both the same... should they be accumulated?  not handled
                raise CumulatingFlows('%s' % parent)
                # external.append(FragmentFlow.cutoff(parent, ref_frag.flow, auto_dirn, ref_mag))
        else:
            ref_frag.dbg_print('uncomplicated ref flow')
            # no autoconsumption or pass-through, but we still want the ref flow to show up in the inventory
            external.append(FragmentFlow.cutoff(parent, ref_frag.flow, comp_dir(ref_frag.direction), ref_mag))

    for flow, value in out.items():
        if value < 0:
            direction = 'Output'
        else:
            direction = 'Input'
        external.append(FragmentFlow.cutoff(parent, flow, direction, abs(value)))

    return external, internal


def frag_flow_lcia(fragmentflows, quantity_ref, scenario=None, refresh=False, ignore_uncached=True):
    """
    Recursive function to compute LCIA of a traversal record contained in a set of Fragment Flows.
    :param fragmentflows:
    :param quantity_ref:
    :param scenario: necessary if any remote traversals are required
    :param refresh: whether to refresh the LCIA CFs
    :param ignore_uncached: [True] whether to allow zero scores for un-cached, un-computable fragments
    :return:
    """
    result = LciaResult(quantity_ref)
    for ff in fragmentflows:
        if ff.term.is_null:
            continue

        node_weight = ff.node_weight
        if node_weight == 0:
            continue

        try:
            v = ff.term.score_cache(quantity=quantity_ref, refresh=refresh, ignore_uncached=ignore_uncached)
        except SubFragmentAggregation:
            # if we were given interior fragments, recurse on them. otherwise ask remote.
            if len(ff.subfragments) == 0:
                v = ff.term.term_node.fragment_lcia(quantity_ref, scenario=scenario, refresh=refresh)
            else:
                v = frag_flow_lcia(ff.subfragments, quantity_ref, refresh=refresh)
        if v.is_null:
            continue

        if ff.term.direction == ff.fragment.direction:
            # if the directions collide (rather than complement), the term is getting run in reverse
            node_weight *= -1

        result.add_summary(ff.fragment.uuid, ff, node_weight, v)
    return result


class GhostFragment(object):
    """
    A GhostFragment is a non-actual fragment used for reporting and aggregating fragment inputs and outputs
      during traversal.
    """
    def __init__(self, parent, flow, direction):
        self._parent = parent
        self.flow = flow
        self.direction = direction

    @property
    def uuid(self):
        if self.flow.uuid is not None:
            return self.flow.uuid
        raise ValueError('should probably assign a random uuid but need a live case')

    @property
    def reference_entity(self):
        return self._parent

    @property
    def is_background(self):
        return False

    @property
    def entity_type(self):
        return 'fragment'

    @property
    def dirn(self):
        return {
            'Input': '-<-',
            'Output': '=>='
        }[self.direction]

    def top(self):
        return self._parent.top()

    def __str__(self):
        re = self.reference_entity.uuid[:7]
        return '(%s) %s %.5s %s --:   [%s] %s' % (re, self.dirn, self.uuid, self.dirn,
                                                  self.flow.unit(), self.flow['Name'])

    def __getitem__(self, item):
        return self.flow[item]
