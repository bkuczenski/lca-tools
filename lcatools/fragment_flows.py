from lcatools.terminations import FlowTermination
from lcatools.exchanges import comp_dir
from lcatools.lcia_results import LciaResult, DetailedLciaResult, SummaryLciaResult
from lcatools.terminations import SubFragmentAggregation

from collections import defaultdict


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
    def from_antelope_v1(cls, j, make_ref):
        """
        Need to:
         * create a termination
         * create a fragment ref
         * extract node weight
         * extract magnitude
         * extract is_conserved
        :param j: JSON-formatted fragmentflow, from a v1 .NET antelope instance.  Must be modified to include StageName
         instead of fragmentStageID
        :param make_ref: a function that accepts (external_id, e_type, reference_entity, **kwargs) and returns a ref
        :return:
        """
        fpms = j['flowPropertyMagnitudes']
        ref_mag = fpms[0]
        magnitude = ref_mag['magnitude']
        ref_qty = make_ref('flowproperties/%s' % ref_mag['flowPropertyID'], 'quantity', ref_mag['unit'])
        flow = make_ref('flows/%s' % j['flowID'], 'flow', ref_qty)
        for fpm in fpms[1:]:
            mag_qty = make_ref('flowproperties/%s' % fpm['flowPropertyID'], 'quantity', fpm['unit'])
            flow.add_characterization(mag_qty, value=fpm['magnitude'] / magnitude)
        dirn = j['direction']

        if 'parentFragmentFlowID' in j:
            parent = 'fragments/%s/fragmentflows/%s' % (j['fragmentID'], j['parentFragmentFlowID'])
            frag = GhostFragment(parent, flow, dirn)

        else:
            if 'StageName' in j:
                stage_name = j['StageName']
            else:
                stage_name = 'InputOutput'
            frag = make_ref('fragments/%s' % j['fragmentID'], 'fragment', None,
                            Name=j['name'], StageName=stage_name)
            frag.set_config(flow, dirn)

        node_type = j['nodeType']
        nw = j['nodeWeight']
        inbound_ev = magnitude / nw

        if node_type == 'Process':
            term_node = make_ref('processes/%s' % j['processID'], 'process', [])
            term = FlowTermination(frag, term_node, term_flow=flow, inbound_ev=inbound_ev)
        elif node_type == 'Fragment':
            term_node = make_ref('fragments/%s' % j['subFragmentID'], 'fragment', [])
            term = FlowTermination(frag, term_node, term_flow=flow, inbound_ev=inbound_ev)
        else:
            term = FlowTermination.null(frag)
        if 'isConserved' in j:
            conserved = j['isConserved']
        else:
            conserved = False
        return cls(frag, magnitude, nw, term, conserved)

    @classmethod
    def ref_flow(cls, parent, scenario=None, observed=False, use_ev=None):
        """

        :param parent:
        :param scenario:
        :param observed:
        :param use_ev: required to create reference flows from fragment refs
        :return:
        """
        if use_ev is None:
            use_ev = parent.exchange_value(scenario=scenario, observed=observed)
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
        self.fragment = fragment
        self.magnitude = magnitude
        self.node_weight = node_weight
        self.term = term
        self.is_conserved = is_conserved
        self._subfragments = []

    @property
    def subfragments(self):
        if self.term.is_subfrag and (self.term.descend is False):
            return self._subfragments
        return []

    def aggregate_subfragments(self, subfrags):
        self._subfragments = subfrags

    def scale(self, x):
        self.node_weight *= x
        self.magnitude *= x

    def __str__(self):
        if self.term.is_null:
            term = '--:'
            name = self.fragment.flow['Name']
        else:
            term = '-# '
            name = self.fragment['Name']
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
        FragmentFlows are equal if they have the same fragment and termination.  magnitudes are allowed to be different
        :param other:
        :return:
        """
        if not isinstance(other, FragmentFlow):
            return False
        return self.fragment == other.fragment and self.term == other.term  # and self.magnitude == other.magnitude

    def __hash__(self):
        return hash(self.fragment)

    def to_antelope(self, fragmentID, stageID):
        pass


def group_ios(parent, ios):
    """
    Utility function for dealing with a traversal result (list of FragmentFlows)
    Creates a list of cutoff flows from the inputs and outputs from a fragment traversal.
    ios is a list of FragmentFlows
    :param parent: the node generating the cutoffs
    :param ios: a list of fragment flows whose termination is Null (non-nulls ignored)
    :return: {set of grouped IO flows}, [list of internal non-null flows]
    """
    out = defaultdict(float)
    internal = []
    external = set()
    for ff in ios:
        if ff.term.is_null:
            if ff.fragment.direction == 'Input':
                magnitude = ff.magnitude
            else:
                magnitude = -ff.magnitude
            out[ff.fragment.flow] += magnitude
        else:
            internal.append(ff)
    for flow, value in out.items():
        if value < 0:
            direction = 'Output'
        else:
            direction = 'Input'
        external.add(FragmentFlow.cutoff(parent, flow, direction, abs(value)))
    return external, internal


def frag_flow_lcia(qdb, fragmentflows, quantity_ref, scenario=None, refresh=False):
    """
    Recursive function to compute LCIA of a traversal record contained in a set of Fragment Flows.
    :param qdb:
    :param fragmentflows:
    :param quantity_ref:
    :param scenario: necessary if any remote traversals are required
    :param refresh: whether to refresh the LCIA CFs
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
            v = ff.term.score_cache(quantity=quantity_ref, qdb=qdb, refresh=refresh)
        except SubFragmentAggregation:
            # if we were given interior fragments, recurse on them. otherwise ask remote.
            if len(ff.subfragments) == 0:
                v = ff.term.term_node.fragment_lcia(quantity_ref, scenario=scenario, refresh=refresh)
            else:
                v = frag_flow_lcia(qdb, ff.subfragments, quantity_ref, refresh=refresh)
        if v.total() == 0:
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
        return self.flow.uuid

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

    def __str__(self):
        re = self.reference_entity.uuid[:7]
        return '(%s) %s %.5s %s --:   [%s] %s' % (re, self.dirn, self.uuid, self.dirn,
                                                  self.flow.unit(), self.flow['Name'])
