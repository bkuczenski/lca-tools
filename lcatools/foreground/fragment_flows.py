"""


"""

import uuid

from lcatools.entities import LcEntity
from lcatools.exchanges import comp_dir
from lcatools.literate_float import LiterateFloat


class InvalidParentChild(Exception):
    pass


class BalanceFlowError(Exception):
    """
    raised if a fragment attempts to traverse a balance flow the normal way
    """
    pass


class BalanceAlreadySet(Exception):
    pass


class CacheAlreadySet(Exception):
    pass


class MissingFlow(Exception):
    pass


class FlowConversionError(Exception):
    pass


class FlowTermination(object):
    """
    these are stored by scenario in a dict on the mainland

    IT IS THE FOREGROUND MANAGER'S RESPONSIBILITY to ensure that the process_ref points to a process in the
    foreground for foreground nodes, or that the term_flow lives in the foreground for background nodes

    LCIA results are always cached in the term_flow as [scenario-specific] CFs (scenario encoded in
    """
    @classmethod
    def from_json(cls, catalog, fragment, j):
        if len(j) == 0:
            return cls.null(fragment)
        index = catalog.index_for_source(j['source'])
        process_ref = catalog.ref(index, j['entityId'])
        term_flow = j.pop('termFlow', None)
        direction = j.pop('direction', None)
        descend = j.pop('descend', None) or True
        return cls(fragment, process_ref, direction=direction, term_flow=term_flow, descend=descend)

    @classmethod
    def from_exchange(cls, fragment, exchange_ref):
        return cls(fragment, exchange_ref.process_ref, direction=exchange_ref.direction,
                   term_flow=exchange_ref.exchange.flow)

    @classmethod
    def null(cls, fragment):
        return cls(fragment, None)

    def __init__(self, fragment, process_ref, direction=None, term_flow=None, descend=True):
        self._parent = fragment
        self._process_ref = process_ref
        self._descend = descend
        if term_flow is None:
            self.term_flow = fragment.flow
        else:
            self.term_flow = term_flow
        if direction is None:
            self.direction = comp_dir(fragment.direction)
        else:
            self.direction = direction

        self._score_cache = dict()

        self._set_flow_conversion()

    def _set_flow_conversion(self):
        ref_qty = self._parent.flow.reference_entity
        if self.term_flow.cf(ref_qty) == 0:
            raise FlowConversionError('Missing cf for %s' % ref_qty)
        self._flow_conversion = self.term_flow.convert(1.0, fr=ref_qty)

    @property
    def is_null(self):
        return self._process_ref is None

    @property
    def index(self):
        if self._process_ref is None:
            return 0
        return self._process_ref.index

    @property
    def term_node(self):
        return self._process_ref

    @property
    def flow_conversion(self):
        return self._flow_conversion

    @property
    def inbound_exchange_value(self):
        if self._process_ref is None:
            return 1.0
        return self._process_ref.entity().exchange(self.term_flow).value

    @property
    def node_weight_multiplier(self):
        return self.flow_conversion / self.inbound_exchange_value

    def set_term_flow(self, flow):
        """
        flow must have an exchange with process ref
        :param flow:
        :return:
        """
        try:
            self._process_ref.entity().exchange(flow)
        except StopIteration:
            raise MissingFlow('%s' % flow)
        self.term_flow = flow
        self._set_flow_conversion()

    def set_score_cache(self, quantity, lcia_result):
        self._score_cache[quantity.get_uuid()] = lcia_result

    def score_cache(self, quantity):
        return self._score_cache[quantity.get_uuid()]

    def serialize(self):
        if self._process_ref is None:
            return {}
        j = {
            'source': self._process_ref.catalog.source_for_index(self._process_ref.index),
            'entityId': self._process_ref.id
        }
        if self.term_flow != self._parent.flow:
            j['termFlow'] = self.term_flow.get_uuid()
        if self.direction != comp_dir(self._parent.direction):
            j['direction'] = self.direction
        if self._descend is False:
            j['descend'] = False
        # don't serialize score cache- could, of course
        return j


class LcFragment(LcEntity):
    """

    """

    _ref_field = 'parent'
    _new_fields = ['Parent', 'StageName']

    @classmethod
    def new(cls, name, *args, **kwargs):
        """
        :param name: the name of the fragment
        :param args: need flow and direction
        :param kwargs: parent, exchange_value, private, balance_flow, background, termination
        :return:
        """
        print('LcFragment - Name: %s:' % name)

        return cls(uuid.uuid4(), *args, Name=name, **kwargs)

    @classmethod
    def from_json(cls, catalog, j):
        foreground = catalog[0]
        if j['parent'] is not None:
            parent = foreground[j['parent']]
        else:
            parent = None
        frag = cls(j['entityId'], foreground[j['flow']], j['direction'], parent=parent,
                   exchange_value=j['exchangeValues'].pop('0'),
                   private=j['isPrivate'],
                   balance_flow=j['isBalanceFlow'],
                   background=j['isBackground'])
        frag.observed_ev = j['exchangeValues'].pop('1')
        for i, v in j['exchangeValues'].items():
            frag.set_exchange_value(i, v)
        for k, v in j['terminations'].items():
            if k == 'null':
                frag.term_from_json(catalog, None, v)
            else:
                frag.term_from_json(catalog, k, v)
        for tag, val in j['tags'].items():
            frag[tag] = val  # just a fragtag group of values
        return frag

    @classmethod
    def from_exchange(cls, parent, exchange):
        frag = cls(uuid.uuid4(), exchange.flow, exchange.direction, parent=parent, exchange_value=exchange.value,
                   Name=exchange.flow['Name'])

        if exchange.termination is not None:
            parent_term = parent.termination(None)
            term = parent_term.process_ref.catalog.ref(parent_term.process_ref.index, exchange.termination)
            term_flow = exchange.flow
            frag.terminate(term, flow=term_flow)
        return frag

    def __init__(self, the_uuid, flow, direction, parent=None,
                 exchange_value=None,
                 private=False,
                 balance_flow=False,
                 background=False,
                 **kwargs):
        """
        Required params:
        :param the_uuid: use .new(Name, ...) for a random UUID
        :param flow: an LcFlow
        :param direction:
        :param parent: auto-set
        :param exchange_value: auto-set- cached; can only be set once
        :param private: forces aggregation of subfragments
        :param balance_flow: if true, exch val is always ignored and calculated based on parent
        :param background: if true, fragment only returns LCIA results. implies parent=None; cannot be traversed
        :param kwargs:
        """

        super(LcFragment, self).__init__('fragment', the_uuid, **kwargs)
        if background:
            parent = None
            # if parent is not None:
            #     raise InvalidParentChild('Background flows are not allowed to have a parent')
        self._set_reference(parent)
        assert flow.entity_type == 'flow'
        self.flow = flow
        self.direction = direction  # w.r.t. parent

        self._private = private
        self._background = background
        self._balance_flow = balance_flow

        self._conserved_quantity = None

        self.observed_magnitude = LiterateFloat(1.0)  # in flow's reference unit

        self._exchange_values = dict()
        self._exchange_values[0] = exchange_value
        self._exchange_values[1] = LiterateFloat(1.0)  # w.r.t. parent activity level
        self._cached_unit_scores = dict()  # of quantities

        self._terminations = dict()
        self.terminate(None)

        if 'StageName' not in self._d:
            self._d['StageName'] = ''

    @property
    def _parent(self):
        return self.reference_entity

    def serialize(self):
        j = super(LcFragment, self).serialize()

        j.update({
            'flow': self.flow.get_uuid(),
            'direction': self.direction,
            'isPrivate': self._private,
            'isBackground': self._background,
            'isBalanceFlow': self._balance_flow,
            'exchangeValues': self._exchange_values,
            'terminations': {k: v.serialize() for k, v in self._terminations.items()},
            'tags': self._d
        })
        for k in self._d.keys():
            j.pop(k)  # we put these together in tags
        return j

    @property
    def cached_ev(self):
        return self._exchange_values[0]

    @cached_ev.setter
    def cached_ev(self, value):
        if self.cached_ev is not None:
            raise CacheAlreadySet('Set Value: %g (new: %g)' % (self.cached_ev, value))
        self._exchange_values[0] = value

    @property
    def observed_ev(self):
        return self._exchange_values[1]

    @observed_ev.setter
    def observed_ev(self, value):
        self._exchange_values[1] = value

    @property
    def is_background(self):
        return self._background

    def __str__(self):
        if self.reference_entity is None:
            re = ' ** ref'
        else:
            re = self.reference_entity.get_uuid()[:7]
        if self.direction == 'Input':
            dirn = '<-- '
        else:
            dirn = ' ==>'
        if self.termination(None).is_null:
            term = '--:'
        else:
            if self.is_background:
                term = '(B)'
            else:
                term = '#  '
        return '(%s) %s %s %s %s : %s' % (re, dirn, self.get_uuid()[:7], dirn, term, self.flow['Name'])

    def exchange_value(self, scenario=None, observed=False):
        """

        :param scenario:
        :param observed:
        :return:
        """
        if scenario not in self._exchange_values.keys():
            if observed:
                return self.observed_ev
            return self.cached_ev
        return self._exchange_values[scenario]

    def set_exchange_value(self, scenario, value):
        if scenario == 0:
            self.cached_ev = value
        elif scenario == 1:
            self.observed_ev = value
        else:
            self._exchange_values[scenario] = value

    def set_magnitude(self, magnitude, quantity=None):
        """
        Specify magnitude, optionally in a specified quantity. Otherwise a conversion is performed
        :param magnitude:
        :param quantity:
        :return:
        """
        if quantity is not None:
            magnitude = self.flow.convert(magnitude, fr=quantity)
        self.observed_magnitude = magnitude

    def set_balance_flow(self):
        if self._balance_flow is False:
            self.reference_entity.set_conserved_quantity(self)
            self._balance_flow = True

    def unset_balance_flow(self):
        if self._balance_flow:
            self.reference_entity.unset_conserved_quantity()
            self._balance_flow = False

    def set_conserved_quantity(self, child):
        if child.parent != self:
            raise InvalidParentChild
        if self._conserved_quantity is not None:
            raise BalanceAlreadySet
        self._conserved_quantity = child.flow.reference_entity

    def unset_conserved_quantity(self):
        self._conserved_quantity = None

    def terminate(self, process_ref, scenario=None, flow=None, direction=None):
        """
        specify a termination.  background=True: if the flow has a parent, will create a new
        :param process_ref: a process CatalogRef
        :param scenario:
        :param flow: if process_ref, specify term_flow (default fragment.flow)
        :param direction: if process_ref, specify term_direction (default comp_dir(fragment.direction))
        :return:
        """
        if scenario in self._terminations:
            raise CacheAlreadySet('This scenario has already been specified')
        self._terminations[scenario] = FlowTermination(self, process_ref, term_flow=flow, direction=direction)

    def term_from_exch(self, exch_ref, scenario=None):
        if scenario in self._terminations:
            raise CacheAlreadySet('This scenario has already been specified')
        self._terminations[scenario] = FlowTermination.from_exchange(self, exch_ref)

    def term_from_json(self, catalog, scenario, j):
        self._terminations[scenario] = FlowTermination.from_json(catalog, self, j)

    def termination(self, scenario=None):
        if scenario in self._terminations.keys():
            return self._terminations[scenario]
        if None in self._terminations.keys():
            return self._terminations[None]
        return None

    def terminations(self):
        return self._terminations.keys()

    def shift_terms_to_background(self, bg):
        for k, v in self._terminations.items():
            bg.terminate(v.term_node, scenario=k, term_flow=v.term_flow, direction=v.direction)
            self._terminations[k] = FlowTermination.null(self)

    def node_weight(self, magnitude, scenario):
        term = self.termination(scenario)
        if term is None:
            return magnitude
        return magnitude * term.node_weight_multiplier

    def _cache_balance_ev(self, _balance, scenario):
        if scenario is None:
            self.observed_ev = _balance
        self.set_exchange_value(scenario, _balance)

    def io_flows(self, childflows, scenario, observed=False, frags_seen=None):
        ffs, _ = self.traverse(childflows, 1.0, scenario, observed=observed, frags_seen=frags_seen)
        return [ff for ff in ffs if ff.term.is_null]

    def traverse(self, childflows, upstream_nw, scenario,
                 observed=False, frags_seen=None, conserved_qty=None, _balance=None):

        """
        If the node has a non-null termination, use that; follow child flows.

        If the node's termination is null- then look for matching background fragments. If one is found, adopt its
        termination, and return.

        else: assume it is a null foreground node; follow child flows

        :param childflows: this is a lambda that takes current frag (and background kwarg) and returns:
        - a child generator if background=False (default)
        - a matching background frag if one exists if background=True
        - must be provided by calling environment

        :param upstream_nw: upstream node weight
        :param scenario:
        :param observed: whether to use observed or cached evs (overridden by scenario specification)
        :param frags_seen: carried along to catch recursion loops
        :param conserved_qty: in case the parent node is a conservation node
        :param _balance: used when flow magnitude is determined during traversal, i.e. for balance flows and
        children of fragment nodes
        :return: an array of FragmentFlow records reporting the traversal
        """

        if _balance is None:
            magnitude = upstream_nw * self.exchange_value(scenario, observed=observed)
        else:
            magnitude = upstream_nw * _balance
            self._cache_balance_ev(_balance, scenario)

        conserved_val = None
        conserved = False
        if conserved_qty is not None:
            if self._balance_flow:
                raise BalanceFlowError  # to be caught
            conserved_val = self.flow.cf(conserved_qty)
            if conserved_val != 0:
                conserved = True
            if self.direction == 'Output':  # convention: inputs to parent are positive
                conserved_val *= -1

        node_weight = self.node_weight(magnitude, scenario)
        term = self.termination(scenario)

        if self.termination(scenario).is_null:
            if not self._background:
                try:
                    bg = next(childflows(self, background=True))
                    ff, _ = bg.traverse(childflows, node_weight, scenario, observed=observed)
                    return ff, conserved_val
                except StopIteration:
                    # must be foreground
                    pass

        ff = [FragmentFlow(self, magnitude, node_weight, term, conserved)]

        if self._background:
            return ff, conserved_val

        if frags_seen is None:
            frags_seen = set()
        elif self.get_uuid() in frags_seen:
            raise InvalidParentChild('Frag seeing self')
        frags_seen.add(self.get_uuid())

        '''
        now looking forward: is our child node conserving?
        '''

        if not term.is_null and term.term_node.entity_type == 'fragment':
            # need to determine child flow magnitudes based on traversal record
            flow_ffs = term.term_node.entity().io_flows(childflows, scenario,
                                                        observed=observed, frags_seen=frags_seen)

            # first, we adjust for any autoconsumption
            in_ex = 1.0
            matches = [ff for ff in flow_ffs if ff.fragment.flow == term.term_flow]
            for m in matches:
                if m.fragment.direction == term.direction:
                    in_ex -= m.magnitude
                else:
                    in_ex += m.magnitude
                flow_ffs.remove(m)

            downstream_nw = node_weight / in_ex

            for f in childflows(self, background=False):
                ev = 0.0
                matches = [ff for ff in flow_ffs if ff.flow == f.flow]
                for m in matches:
                    if m.direction == f.direction:
                        ev += m.magnitude
                    else:
                        ev -= m.magnitude
                    flow_ffs.remove(m)

                child_ff, cons = f.traverse(childflows, downstream_nw, scenario, observed=observed,
                                            frags_seen=frags_seen, _balance=ev)
                ff.extend(child_ff)

            ff.extend([x.scale(node_weight) for x in flow_ffs])  # any remaining

        else:
            stock = None
            bal_f = None
            if self._conserved_quantity is not None:
                stock = self.exchange_value(scenario, observed=observed)
                if self.direction == 'Input':  # convention: inputs to self are positive
                    stock *= -1

            for f in childflows(self, background=False):
                try:
                    child_ff, cons = f.traverse(childflows, node_weight, scenario, observed=observed,
                                                frags_seen=frags_seen, conserved_qty=self._conserved_quantity)
                    if cons is not None:
                        stock += cons
                except BalanceFlowError:
                    bal_f = f
                    child_ff = []

                ff.extend(child_ff)

            if bal_f is not None:
                bal_ff, cons = bal_f.traverse(childflows, node_weight, scenario, observed=observed,
                                              frags_seen=frags_seen, conserved_qty=None, _balance=stock)
                ff.extend(bal_ff)

        return ff, conserved_val


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
    def __init__(self, fragment, magnitude, node_weight, term, is_conserved):
        self.fragment = fragment
        self.magnitude = magnitude
        self.node_weight = node_weight
        self.term = term
        self.is_conserved = is_conserved

    def scale(self, x):
        self.node_weight *= x
        self.magnitude *= x

    def to_antelope(self, fragmentID, stageID):
        pass
