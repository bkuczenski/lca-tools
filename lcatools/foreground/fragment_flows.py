"""


"""

import uuid

from lcatools.entities import LcEntity
from lcatools.exchanges import comp_dir, Exchange
from lcatools.catalog import CatalogRef, ExchangeRef
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
        index = catalog.index_for_source(j['source'])
        process_ref = catalog.ref(index, j['entityId'])
        term_flow = j.pop('termFlow', None)
        direction = j.pop('direction', None)
        descend = j.pop('descend', None) or False
        return cls(fragment, process_ref, direction=direction, term_flow=term_flow, descend=descend)

    @classmethod
    def from_exchange(cls, fragment, exchange_ref):
        process_ref = exchange_ref.catalog.ref(exchange_ref.index, exchange_ref.exchange.process.get_uuid())
        return cls(fragment, process_ref, direction=exchange_ref.exchange.direction,
                   term_flow=exchange_ref.exchange.flow)

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
        self._flow_conversion = self.term_flow.convert(fr=ref_qty)

    @property
    def flow_conversion(self):
        return self._flow_conversion

    @property
    def inbound_exchange_value(self):
        return self._process_ref.entity().exchange(self.term_flow)

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

    def serialize(self):
        j = {
            'source': self._process_ref.catalog.source_for_index(self._process_ref.index),
            'entityId': self._process_ref.id
        }
        if self.term_flow != self._parent.flow:
            j['termFlow'] = self.term_flow.get_uuid()
        if self.direction != comp_dir(self._parent.direction):
            j['direction'] = self.direction
        if self._descend:
            j['descend'] = True
        # don't serialize score cache- could, of course
        return j


class LcFragment(LcEntity):
    """

    """

    _ref_field = 'parent'
    _new_fields = ['Parent', 'Flow', 'Direction', 'StageName']

    @classmethod
    def new(cls, name, *args, **kwargs):
        """
        :param name: the name of the process
        :param args: need flow and direction
        :param kwargs: parent, exchange_value, private, balance_flow, background, termination
        :return:
        """
        return cls(uuid.uuid4(), *args, Name=name, **kwargs)

    @classmethod
    def from_json(cls, catalog, j):
        foreground = catalog[0]
        if j['parent'] is not None:
            parent = foreground[j['parent']]
        else:
            parent = None
        frag = cls(j['entityId'], foreground[j['flow']], j['direction'], parent=parent,
                   exchange_value=j['exchangeValues'].pop(0),
                   private=j['isPrivate'],
                   balance_flow=j['isBalanceFlow'],
                   background=j['isBackground'])
        for i, v in j['exchangeValues'].items():
            frag.set_exchange_value(i, v)
        frag.term_from_json(catalog, None, j['terminations'].pop('null'))
        for i, t in j['terminations']:
            frag.term_from_json(catalog, i, t)
        for tag, val in j['tags'].items():
            frag[tag] = val  # just a fragtag group of values
        return frag

    @classmethod
    def from_exchange(cls, parent, exchange):
        if exchange.termination is not None:
            parent_term = parent.termination(None)
            term = parent_term.process_ref.catalog.ref(parent_term.process_ref.index, exchange.termination)
            term_flow = exchange.flow
        else:
            term = None
            term_flow = None
        return cls(uuid.uuid4(), exchange.flow, exchange.direction, parent=parent, exchange_value=exchange.value,
                   Name=exchange.flow['Name'], termination=term, term_flow=term_flow)

    def __init__(self, the_uuid, flow, direction, parent=None,
                 exchange_value=None,
                 private=False,
                 balance_flow=False,
                 background=False,
                 termination=None, term_flow=None, term_direction=None,
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
        :param termination: default termination for the fragment
        :param kwargs:
        """

        super(LcFragment, self).__init__('fragment', the_uuid, **kwargs)
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
        if termination is not None:
            self.terminate(termination, flow=term_flow, direction=term_direction)

        if 'Flow' not in self._d:
            self._d['Flow'] = flow['Name']
        if 'Direction' not in self._d:
            self._d['Direction'] = direction
        if 'StageName' not in self._d:
            self._d['StageName'] = ''

    @property
    def _parent(self):
        return self.reference_entity

    def serialize(self):
        j = super(LcFragment, self).serialize()
        j.update({
            'eXtityId': self.get_uuid(),
            'pXrent': self._parent.get_uuid(),
            'flow': self.flow.get_uuid(),
            'direction': self.direction,
            'isPrivate': self._private,
            'isBackground': self._background,
            'isBalanceFlow': self._balance_flow,
            'exchangeValues': self._exchange_values,
            'terminations': {k: v.serialize() for k, v in self._terminations.items()},
            'tags': self._d
        })
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
            re = '(**) ref'
        else:
            re = self.reference_entity.get_uuid()[:7]
        return '(%s) [%s] -- %s : %s' % (re, self.direction, self.get_uuid()[:7], self.flow['Name'])

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

    def terminate(self, ref, scenario=None, flow=None, direction=None):
        """
        specify a termination.  background=True: if the flow has a parent, will create a new
        :param ref: either a process ref or an exchange ref
        :param scenario:
        :param flow: if process_ref, specify term_flow (default fragment.flow)
        :param direction: if process_ref, specify term_direction (default comp_dir(fragment.direction))
        :return:
        """
        if scenario in self._terminations:
            raise CacheAlreadySet('This scenario has already been specified')
        if isinstance(ref, ExchangeRef):
            self._terminations[scenario] = FlowTermination.from_exchange(self, ref)
        elif isinstance(ref, CatalogRef):
            self._terminations[scenario] = FlowTermination(self, ref, term_flow=flow, direction=direction)

    def term_from_json(self, catalog, scenario, j):
        self._terminations[scenario] = FlowTermination.from_json(catalog, self, j)

    def termination(self, scenario):
        if scenario in self._terminations.keys():
            return self._terminations[scenario]
        return self._terminations[None]

    def node_weight(self, magnitude, scenario):
        term = self.termination(scenario)
        return magnitude * term.node_weight_multiplier

    # TODO:
    def traverse(self, childflows, upstream_nw, scenario,
                 observed=False, frags_seen=None, conserved_qty=None, _balance=None):
        """

        :param childflows: this is a lambda that takes current frag id and returns a child generator
          - must be provided by calling environment

        :param upstream_nw:
        :param scenario:
        :param observed:
        :param frags_seen:
        :param conserved_qty:
        :param _balance:
        :return:
        """

        if _balance is None:
            magnitude = upstream_nw * self.exchange_value(scenario, observed=observed)
        else:
            magnitude = upstream_nw * _balance

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
        ff = [FragmentFlow(self, magnitude, node_weight, self.termination(scenario).process_ref, conserved)]
        if self._background:
            return ff, conserved_val

        if frags_seen is None:
            frags_seen = []
        elif self in frags_seen:
            raise InvalidParentChild('Frag seeing self')
        frags_seen.append(self)

        '''
        now looking forward: is our child node conserving?
        '''
        stock = None
        bal_f = None
        if self._conserved_quantity is not None:
            stock = self.exchange_value(scenario, observed=observed)
            if self.direction == 'Input':  # convention: inputs to self are positive
                stock *= -1

        for f in childflows(self):
            try:
                child_ff, cons = f.traverse(childflows, magnitude, node_weight, scenario, observed=observed,
                                            frags_seen=frags_seen, conserved_qty=self._conserved_quantity)
                if cons is not None:
                    stock += cons
            except BalanceFlowError:
                bal_f = f
                child_ff = []
            ff.extend(child_ff)

        if bal_f is not None:
            bal_ff, cons = bal_f.traverse(childflows, magnitude, node_weight, scenario, observed=observed,
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
    def __init__(self, fragment, magnitude, node_weight, term_ref, is_conserved):
        self.fragment = fragment
        self.magnitude = magnitude
        self.node_weight = node_weight
        self.term_ref = term_ref
        self.is_conserved = is_conserved

    def to_antelope(self, fragmentID, stageID):
        pass
