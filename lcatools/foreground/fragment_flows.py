"""


"""

import uuid

from lcatools.entities import LcEntity
from lcatools.exchanges import comp_dir, ExchangeValue, AllocatedExchange
from lcatools.characterizations import Characterization
from lcatools.literate_float import LiterateFloat
from lcatools.lcia_results import LciaResult


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


def traversal_to_lcia(ffs):
    """
    This function takes in a list of fragment flow records and aggregates their ScoreCaches into a set of LciaResults.
    The function is surprisingly slow, because AggregateLciaScore objects contain sets, so there is a lot of container
    checking. (I think that's why, anyway...)
    :param ffs:
    :return: dict of quantity uuid to LciaResult -> suitable for storing directly into a new term scorecache
    """
    results = dict()
    for i in ffs:
        if not i.term.is_null:
            for q, v in i.term.score_cache_items():
                quantity = v.quantity
                if q not in results.keys():
                    results[q] = LciaResult(quantity, scenario=v.scenario)
                results[q].add_component(i.fragment.get_uuid(), entity=i.fragment)
                x = ExchangeValue(i.term.term_node.entity(), i.term.term_flow, i.term.direction, value=i.node_weight)
                try:
                    l = i.term.term_node.entity()['SpatialScope']
                except KeyError:
                    l = None
                f = Characterization(i.term.term_flow, quantity, value=i.term.score_cache(quantity).total(), location=l)
                results[q].add_score(i.fragment.get_uuid(), x, f, l)
    return results


class FlowTermination(object):
    """
    these are stored by scenario in a dict on the mainland

    A fragment can have the following types of terminations:
     * None - the termination is null- the flow enters the foreground and becomes an i/o
     * Self - the flow enters a foreground node. The node can have children but only has LCIA impacts based on
       the terminating flow, which have to be looked up in the database. fg-terminated nodes don't have scenarios
       (e.g. the scenarios are in the exchange values)
       (created with the yet-unwritten add child flow function if the node is null)
     * Process - the flow enters a process referenced by CatalogRef.  The node's LCIA impacts are fg_lcia. The
       node's children are the process's non-term intermediate exchanges. The node can also have other children.
       (created with terminate or term_from_exch)
     * Fragment - the flow enters a sub-fragment.  The sub-fragment must be traversable. The node's children are
       the fragment's non-term io flows. The node cannot have other children.  If the sub-fragment is background,
       then the background fragment flow supplants the foreground one during traversal.

    LCIA results are always cached in the terminations, and are not (by default) persistent across instantiations.
    """
    @classmethod
    def from_json(cls, catalog, fragment, j):
        if len(j) == 0:
            return cls.null(fragment)
        if j['source'] == 'foreground':
            index = 0
        else:
            index = catalog.index_for_source(j['source'])
        process_ref = catalog.ref(index, j['entityId'])
        if process_ref.entity_type == 'fragment':
            process_ref = process_ref.entity()
        term_flow = j.pop('termFlow', None)
        direction = j.pop('direction', None)
        descend = j.pop('descend', None)
        return cls(fragment, process_ref, direction=direction, term_flow=term_flow, descend=descend)

    @classmethod
    def from_exchange(cls, fragment, exchange_ref):
        return cls(fragment, exchange_ref.process_ref, direction=exchange_ref.direction,
                   term_flow=exchange_ref.exchange.flow, inbound_ev=exchange_ref.exchange.value)

    @classmethod
    def null(cls, fragment):
        return cls(fragment, None)

    def __init__(self, fragment, process_ref, direction=None, term_flow=None, descend=True, inbound_ev=None):
        self._parent = fragment
        self._process_ref = process_ref  # this is either a catalog_ref (for process) or just a fragment
        self._descend = True
        self.term_flow = None
        self._cached_ev = None
        self._score_cache = dict()
        if direction is None:
            self.direction = comp_dir(fragment.direction)
        else:
            self.direction = direction

        self.descend = descend
        self.set_term_flow(term_flow)
        self._set_inbound_ev(inbound_ev)

    def update(self, process_ref, direction=None, term_flow=None, descend=None, inbound_ev=None):
        self._process_ref = process_ref
        if direction is not None:
            self.direction = direction
        if descend is not None:
            self._descend = descend
        self.set_term_flow(term_flow)
        self._score_cache = dict()
        self._set_inbound_ev(inbound_ev)

    def matches(self, exchange):
        """
        returns True if the exchange specifies the same process and flow as the term's process_ref and term_flow
        :param exchange:
        :return:
        """
        if self.is_null:
            return False
        if self.term_node.entity_type != 'process':
            return False
        return (self._process_ref.id == exchange.process.get_uuid()) and (self.term_flow.match(exchange.flow))

    def terminates(self, exchange):
        """
        Returns True if the exchange's termination matches the term's term_node, and the flows also match, and the
        directions are complementary.
        If the exchange does not specify a termination, returns True if the flows match and directions are comp.
        :param exchange:
        :return:
        """
        if self.is_null:
            return False
        if self.term_node.entity_type != 'process':
            return False
        if self.term_flow.match(exchange.flow) and self.direction == comp_dir(exchange.direction):
            if exchange.termination is None:
                return True
            elif exchange.termination == self._process_ref.id:
                return True
        return False

    def to_exchange(self):
        if self.is_null:
            return None
        elif self.term_node.entity_type == 'fragment':
            return ExchangeValue(self.term_node, self.term_flow, self.direction, value=self._cached_ev)
        return ExchangeValue(self.term_node.entity(), self.term_flow, self.direction, value=self._cached_ev)

    @property
    def is_fg(self):
        return (not self.is_null) and (self.term_node is self._parent)

    @property
    def is_bg(self):
        return (not self.is_null) and (self.term_node.entity_type == 'fragment') and self.term_node.is_background

    @property
    def is_null(self):
        return self._process_ref is None

    @property
    def descend(self):
        return self._descend

    @descend.setter
    def descend(self, value):
        if value is None:
            return
        if isinstance(value, bool):
            self._descend = value
            self.clear_score_cache()
        else:
            raise ValueError('Descend setting must be True or False')

    def self_terminate(self, term_flow=None):
        self._process_ref = self._parent
        self.set_term_flow(term_flow)

    @property
    def index(self):
        if self._process_ref is None:
            return 0
        elif self._process_ref.entity_type == 'fragment':
            return 0
        return self._process_ref.index

    @property
    def term_node(self):
        return self._process_ref

    @property
    def flow_conversion(self):
        """
        this gets computed at query time- raises an issue about parameterization (scenario must be known?)
        # TODO: figure out flow conversion params
        :return:
        """
        ref_qty = self._parent.flow.reference_entity
        if self.term_flow.cf(ref_qty) == 0:
            raise FlowConversionError('Missing cf for %s' % ref_qty)
        return self.term_flow.convert(1.0, fr=ref_qty)

    def _set_inbound_ev(self, inbound_ev):
        if self.is_fg:
            # foreground nodes can't have inbound EVs since there is no where to serialize them
            self._cached_ev = 1.0
            return
        if inbound_ev is None:
            if self._process_ref is None:
                inbound_ev = 1.0
            elif self.term_node.entity_type == 'process':
                process = self._process_ref.fg()
                ex = next(x for x in process.exchange(self.term_flow)
                          if x in process.reference_entity)
                if isinstance(ex, AllocatedExchange):
                    inbound_ev = ex[self.term_flow]
                else:
                    inbound_ev = ex.value
            elif self.term_node.entity_type == 'fragment':
                inbound_ev = 1.0  # the inbound ev must be applied at traversal time; not known at termination time
            else:
                raise TypeError('How did we get here??? %s' % self._process_ref)
        self._cached_ev = inbound_ev

    @property
    def id(self):
        if self.is_null:
            return None
        elif self._process_ref.entity_type == 'process':
            return self._process_ref.id
        else:
            return self._process_ref.get_uuid()

    @property
    def inbound_exchange_value(self):
        return self._cached_ev

    @property
    def node_weight_multiplier(self):
        return self.flow_conversion / self.inbound_exchange_value

    def set_term_flow(self, flow):
        """
        flow must have an exchange with process ref
        :param flow:
        :return:
        """
        if self.is_null:
            flow = None
        elif self._process_ref.entity_type == 'fragment':
            # term flow must be sub-fragment's reference flow
            flow = self.term_node.flow
        else:
            if flow is None:
                flow = self._parent.flow
            try:
                next(self._process_ref.fg().exchange(flow))
            except StopIteration:
                raise MissingFlow('%s missing flow %s' % (self._process_ref, flow))
            except TypeError:
                print('Fragment: %s\nprocess_ref: %s' % (self._parent, self._process_ref))
                raise
        self.term_flow = flow

    def aggregate_subfragments(self, subfrags):
        """
        Performs an aggregation of the subfragment score caches to compute a fragment score cache. use with caution!
        :param subfrags:
        :return:
        """
        self._score_cache = traversal_to_lcia(subfrags)

    def flowdb_results(self, lcia_results):
        self._score_cache = lcia_results

    def set_score_cache(self, lcia, quantities):
        """

        :param lcia: a lambda that takes as input a process ref and a ref flow and a list of quantities, and
        returns a dict of LciaResults
        fragment LCIA results are not cached, but instead are computed on demand. we'll see if that works for highly
        nested models. we will have to cache traversals somewhere- but I think that can be done by the manager.
        :param quantities:
        :return:
        """
        if self.is_null:
            return
        if self.term_node.entity_type == 'process':
            q_run = []
            for q in quantities:
                if q.get_uuid() not in self._score_cache.keys():
                    q_run.append(q)
            if len(q_run) != 0:
                results = lcia(self.term_node, self.term_flow, q_run)
                self._score_cache.update(results)

    def score_cache(self, quantity):
        if quantity.get_uuid() in self._score_cache:
            return self._score_cache[quantity.get_uuid()]
        return None

    def score_cache_items(self):
        return self._score_cache.items()

    def lcia(self):
        for k, v in self.score_cache_items():
            print('%s' % v)

    def clear_score_cache(self):
        self._score_cache.clear()

    def serialize(self):
        if self._process_ref is None:
            return {}
        if self.index == 0:
            source = 'foreground'
        else:
            source = self._process_ref.catalog.source_for_index(self._process_ref.index)
        j = {
            'source': source,
            'entityId': self.id
        }
        if self.term_flow != self._parent.flow:
            j['termFlow'] = self.term_flow.get_uuid()
        if self.direction != comp_dir(self._parent.direction):
            j['direction'] = self.direction
        if self._descend is False:
            j['descend'] = False
        # don't serialize score cache- could, of course
        return j

    def __str__(self):
        """

        :return:
          '--:' = fragment I/O
          '-O ' = foreground node
          '-* ' = process
          '-# ' - sub-fragment
          '-B#' - terminated background
          '--C' - cut-off background
        """
        if self.is_null:
            term = '---:'  # fragment IO
        elif self.is_fg:
            term = '-O  '
        elif self.term_node.entity_type == 'process':
            term = '-*  '
        elif self.term_node.entity_type == 'fragment':
            if self.term_node.is_background:
                if self.term_node.term.is_null:
                    term = '--C '
                else:
                    term = '-B  '
            else:
                term = '-#  '
        else:
            raise TypeError('I Do not understand this term for frag %.7s' % self._parent.get_uuid())
        return term


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
        if j['parent'] is not None:
            parent = catalog[0][j['parent']]
        else:
            parent = None
        frag = cls(j['entityId'], catalog[0][j['flow']], j['direction'], parent=parent,
                   exchange_value=j['exchangeValues'].pop('0'),
                   private=j['isPrivate'],
                   balance_flow=j['isBalanceFlow'],
                   background=j['isBackground'])
        frag.observed_ev = j['exchangeValues'].pop('1')
        for i, v in j['exchangeValues'].items():
            frag.set_exchange_value(i, v)
        for tag, val in j['tags'].items():
            frag[tag] = val  # just a fragtag group of values
        return frag

    def finish_json_load(self, catalog, j):
        self.reference_entity = catalog[0][j['parent']]
        for k, v in j['terminations'].items():
            if k == 'null':
                self.term_from_json(catalog, None, v)
            else:
                self.term_from_json(catalog, k, v)

    @classmethod
    def from_exchange(cls, parent, exchange):
        """
        This method creates a child flow, positioning the parent node as the 'process' component of the exchange
        and using the exchange's 'flow' and 'direction' components to define the child flow.  If the exchange
        also includes a 'termination', then that is used to automatically terminate the child flow.
        :param parent:
        :param exchange:
        :return:
        """
        frag = cls(uuid.uuid4(), exchange.flow, exchange.direction, parent=parent, exchange_value=exchange.value,
                   Name=exchange.flow['Name'])

        if exchange.termination is not None:
            parent_term = parent.termination(None)
            term = parent_term.term_node.catalog.ref(parent_term.term_node.index, exchange.termination)
            term_flow = exchange.flow
            frag.terminate(term, flow=term_flow)
        return frag

    def __init__(self, the_uuid, flow, direction, parent=None,
                 exchange_value=1.0,
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
        self._balance_flow = False
        if balance_flow:
            self.set_balance_flow()

        self._conserved_quantity = None

        self.observed_magnitude = LiterateFloat(1.0)  # in flow's reference unit - strictly documentary

        self._exchange_values = dict()
        self._exchange_values[0] = exchange_value
        self._exchange_values[1] = LiterateFloat(0.0)  # w.r.t. parent activity level
        self._cached_unit_scores = dict()  # of quantities

        self._terminations = dict()
        self._terminations[None] = FlowTermination.null(self)

        if 'StageName' not in self._d:
            self._d['StageName'] = ''

    def entity(self):
        """
        for compat with catalog_refs
        :return:
        """
        return self

    @property
    def _parent(self):
        return self.reference_entity

    @property
    def term(self):
        return self._terminations[None]

    @property
    def dirn(self):
        return {
            'Input': '-<-',
            'Output': '=>='
        }[self.direction]

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

    def __str__(self):
        if self.reference_entity is None:
            if self.is_background:
                re = '(B) ref'
            else:
                re = ' ** ref'
        else:
            re = self.reference_entity.get_uuid()[:7]
        return '(%s) %s %s %s %s  %s' % (re, self.dirn, self.get_uuid()[:7], self.dirn, self.term, self['Name'])

    def show_tree(self, childflows, prefix=''):
        dirn = {
            'Input': '-<-',
            'Output': '=>='
        }[self.direction]

        children = [c for c in childflows(self)]
        if len(children) > 0 and self.term.is_null:
            raise InvalidParentChild('null-terminated fragment %.7s has children' % self.get_uuid())

        print('%s%s%s %.7s (%7.2g) %s' % (prefix, dirn, self.term, self.get_uuid(), self.exchange_value(0),
                                          self['Name']))
        prefix += '    | '
        for c in sorted(children, key=lambda x: (not x.term.is_null, x.term.is_bg)):
            c.show_tree(childflows, prefix=prefix)
        if len(children) > 0:
            prefix = prefix[:-3] + ' x '
            print('%s' % prefix)

    @property
    def cached_ev(self):
        return self._exchange_values[0]

    @cached_ev.setter
    def cached_ev(self, value):
        if self.cached_ev != 1.0:
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

    def to_foreground(self):
        self._background = False
        for v in self._terminations.values():
            v.clear_score_cache()

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

    def terminate(self, process_ref, scenario=None, flow=None, **kwargs):
        """
        specify a termination.  background=True: if the flow has a parent, will create a new
        :param process_ref: a process CatalogRef
        :param scenario:
        :param flow: if process_ref, specify term_flow (default fragment.flow)
        :return:
        """
        if scenario in self._terminations:
            self._terminations[scenario].update(process_ref, term_flow=flow, **kwargs)
        else:
            self._terminations[scenario] = FlowTermination(self, process_ref, term_flow=flow, **kwargs)
        if scenario is None:
            if self['StageName'] == '' and process_ref is not None:
                try:
                    self['StageName'] = process_ref['Classifications'][-1]
                except (KeyError, TypeError):
                    self['StageName'] = process_ref['Name']

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
        # print('shifting %s' % self)
        for k, v in self._terminations.items():
            # print('%s %s' % (k, v.term_node))
            if v.is_null:
                # print('Naming background cutoff flow')
                bg.terminate(None)
            else:
                # print('bg %s' % v.term_node)
                bg.terminate(v.term_node, scenario=k, flow=v.term_flow, direction=v.direction)
                bg['Name'] = '%s' % v.term_node.entity()
                self.terminate(bg, scenario=k)
        print('BG: %s' % bg)

    def node_weight(self, magnitude, scenario):
        term = self.termination(scenario)
        if term is None or term.is_null:
            return magnitude
        return magnitude * term.node_weight_multiplier

    def _cache_balance_ev(self, _balance, scenario):
        if scenario is None:
            self.observed_ev = _balance
        self.set_exchange_value(scenario, _balance)

    def fragment_lcia(self, childflows, scenario=None, observed=False):
        ffs, _ = self.traverse(childflows, 1.0, scenario, observed=observed)
        return traversal_to_lcia(ffs)

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

        :param childflows: this is a lambda that takes current frag and returns a generator of frags listing self
        as parent
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

        '''
        First handle the traversal entry
        inputs:
         _balance
         conserved_qty
         observed
         scenario
         upstream_nw

        outputs:
         own ff
         conserved_val
        '''
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

        ff = [FragmentFlow(self, magnitude, node_weight, term, conserved)]

        if term.is_null or self.is_background:
            return ff, conserved_val

        '''
        now looking forward: is our child node conserving?
        '''

        if frags_seen is None:
            frags_seen = set()
        elif self.get_uuid() in frags_seen:
            raise InvalidParentChild('Frag seeing self')
        frags_seen.add(self.get_uuid())

        if term.is_fg or term.term_node.entity_type == 'process':
            '''
            Handle foreground nodes and processes--> these can be quantity-conserving, but except for
            balancing flows the flow magnitudes are determined at the time of construction
            '''
            stock = None
            bal_f = None
            if self._conserved_quantity is not None:
                stock = self.exchange_value(scenario, observed=observed)
                if self.direction == 'Input':  # convention: inputs to self are positive
                    stock *= -1

            for f in childflows(self):
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

        else:
            '''
            handle sub-fragments, including background flows--
            for sub-fragments, the flow magnitudes are determined at the time of traversal and must be pushed out to
             child flows
            for background flows, the background ff should replace the current ff, except maintaining self as fragment
            '''
            if term.term_node.is_background:
                bg_ff, cons = term.term_node.traverse(childflows, node_weight, scenario, observed=observed)
                bg_ff[0].fragment = self
                return bg_ff, conserved_val

            # for proper subfragments, need to determine child flow magnitudes based on traversal record
            subfrag_ffs, cons = term.term_node.traverse(childflows, 1.0, scenario,
                                                        observed=observed, frags_seen=frags_seen)
            ios = [f for f in subfrag_ffs if f.term.is_null]
            subfrags = [f for f in subfrag_ffs if not f.term.is_null]

            # first, we determine subfragment activity level by adjusting for any autoconsumption
            in_ex = 1.0
            matches = [f for f in ios if f.fragment.flow == term.term_flow]
            for m in matches:
                if m.fragment.direction == term.direction:
                    in_ex -= m.magnitude
                else:
                    in_ex += m.magnitude
                ios.remove(m)

            downstream_nw = node_weight / in_ex

            # then we add the results of the subfragment, either in aggregated or disaggregated form
            if term.descend:
                # if appending, we are traversing in situ, so do scale
                print('descending')
                for i in subfrags:
                    i.scale(downstream_nw)
                ff.extend(subfrags)
            else:
                # if aggregating, we are only setting unit scores- so don't scale
                print('aggregating')
                ff[0].term.aggregate_subfragments(subfrags)

            # next we traverse our own child flows, determining the exchange values from the subfrag traversal
            for f in childflows(self):
                ev = 0.0
                matches = [j for j in ios if j.fragment.flow == f.flow]
                # exchange values are per unit- so don't scale
                for m in matches:
                    if m.direction == f.direction:
                        ev += m.magnitude
                    else:
                        ev -= m.magnitude
                    ios.remove(m)

                child_ff, cons = f.traverse(childflows, downstream_nw, scenario, observed=observed,
                                            frags_seen=frags_seen, _balance=ev)
                ff.extend(child_ff)

            # remaining un-accounted io flows are getting appended, so do scale
            for x in ios:
                x.scale(node_weight)
            ff.extend(ios)

        # if descend is true- we give back everything- otherwise we aggregate
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

    def __str__(self):
        if self.term.is_null:
            term = '--:'
        else:
            term = '-# '
        return '%.7s %10.3g [%6s] %s %s' % (self.fragment.get_uuid(), self.magnitude, self.fragment.direction,
                                            term, self.fragment['Name'])

    def to_antelope(self, fragmentID, stageID):
        pass
