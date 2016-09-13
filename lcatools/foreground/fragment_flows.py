"""


"""

import uuid

from lcatools.entities import LcEntity, LcFlow
from lcatools.exchanges import comp_dir, ExchangeValue, AllocatedExchange
from lcatools.characterizations import Characterization
from lcatools.literate_float import LiterateFloat
from lcatools.lcia_results import LciaResult, LciaResults
from lcatools.interact import pick_one, parse_math


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


class ScenarioConflict(Exception):
    pass


def _new_evs():
    d = dict()
    d[0] = 1.0
    d[1] = LiterateFloat(0.0)  # w.r.t. parent activity level
    return d


def traversal_to_lcia(ffs):
    """
    This function takes in a list of fragment flow records and aggregates their ScoreCaches into a set of LciaResults.
    The function is surprisingly slow, because AggregateLciaScore objects contain sets, so there is a lot of container
    checking. (I think that's why, anyway...)
    :param ffs:
    :return: dict of quantity uuid to LciaResult -> suitable for storing directly into a new term scorecache
    """
    results = LciaResults(ffs[0].fragment)
    for i in ffs:
        if not i.term.is_null:
            for q, v in i.term.score_cache_items():
                quantity = v.quantity
                if q not in results.keys():
                    results[q] = LciaResult(quantity, scenario=v.scenario)
                results[q].add_component(i.fragment.get_uuid(), entity=i)
                x = ExchangeValue(i.fragment, i.term.term_flow, i.term.direction, value=i.node_weight)
                try:
                    l = i.term.term_node.entity()['SpatialScope']
                except KeyError:
                    l = None
                value = i.term.score_cache(quantity).total()
                if i.term.direction == i.fragment.direction:
                    # if the directions collide (rather than complement), the term is getting run in reverse
                    value *= -1
                f = Characterization(i.term.term_flow, quantity, value=value, location=l)
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
        if term_flow is not None:
            term_flow = catalog.ref(index, term_flow).entity()
        direction = j.pop('direction', None)
        descend = j.pop('descend', None)
        return cls(fragment, process_ref, direction=direction, term_flow=term_flow, descend=descend)

    @classmethod
    def from_exchange_ref(cls, fragment, exchange_ref):
        return cls(fragment, exchange_ref.process_ref, direction=exchange_ref.direction,
                   term_flow=exchange_ref.exchange.flow, inbound_ev=exchange_ref.exchange.value)

    @classmethod
    def from_term(cls, fragment, term):
        return cls(fragment, term.term_node, direction=term.direction, term_flow=term.term_flow,
                   descend=term.descend, inbound_ev=term.inbound_exchange_value)

    @classmethod
    def null(cls, fragment):
        return cls(fragment, None)

    def __init__(self, fragment, process_ref, direction=None, term_flow=None, descend=True, inbound_ev=None):
        self._parent = fragment
        self._process_ref = process_ref  # this is either a catalog_ref (for process) or just a fragment
        self._descend = True
        self.term_flow = None
        self._cached_ev = 1.0
        self._score_cache = LciaResults(fragment)
        if direction is None:
            self.direction = comp_dir(fragment.direction)
        else:
            self.direction = direction

        self.descend = descend
        self.set_term_flow(term_flow)
        self._set_inbound_ev(inbound_ev)
        self.validate_flow_conversion()

    def update(self, process_ref, direction=None, term_flow=None, descend=None, inbound_ev=None):
        self._process_ref = process_ref
        if direction is not None:
            self.direction = direction
        if descend is not None:
            self._descend = descend
        self.set_term_flow(term_flow)
        self._score_cache = LciaResults(self._parent)
        self._set_inbound_ev(inbound_ev)
        self.validate_flow_conversion()

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
        if self.term_flow.match(exchange.flow) and self.direction == comp_dir(exchange.direction):
            if exchange.termination is None:
                return True
            else:
                if self.is_null:
                    return False
                if self.term_node.entity_type != 'process':
                    return False
                if exchange.termination == self._process_ref.id:
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
        self.clear_score_cache()
        self._cached_ev = 1.0

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
        flow conversion info must be saved in fragment's flow because it is guaranteed to live in foreground
        :return:
        """
        tgt_qty = self.term_flow.reference_entity
        if self._parent.flow.cf(tgt_qty) == 0:
            '''
            print('term flow')
            self.term_flow.show()
            self.term_flow.profile()
            '''
            print('\nfragment flow')
            self._parent.flow.show()
            self._parent.flow.profile()
            raise FlowConversionError('Missing cf for %s' % tgt_qty)
        return self._parent.flow.convert(1.0, to=tgt_qty)

    def validate_flow_conversion(self):
        try:
            a = self.flow_conversion
            if a == 42:
                print('you are so lucky!')
        except FlowConversionError:
            print('Flow %s ' % self._parent.flow)
            print('Provide conversion factor %s (fragment) to %s (termination)' % (self._parent.flow.unit(),
                                                                                   self.term_flow.unit()))
            cf = parse_math(input('Enter conversion factor: '))
            self._parent.flow.add_characterization(self.term_flow.reference_entity, value=cf)

            # this is surely a hack!
            if not isinstance(self._process_ref, LcFragment):
                # if it's a fragment, then its flow's quantities are already in catalog[0]
                self._process_ref.catalog[0].add(self.term_flow.reference_entity)
            # funny, it doesn't look like that bad of a hack.

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
                try:
                    ex = next(x for x in process.exchange(self.term_flow)
                              if x.direction == self.direction)
                    if isinstance(ex, AllocatedExchange):
                        inbound_ev = ex[self.term_flow]
                    else:
                        inbound_ev = ex.value
                except StopIteration:
                    inbound_ev = 1.0
            elif self.term_node.entity_type == 'fragment':
                inbound_ev = 1.0  # the inbound ev must be applied at traversal time;
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

    @property
    def unit(self):
        if isinstance(self.term_node, LcFragment):  # fg, bg, or subfragment
            return '%4g unit' % self._cached_ev
        return '%4g %s' % (self._cached_ev, self.term_flow.unit())  # process

    def set_term_flow(self, flow):
        """
        flow must have an exchange with process ref
        :param flow:
        :return:
        """
        if self.is_null:
            flow = self._parent.flow
        elif self._process_ref.entity_type == 'fragment':
            # term flow must be sub-fragment's reference flow
            flow = self.term_node.flow
        else:
            if flow is None:
                flow = self._parent.flow
            try:
                next(self._process_ref.fg().exchange(flow))
            except StopIteration:
                r_e = self._process_ref.fg().reference_entity
                if len(r_e) == 1:
                    r_e = list(r_e)[0]
                    flow = r_e.flow
                    self.direction = r_e.direction
                elif len(r_e) > 0:
                    r_e = pick_one(list(r_e))
                    flow = r_e.flow
                    self.direction = r_e.direction
                else:
                    # instead of throwing exception, just tolerate a no-reference-flow node using _parent.flow
                    pass
                    # raise MissingFlow('%s missing flow %s\nAND no reference exchange' % (self._process_ref, flow))

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
        q_run = []
        for q in quantities:
            if q.get_uuid() not in self._score_cache.keys():
                q_run.append(q)
        if len(q_run) != 0:
            if self.is_fg or self.term_node.entity_type == 'process':
                results = lcia(self.term_node, self.term_flow, q_run)
                self._score_cache.update(results)

    def score_cache(self, quantity=None):
        if quantity is None:
            return self._score_cache
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

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, FlowTermination):
            return False
        if self.is_null:
            if other.is_null:
                return True
        return (self.term_node.get_uuid() == other.term_node.get_uuid() and
                self.term_flow == other.term_flow and
                self.direction == other.direction)

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
        flow = catalog[0][j['flow']]
        if flow is None:
            flow = LcFlow(j['flow'], Name=j['tags']['Name'], Compartment=['Intermediate Flows', 'Fragments'])
            catalog.add(flow)
        frag = cls(j['entityId'], flow, j['direction'], parent=parent,
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
            if k == 'default' or k == 'null':
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

        self._exchange_values = _new_evs()
        self.cached_ev = exchange_value

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
    def index(self):
        return 0

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

    def _serialize_evs(self):
        evs = dict()
        for k, v in self._exchange_values.items():
            if k is None:
                evs["0"] = v
            elif isinstance(k, int):
                evs[str(k)] = v
            else:
                evs[k] = v
        return evs

    def _serialize_terms(self):
        terms = dict()
        for k, v in self._terminations.items():
            if k is None:
                terms['default'] = v.serialize()
            else:
                terms[k] = v.serialize()
        return terms

    def serialize(self):
        j = super(LcFragment, self).serialize()

        j.update({
            'flow': self.flow.get_uuid(),
            'direction': self.direction,
            'isPrivate': self._private,
            'isBackground': self._background,
            'isBalanceFlow': self._balance_flow,
            'exchangeValues': self._serialize_evs(),
            'terminations': self._serialize_terms(),
            'tags': self._d
        })
        for k in self._d.keys():
            j.pop(k)  # we put these together in tags
        return j

    @property
    def unit(self):
        """
        used for formatting the fragment in display
        :return:
        """
        if self.reference_entity is None:
            return '%4g %s' % (self.cached_ev, self.flow.unit())
        return self.term.unit

    def __str__(self):
        if self.reference_entity is None:
            if self.is_background:
                re = '(B) ref'
            else:
                re = ' ** ref'
        else:
            re = self.reference_entity.get_uuid()[:7]
        return '(%s) %s %.5s %s %s  [%s] %s' % (re, self.dirn, self.get_uuid(), self.dirn, self.term,
                                                self.unit, self['Name'])

    def show(self):
        print('%s' % self)
        super(LcFragment, self).show()
        evs = list(self._exchange_values.keys())
        evs.remove(0)
        evs.remove(1)
        print('Exchange values: ')
        print('%20.20s: %g' % ('Cached', self.cached_ev))
        print('%20.20s: %g' % ('Observed', self.observed_ev))
        for k in evs:
            print('%20.20s: %g' % (k, self.exchange_value(k)))
        if self._balance_flow:
            print('\nBalance flow: True (%s)' % self.flow.reference_entity)
        else:
            print('\nBalance flow: False')
        print('Terminations: ')
        print('%20s  %s' % ('Scenario', 'Termination'))
        for k, v in self._terminations.items():
            if v.term_node is self:
                print('%20.20s: %s Foreground' % (k, v))
            else:
                if v.descend:
                    desc = '     '
                else:
                    desc = '(agg)'
                print('%20.20s: %s %s %s' % (k, v, desc, v.term_node))

    def show_tree(self, childflows, prefix='', scenario=None, observed=False):
        children = [c for c in childflows(self)]
        term = self.termination(scenario)
        if len(children) > 0 and term.is_null:
            raise InvalidParentChild('null-terminated fragment %.7s has children' % self.get_uuid())

        delim = '()'
        if self.observed_ev != 0.0:
            delim = '[]'
        if not(observed and self.observed_ev == 0.0):
            # when doing the observed mode, don't print zero results
            dirn = {
                'Input': '-<-',
                'Output': '=>='
            }[self.direction]

            print('   %s%s%s %.5s %s%s%7.3g %s%s %s' % (prefix, dirn, term, self.get_uuid(),
                                                        delim[0],
                                                        self._mod(scenario),
                                                        self.exchange_value(scenario, observed=observed) or 0.0,
                                                        self.flow.unit(),
                                                        delim[1],
                                                        self['Name']))
        # print fragment reference
        latest_stage = ''
        if len(children) > 0:
            print('   %s [%s] %s' % (prefix, term.unit, self['Name']))
            prefix += '    | '
            for c in sorted(children, key=lambda x: (x['StageName'], not x.term.is_null, x.term.is_bg)):
                if c['StageName'] != latest_stage:
                    latest_stage = c['StageName']
                    print('   %s %5s Stage: %s' % (prefix, ' ', latest_stage))
                c.show_tree(childflows, prefix=prefix, scenario=scenario, observed=observed)
            prefix = prefix[:-3] + ' x '
            print('   %s' % prefix)

    @property
    def cached_ev(self):
        return self._exchange_values[0]

    @cached_ev.setter
    def cached_ev(self, value):
        if self.cached_ev != 1.0:
            raise CacheAlreadySet('Set Value: %g (new: %g)' % (self.cached_ev, value))
        self._exchange_values[0] = value

    def reset_cache(self):
        """
        this must be done explicitly
        :return:
        """
        self._exchange_values[0] = 1.0

    def scale_evs(self, factor):
        """
        needed when foregrounding terminations
        :param factor:
        :return:
        """
        for k, v in self._exchange_values.items():
            self._exchange_values[k] = v * factor

    def clear_evs(self):
        self._exchange_values = _new_evs()

    @property
    def observed_ev(self):
        return self._exchange_values[1]

    @observed_ev.setter
    def observed_ev(self, value):
        if self._balance_flow:
            print('value set by balance.')
        else:
            self._exchange_values[1] = value

    @property
    def is_background(self):
        return self._background

    @property
    def scenarios(self):
        return set(list(self._exchange_values.keys()) + list(self._terminations.keys())).difference({0, 1})

    def to_foreground(self):
        self._background = False
        for v in self._terminations.values():
            v.clear_score_cache()

    def _match_scenario_ev(self, scenario):
        match = None
        if isinstance(scenario, tuple):
            for scen in scenario:
                if scen in self._exchange_values.keys():
                    if match is not None:
                        raise ScenarioConflict('fragment: %s\nexchange value: %s, %s' % (self, scenario, match))
                    match = scen
            return match
        if scenario in self._exchange_values.keys():
            return scenario
        return None

    def _match_scenario_term(self, scenario):
        match = None
        if isinstance(scenario, tuple):
            for scen in scenario:
                if scen in self._terminations.keys():
                    if match is not None:
                        raise ScenarioConflict('fragment: %s\ntermination: %s, %s' % (self, scenario, match))
                    match = scen

            return match
        if scenario in self._terminations.keys():
            return scenario
        return None

    def exchange_value(self, scenario=None, observed=False):
        """

        :param scenario: None, a string, or a tuple of strings. If tuple, raises error if more than one match.
        :param observed:
        :return:
        """
        match = self._match_scenario_ev(scenario)
        if match is None:
            if observed or self._balance_flow:
                ev = self.observed_ev
            else:
                ev = self.cached_ev
        else:
            ev = self._exchange_values[match]
        if ev is None:
            return 0.0
        if ev == 0 and self.reference_entity is None:
            ev = self.cached_ev
        return ev

    def exchange_values(self):
        return self._exchange_values.keys()

    def _mod(self, scenario):
        if self._balance_flow:
            return '='
        match_e = self._match_scenario_ev(scenario)
        match_t = self._match_scenario_term(scenario)
        if match_e is None:
            if match_t is None:
                return ' '  # no scenario
            return '+'  # term scenario
        if match_t is None:
            return '*'  # ev scenario
        return '%'  # both scenario

    def set_exchange_value(self, scenario, value):
        if isinstance(scenario, tuple):
            raise ScenarioConflict('Set EV must specify single scenario')
        if scenario == 0 or scenario == '0':
            self.cached_ev = value
        elif scenario == 1 or scenario == '1':
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
        if child.reference_entity != self:
            raise InvalidParentChild
        if self._conserved_quantity is not None:
            print('%.5s conserving %s' % (self.get_uuid(), self._conserved_quantity))
            raise BalanceAlreadySet
        self._conserved_quantity = child.flow.reference_entity
        print('%.5s setting balance from %.5s: %s' % (self.get_uuid(), child.get_uuid(), self._conserved_quantity))

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
        if isinstance(scenario, tuple):
            raise ScenarioConflict('Set termination must specify single scenario')
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
        if isinstance(scenario, tuple):
            raise ScenarioConflict('Set termination must specify single scenario')
        if scenario in self._terminations and not self._terminations[scenario].is_null:
            raise CacheAlreadySet('This scenario has already been specified')
        self._terminations[scenario] = FlowTermination.from_exchange_ref(self, exch_ref)

    def term_from_json(self, catalog, scenario, j):
        if isinstance(scenario, tuple):
            raise ScenarioConflict('Set termination must specify single scenario')
        self._terminations[scenario] = FlowTermination.from_json(catalog, self, j)

    def term_from_term(self, term, scenario=None):
        if isinstance(scenario, tuple):
            raise ScenarioConflict('Set termination must specify single scenario')
        self._terminations[scenario] = FlowTermination.from_term(self, term)

    def termination(self, scenario=None):
        match = self._match_scenario_term(scenario)
        if match in self._terminations.keys():
            return self._terminations[match]
        # if None in self._terminations.keys():  # this should be superfluous, as match will be None
        #     return self._terminations[None]
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
        match = self._match_scenario_ev(scenario)
        if match is None:
            self._exchange_values[1] = _balance
        else:
            self.set_exchange_value(match, _balance)

    def fragment_lcia(self, childflows, scenario=None, observed=False):
        ffs = self.traversal_entry(childflows, scenario, observed=observed)
        return traversal_to_lcia(ffs)

    def io_flows(self, childflows, scenario, observed=False):
        ffs = self.traversal_entry(childflows, scenario, observed=observed)
        return [ff for ff in ffs if ff.term.is_null]

    def traversal_entry(self, childflows, scenario, observed=False):
        if self.reference_entity is None:
            in_wt = self.exchange_value(scenario, observed=observed)
        else:
            in_wt = 1.0 / self.exchange_value(scenario, observed=observed)
        ffs, _ = self.traverse(childflows, in_wt, scenario, observed=observed)
        return ffs

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
        :param scenario: string or tuple of strings
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
            if self.reference_entity is None:
                # reference fragment exchange values are inbound
                ev = 1.0 / self.exchange_value(scenario, observed=observed)
            else:
                ev = self.exchange_value(scenario, observed=observed)
        else:
            # print('%.3s %g balance' % (self.get_uuid(), _balance))
            ev = _balance
            self._cache_balance_ev(_balance, scenario)

        magnitude = upstream_nw * ev

        conserved_val = None
        conserved = False
        if conserved_qty is not None:
            if self._balance_flow:
                raise BalanceFlowError  # to be caught
            conserved_val = ev * self.flow.cf(conserved_qty)
            if conserved_val != 0:
                conserved = True
            if self.direction == 'Output':  # convention: inputs to parent are positive
                conserved_val *= -1
            # print('%.3s %g' % (self.get_uuid(), conserved_val))

        node_weight = self.node_weight(magnitude, scenario)
        term = self.termination(scenario)

        # print('%6f %6f %s' % (magnitude, node_weight, self))
        ff = [FragmentFlow(self, magnitude, node_weight, term, conserved)]

        if term.is_null or self.is_background or magnitude == 0:
            return ff, conserved_val

        '''
        now looking forward: is our child node conserving?
        '''

        if frags_seen is None:
            frags_seen = set()

        if self.reference_entity is None:
            if self.get_uuid() in frags_seen:
                raise InvalidParentChild('Frag %s seeing self\n %s' % (self.get_uuid(), '; '.join(frags_seen)))
            frags_seen.add(self.get_uuid())
        # print('Traversing %s\nfrags seen: %s\n' % (self, '; '.join(frags_seen)))

        if term.is_fg or term.term_node.entity_type == 'process':
            '''
            Handle foreground nodes and processes--> these can be quantity-conserving, but except for
            balancing flows the flow magnitudes are determined at the time of construction
            '''
            stock = None
            bal_f = None
            if self._conserved_quantity is not None:
                stock = self.flow.cf(self._conserved_quantity)
                if self.reference_entity is None:
                    # for reference nodes only, e.v. is inbound exchange-> scale balance back to real units
                    stock *= self.exchange_value(scenario, observed=observed)
                    # use repeat call to avoid double division
                if self.direction == 'Input':  # convention: inputs to self are positive
                    stock *= -1
                # print('%.3s %g inbound-balance' % (self.get_uuid(), stock))

            for f in childflows(self):
                try:
                    child_ff, cons = f.traverse(childflows, node_weight, scenario, observed=observed,
                                                frags_seen=set(frags_seen), conserved_qty=self._conserved_quantity)
                    if cons is not None:
                        stock += cons
                except BalanceFlowError:
                    bal_f = f
                    child_ff = []

                ff.extend(child_ff)

            if bal_f is not None:
                # balance reports net inflows; positive value is more coming in than out
                # if balance flow is an input, its exchange must be the negative of the balance
                # if it is an output, its exchange must equal the balance
                if bal_f.direction == 'Input':
                    stock *= -1
                bal_ff, cons = bal_f.traverse(childflows, node_weight, scenario, observed=observed,
                                              frags_seen=set(frags_seen), conserved_qty=None, _balance=stock)
                ff.extend(bal_ff)

        else:
            '''
            handle sub-fragments, including background flows--
            for sub-fragments, the flow magnitudes are determined at the time of traversal and must be pushed out to
             child flows
            for background flows, the background ff should replace the current ff, except maintaining self as fragment
            '''
            in_ex = term.term_node.exchange_value(scenario, observed=observed)

            if term.term_node.is_background:
                bg_ff, cons = term.term_node.traverse(childflows, node_weight, scenario, observed=observed)
                bg_ff[0].fragment = self
                return bg_ff, conserved_val

            # for proper subfragments, need to determine child flow magnitudes based on traversal record
            subfrag_ffs, cons = term.term_node.traverse(childflows, in_ex, scenario,
                                                        observed=observed, frags_seen=set(frags_seen))
            ios = [f for f in subfrag_ffs if f.term.is_null]
            subfrags = [f for f in subfrag_ffs if not f.term.is_null]

            # first, we determine subfragment activity level by adjusting for any autoconsumption
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
                    if m.fragment.direction == f.direction:
                        ev += m.magnitude
                    else:
                        ev -= m.magnitude
                    ios.remove(m)

                child_ff, cons = f.traverse(childflows, downstream_nw, scenario, observed=observed,
                                            frags_seen=frags_seen, _balance=ev)
                ff.extend(child_ff)

            # remaining un-accounted io flows are getting appended, so do scale
            for x in ios:
                x.scale(downstream_nw)
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
        return '%.5s  %10.3g [%6s] %s %s' % (self.fragment.get_uuid(), self.magnitude, self.fragment.direction,
                                            term, self.fragment['Name'])

    def to_antelope(self, fragmentID, stageID):
        pass
