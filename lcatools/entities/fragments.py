"""


"""

import uuid
from collections import namedtuple, defaultdict

from lcatools.entities import LcEntity, LcFlow
from lcatools.exchanges import comp_dir, ExchangeValue
from lcatools.literate_float import LiterateFloat
from lcatools.lcia_results import DetailedLciaResult, SummaryLciaResult, traversal_to_lcia
from lcatools.interact import ifinput, parse_math
from lcatools.terminations import FlowTermination
from lcatools.catalog_ref import CatalogRef, NoCatalog


GhostFrag = namedtuple('GhostFrag', ['flow', 'direction'])


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


class ScenarioConflict(Exception):
    pass


class DependentFragment(Exception):
    pass


def _new_evs():
    d = dict()
    d[0] = 1.0
    d[1] = LiterateFloat(0.0)  # w.r.t. parent activity level
    return d


class LcFragment(LcEntity):
    """

    """

    @classmethod
    def new(cls, name, *args, verbose=False, **kwargs):
        """
        :param name: the name of the fragment
        :param args: need flow and direction
        :param verbose: print when a fragment is created
        :param kwargs: parent, exchange_value, private, balance_flow, background, termination
        :return:
        """
        if verbose:
            print('LcFragment - Name: %s:' % name, 0)  # this will never run since the constructor does not set dbg
        return cls(uuid.uuid4(), *args, Name=name, **kwargs)

    _ref_field = 'parent'

    _new_fields = ['Parent', 'StageName']

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
        if j['externalId'] != j['entityId']:
            frag.external_ref = j['externalId']
        frag._exchange_values[1] = j['exchangeValues'].pop('1')
        for i, v in j['exchangeValues'].items():
            if i.find('____') >= 0:
                i = tuple(i.split('____'))
            frag._exchange_values[i] = v
            # frag.set_exchange_value(i, v)
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
        frag = cls(uuid.uuid4(), exchange.flow, exchange.direction, parent=parent,
                   exchange_value=exchange.value, Name=exchange.flow['Name'])

        if exchange.termination is not None:
            term = FlowTermination(frag, CatalogRef(exchange.flow.origin, exchange.termination, entity_type='process'),
                                   term_flow=exchange.flow)
            frag.terminate(term)
        return frag

    def __init__(self, the_uuid, flow, direction, parent=None,
                 exchange_value=1.0,
                 private=False,
                 balance_flow=False,
                 background=False,
                 termination=None,
                 **kwargs):
        """
        Required params:
        :param the_uuid: use .new(Name, ...) for a random UUID
        :param flow: an LcFlow (catalog ref doesn't cut it)
        :param direction:
        :param parent:
        :param exchange_value: auto-set- cached; can only be set once
        :param private: forces aggregation of subfragments
        :param balance_flow: if true, exch val is always ignored and calculated based on parent
        :param background: if true, fragment only returns LCIA results.
        :param kwargs:
        """

        super(LcFragment, self).__init__('fragment', the_uuid, **kwargs)
        self._child_flows = set()

        if parent is not None:
            self.set_parent(parent)

        assert flow.entity_type == 'flow'
        self.flow = flow
        self.direction = direction  # w.r.t. parent

        self._private = private
        self._background = background
        self._balance_flow = False
        if balance_flow:
            self.set_balance_flow()

        self._conserved_quantity = None

        self._exchange_values = _new_evs()
        self.cached_ev = exchange_value

        self._terminations = dict()
        if termination is None:
            self._terminations[None] = FlowTermination.null(self)
            if 'StageName' not in self._d:
                self._d['StageName'] = ''
        else:
            self._terminations[None] = FlowTermination(self, termination)
            if 'StageName' not in self._d:
                try:
                    self._d['StageName'] = termination['Name']
                except NoCatalog:
                    self._d['StageName'] = ''

        self.__dbg_threshold = -1  # higher number is more verbose

    def set_debug_threshold(self, level):
        self.__dbg_threshold = level

    def _print(self, qwer, level=1):
        if level < self.__dbg_threshold:
                print(qwer)

    def top(self):
        if self.reference_entity is None:
            return self
        return self.reference_entity.top()

    def set_parent(self, parent):
        self._set_reference(parent)
        parent.add_child(self)

    def unset_parent(self):
        self.reference_entity.remove_child(self)
        self._set_reference(None)

    def add_child(self, child):
        """
        This should only be called from the child's set_parent function
        :param child:
        :return:
        """
        if child.reference_entity is not self:
            raise InvalidParentChild('Fragment should list parent as reference entity')
        self._child_flows.add(child)

    def remove_child(self, child):
        """
        This should only be called from the child's unset_parent function
        :param child:
        :return:
        """
        if child.reference_entity is not self:
            raise InvalidParentChild('Fragment is not a child')
        self._child_flows.remove(child)

    @property
    def child_flows(self):
        for k in sorted(self._child_flows, key=lambda x: x.uuid):
            yield k

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
            elif isinstance(k, tuple):
                evs['____'.join(k)] = v  # warning! ____ is now a special secret scenario delimiter
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
            'isBalanceFlow': self.balance_flow,
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
        if self.balance_flow:
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

    def show_tree(self, prefix='', scenario=None, observed=False):
        children = [c for c in self.child_flows]
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
                c.show_tree(prefix=prefix, scenario=scenario, observed=observed)
            prefix = prefix[:-3] + ' x '
            print('   %s' % prefix)

    @property
    def cached_ev(self):
        return self._exchange_values[0] or 1.0

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

    def _check_observability(self, scenario=None):
        if self.reference_entity is None:
            return True
        elif self.balance_flow:
            self._print('observability: value set by balance.')
            return False
        elif self.reference_entity.termination(scenario).is_subfrag:
            self._print('observability: value set during traversal')
            return False
        else:
            return True

    @observed_ev.setter
    def observed_ev(self, value):
        if self._check_observability(None):
            self._exchange_values[1] = value

    def _observe(self, scenario=None, accept_all=False):
        """
        observe engine
        :param scenario:
        :param accept_all:
        :return:
        """
        if scenario is None:
            prompt = 'Observed value'
        else:
            prompt = 'Scenario value'

        print('%s' % self)
        print(' Cached EV: %6.4g\n Observed EV: %6.4g [%s]' % (self.cached_ev, self.observed_ev, self.flow.unit()))
        if scenario is None:
            string_ev = '%10g' % self.observed_ev
        else:
            string_ev = '%10g' % self.exchange_value(scenario)
            print(' Scenario EV: %s [%s]' % (string_ev,
                                             self.flow.unit()))
        if accept_all:
            val = '='
        else:
            val = ifinput('%s ("=" to use cached): ' % prompt, string_ev)

        if val != string_ev:
            if val == '=':
                new_val = self.cached_ev
            else:
                new_val = parse_math(val)
            if scenario is None:
                self.observed_ev = new_val
            else:
                self.set_exchange_value(scenario, new_val)

    def observe(self, scenario=None, accept_all=False, recurse=True):
        """
        Interactively specify the fragment's observed exchange value-
        if fragment is a balance flow or if fragment is a child of a subfragment (for the specified scenario), then
         the ev is set during traversal and may not be observed.

        :param scenario:
        :param accept_all: whether to automatically apply the cached EV to the observation
        :param recurse: whether to observe child fragments
        :return:
        """
        if self._check_observability(scenario=scenario):
            self._observe(scenario=scenario, accept_all=accept_all)

        if recurse:
            for c in self.child_flows:
                c.observe(scenario=scenario, accept_all=accept_all, recurse=True)

    @property
    def is_background(self):
        return self._background

    @property
    def scenarios(self):
        return set(list(self._exchange_values.keys()) + list(self._terminations.keys())).difference({0, 1})

    def _match_scenario_ev(self, scenario):
        match = None
        if isinstance(scenario, set):
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
        if isinstance(scenario, set):
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
        # TODO: scenarios should be sets of strings, not tuples of strings (HARD! sets are not hashable)

        :param scenario: None, a string, or a tuple of strings. If tuple, raises error if more than one match.
        :param observed:
        :return:
        """
        match = self._match_scenario_ev(scenario)
        if match is None:
            if observed or self.balance_flow:
                ev = self.observed_ev
            else:
                ev = self.cached_ev
        else:
            ev = self._exchange_values[match]
        if ev is None:
            return 0.0
        if ev == 0 and self.reference_entity is None:
            # not sure what case this addresses
            ev = self.cached_ev
        return ev

    def exchange_values(self):
        return self._exchange_values.keys()

    def _mod(self, scenario):
        """
        Returns a visual indicator that reports whether the fragment's exchange_value or termination are affected
        under a given scenario.
        :param scenario:
        :return: ' ' no effect
                 '=' balance flow
                 '+' termination affected under scenario
                 '*' exchange value affected under scenario
                 '%' both termination and exchange value affected under scenario
        """
        if self.balance_flow:
            return '='
        match_e = self._match_scenario_ev(scenario)
        match_t = self._match_scenario_term(scenario)
        if match_e is None or self.exchange_value(match_e) == self.cached_ev:
            if match_t is None:
                return ' '  # no scenario
            return '+'  # term scenario
        if match_t is None:
            return '*'  # ev scenario
        return '%'  # both scenario

    def set_exchange_value(self, scenario, value):
        """
        TODO: needs to test whether ev is set-able (i.e. not if balance_flow, not if parent is subfragment)
        :param scenario:
        :param value:
        :return:
        """
        if not self._check_observability(scenario=scenario):
            raise DependentFragment('Fragment exchange value set during traversal')
        if isinstance(scenario, tuple) or isinstance(scenario, set):
            raise ScenarioConflict('Set EV must specify single scenario')
        if scenario == 0 or scenario == '0' or scenario is None:
            self.cached_ev = value
        elif scenario == 1 or scenario == '1':
            self._exchange_values[1] = value
        else:
            self._exchange_values[scenario] = value

    @property
    def balance_flow(self):
        return self._balance_flow

    def reverse_direction(self):
        """
        Changes the direction of a fragment to its complement, and negates all stored exchange values.
        Does NOT change termination directions- since the direction of the fragment flow is arbitrary- but the
        direction of the termination is not.
        :return:
        """
        d = dict()
        for k, v in self._exchange_values.items():
            d[k] = -1 * v
        self.direction = comp_dir(self.direction)
        self._exchange_values = d

    def set_balance_flow(self):
        if self.balance_flow is False:
            self.reference_entity.set_conserved_quantity(self)
            self._balance_flow = True

    def unset_balance_flow(self):
        if self.balance_flow:
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

    def balance(self, scenario=None, observed=False):
        """
        display a balance the inputs and outputs from a fragment termination.
        :param scenario:
        :param observed:
        :return: a dict of quantities to balance magnitudes (positive = input to term node)
        """
        qs = defaultdict(float)
        if self.reference_entity is None:
            in_ex = self.exchange_value(scenario, observed=observed)
        else:
            in_ex = 1.0
        for cf in self.flow.characterizations():
            if cf.value is not None:
                if self.direction == 'Input':  # output from term
                    qs[cf.quantity] -= cf.value * in_ex
                else:
                    qs[cf.quantity] += cf.value * in_ex
        for c in self.child_flows:
            for cf in c.flow.characterizations():
                mag = c.exchange_value(scenario, observed=observed) * (cf.value or 0.0)
                if mag != 0:
                    if c.direction == 'Output':
                        qs[cf.quantity] -= mag
                    else:
                        qs[cf.quantity] += mag

        for k, v in qs.items():
            print('%10.4g %s' % (v, k))
        return qs

    def show_balance(self, quantity=None, scenario=None, observed=False):
        def _p_line(f, m, d):
            try:
                # will fail if m is None or non-number
                print(' %+10.4g  %6s  %.5s %s' % (m, d, f.get_uuid(), f['Name']))
            finally:
                pass

        if quantity is None:
            quantity = self.flow.reference_entity

        print('%s' % quantity)
        mag = self.flow.cf(quantity)
        if self.reference_entity is None:
            mag *= self.exchange_value(scenario, observed=observed)
        if self.direction == 'Input':
            mag *= -1

        net = mag

        _p_line(self, mag, comp_dir(self.direction))

        for c in sorted(self.child_flows, key=lambda x: x.direction):
            mag = c.exchange_value(scenario, observed=observed) * c.flow.cf(quantity)
            if c.direction == 'Output':
                mag *= -1
            if mag is None or mag != 0:
                _p_line(c, mag, c.direction)
            net += mag

        print('----------\n %+10.4g net' % net)

    '''
    Terminations and related functions
    '''

    def terminate(self, termination, scenario=None):
        """
        specify a termination.  background=True: if the flow has a parent, will create a new
        :param termination: a FlowTermination
        :param scenario:
        :return:
        """
        if isinstance(scenario, tuple) or isinstance(scenario, set):
            raise ScenarioConflict('Set termination must specify single scenario')
        if scenario in self._terminations:
            if not self._terminations[scenario].is_null:
                raise CacheAlreadySet('Scenario termination already set. use clear_termination()')
        self._terminations[scenario] = termination
        if scenario is None:
            if self['StageName'] == '' and not termination.is_null:
                try:
                    self['StageName'] = termination.term_node['Classifications'][-1]
                except (KeyError, TypeError):
                    self['StageName'] = termination.term_node['Name']

    def clear_termination(self, scenario=None):
        self._terminations[scenario] = FlowTermination.null(self)

    def to_foreground(self, scenario=None):
        """
        make the fragment a foreground node. This is done by setting the termination to self.  A foreground node
        may not be a background node (obv.)  Also, a foreground node has no LCIA scores.
        :param scenario:
        :return:
        """
        self._background = False  # a background fragment can't sometimes be foreground
        self.terminate(FlowTermination(self, self), scenario=scenario)

    def set_background(self):
        self._background = True

    def term_from_json(self, catalog, scenario, j):
        if isinstance(scenario, tuple):
            raise ScenarioConflict('Set termination must specify single scenario')
        self._terminations[scenario] = FlowTermination.from_json(self, catalog, scenario, j)

    def termination(self, scenario=None):
        match = self._match_scenario_term(scenario)
        if match in self._terminations.keys():
            return self._terminations[match]
        # if None in self._terminations.keys():  # this should be superfluous, as match will be None
        #     return self._terminations[None]
        return None

    def terminations(self):
        return self._terminations.keys()

    def set_child_exchanges(self, scenario=None, reset_cache=False):
        """
        Set exchange values of child flows based on inventory data for the given scenario.  The termination must be
         a foreground process.
        :param scenario: [None] for the default scenario, set observed ev
        :param reset_cache: [False] if True, for the default scenario set cached ev
        :return:
        """
        term = self.termination(scenario)
        if not term.term_node.entity_type == 'process':
            raise DependentFragment('Child flows are set during traversal')

        children = defaultdict(list)  # need to allow for differently-terminated child flows -- distinguish by term.id

        for k in self.child_flows:
            key = (k.flow, k.direction)
            children[key].append(k)
        if len(children) == 0:
            return

        for x in term.term_node.inventory(ref_flow=term.term_flow):
            key = (x.flow, x.direction)
            if key in children:
                try:
                    if len(children[key]) > 1:
                        child = next(c for c in children[key] if c.termination(scenario).id == x.termination)
                    else:
                        child = next(c for c in children[key])
                except StopIteration:
                    continue

                self._print('setting %s' % child)
                if scenario is None:
                    if reset_cache:
                        child.reset_cache()
                        child.cached_ev = x.value
                    else:
                        child.observed_ev = x.value
                child.set_exchange_value(scenario, x.value)

    def node_weight(self, magnitude, scenario):
        term = self.termination(scenario)
        if term is None or term.is_null:
            return magnitude
        return magnitude * term.node_weight_multiplier

    def _cache_balance_ev(self, _balance, scenario):
        """
        BIG FAT BUG: evs can be modified by scenarios not defined locally in the current fragment.  Ergo, checking to
        see if the fragment's ev dict has the given scenario is not sufficient- we should not be setting the
        'observed ev' when any scenario is in effect. the whole scenario tuple needs to be used. this is a cheap dict,
        after all.  For balancing + fragment child flows only.  so this needs to be thought through somewhat.
        ans- no it doesn't! if they are balance / fffc flows, then their ev is never used! the ev dict is only for
        recordkeeping!  except set_exchange_value is limited to one scenario. so- don't use it.
        :param _balance:
        :param scenario:
        :return:
        """
        # match = self._match_scenario_ev(scenario)  # !TODO:
        # if match is None:
        #     self._exchange_values[1] = _balance
        # else:
        #     self.set_exchange_value(match, _balance)
        if scenario is None:
            self._exchange_values[1] = _balance
        else:
            self._exchange_values[scenario] = _balance

    def fragment_lcia(self, scenario=None, observed=False):
        ffs = self.traversal_entry(scenario, observed=observed)
        return traversal_to_lcia(ffs)

    def io_flows(self, scenario, observed=False):
        ffs = self.traversal_entry(scenario, observed=observed)
        return [ff for ff in ffs if ff.term.is_null]

    def inventory(self, scenario=None, scale=None, observed=False):
        """
        Aggregates inputs and outputs (un-terminated flows) from a fragment; returns a list of exchanges.
        :param scenario:
        :param scale:
        :param observed:
        :return:
        """
        io_ffs = self.io_flows(scenario, observed=observed)
        if scale is not None:
            for i in io_ffs:
                i.scale(scale)

        accum = defaultdict(float)
        ent = dict()
        ev = self.exchange_value(scenario)
        if self.direction == 'Input':  # this is input to parent flow, so output to us
            ev = -ev
        accum[self.flow.get_uuid()] = ev
        for i in io_ffs:
            ent[i.fragment.flow.get_uuid()] = i.fragment.flow
            if i.fragment.direction == 'Input':
                accum[i.fragment.flow.get_uuid()] += i.magnitude
            else:
                accum[i.fragment.flow.get_uuid()] -= i.magnitude

        in_ex = accum.pop(self.flow.get_uuid())
        if in_ex * ev < 0:  # i.e. if the signs are different
            raise ValueError('Fragment requires more reference flow than it generates')
        frag_exchs = []
        for k, v in accum.items():
            val = abs(v)
            if self.reference_entity is None:
                if ev != in_ex:
                    val *= (ev / in_ex)
            if v == 0:
                continue
            elif val < 1.0e-16:
                # controversial?
                self._print('SQUASHING %s: %g' % (ent[k], v), level=3)
                continue
            elif v < 0:
                dirn = 'Output'
            else:
                dirn = 'Input'

            frag_exchs.append(ExchangeValue(self, ent[k], dirn, value=val))
        return sorted(frag_exchs, key=lambda x: x.direction)

    def traversal_entry(self, scenario, observed=False):
        if self.reference_entity is None:
            in_wt = self.exchange_value(scenario, observed=observed)
        else:
            in_wt = 1.0 / self.exchange_value(scenario, observed=observed)
        ffs, _ = self.traverse(in_wt, scenario, observed=observed)
        return ffs

    def traverse(self, upstream_nw, scenario,
                 observed=False, frags_seen=None, conserved_qty=None, _balance=None):

        """
        If the node has a non-null termination, use that; follow child flows.

        If the node's termination is null- then look for matching background fragments. If one is found, adopt its
        termination, and return.

        else: assume it is a null foreground node; follow child flows

        :param upstream_nw: upstream node weight
        :param scenario: string or tuple of strings
        :param observed: whether to use observed or cached evs (overridden by scenario specification)
        :param frags_seen: carried along to catch recursion loops
        :param conserved_qty: in case the parent node is a conservation node
        :param _balance: used when flow magnitude is determined during traversal, i.e. for balance flows and
        children of fragment nodes
        :return: an array of FragmentFlow records reporting the traversal
        """

        def _print(qwer, level=1):
            self._print(qwer, level=level)
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
            _print('%.3s %g balance' % (self.get_uuid(), _balance), level=2)
            ev = _balance
            self._cache_balance_ev(_balance, scenario)

        magnitude = upstream_nw * ev

        conserved_val = None
        conserved = False
        if conserved_qty is not None:
            if self.balance_flow:
                raise BalanceFlowError  # to be caught
            conserved_val = ev * self.flow.cf(conserved_qty)
            if conserved_val != 0:
                conserved = True
            if self.direction == 'Output':  # convention: inputs to parent are positive
                conserved_val *= -1
            _print('%.3s %g' % (self.get_uuid(), conserved_val), level=2)

        node_weight = self.node_weight(magnitude, scenario)
        term = self.termination(scenario)

        # print('%6f %6f %s' % (magnitude, node_weight, self))
        ff = [FragmentFlow(self, magnitude, ev, node_weight, term, conserved)]

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
                _print('%.3s %g inbound-balance' % (self.get_uuid(), stock), level=2)

            for f in self.child_flows:
                try:
                    child_ff, cons = f.traverse(node_weight, scenario, observed=observed,
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
                bal_ff, cons = bal_f.traverse(node_weight, scenario, observed=observed,
                                              frags_seen=set(frags_seen), conserved_qty=None, _balance=stock)
                ff.extend(bal_ff)

        else:
            '''
            handle sub-fragments, including background flows--
            for sub-fragments, the flow magnitudes are determined at the time of traversal and must be pushed out to
             child flows
            for background flows, the background ff should replace the current ff, except maintaining self as fragment
            '''

            if term.term_node.is_background:
                bg_ff, cons = term.term_node.traverse(node_weight, scenario, observed=observed)
                bg_ff[0].fragment = self
                return bg_ff, conserved_val

            """
            if target fragment is not reference flow, fragment needs to be rebased from traversal result.

            to do this:
             * follow reference entities to find the fragment's reference
             * when it comes back:
              - remove current flow from ios
              - create a new ios fragmentflow corresponding to the reference flow reversed
             * if we're aggregating, set this node's node weight to the downstream nw
            """
            if term.term_node.reference_entity is not None:
                _print('inverse traversal---')
                the_ref = term.term_node.top()
                correct_reference = True
                in_ex = the_ref.exchange_value(scenario, observed=observed)
                # If flow directions conflict, subfragment is being run in reverse
                if term.term_node.direction == self.direction:
                    in_ex *= -1
                    _print('%s\nNegating subfragments-- caution ahead!' % self, level=0)
            else:
                the_ref = term.term_node
                correct_reference = False
                # ## isn't this a repeat of above?
                in_ex = the_ref.exchange_value(scenario, observed=observed)
                # If flow directions conflict, subfragment is being run in reverse
                if term.term_node.direction != self.direction:
                    in_ex *= -1
                    _print('%s\nNegating subfragments-- caution ahead!' % self, level=0)

            # for proper subfragments, need to determine child flow magnitudes based on traversal record
            subfrag_ffs, cons = the_ref.traverse(in_ex, scenario,
                                                 observed=observed, frags_seen=set(frags_seen))
            ios = [f for f in subfrag_ffs if f.term.is_null]
            subfrags = [f for f in subfrag_ffs if not f.term.is_null]
            ref_io = None

            if correct_reference:
                for _zz in ios:
                    if _zz.fragment is term.term_node:
                        in_ex = _zz.magnitude
                        _print('%s' % _zz)
                        _print('setting in_ex = %g' % in_ex)
                ios = [_zz for _zz in ios if _zz.fragment is not term.term_node]
                ref_io = GhostFragmentFlow(the_ref.flow, comp_dir(the_ref.direction))
                ios.append(ref_io)

            # first, we determine subfragment activity level by adjusting for any autoconsumption
            matches = [f for f in ios if f.fragment.flow == term.term_flow]
            for m in matches:
                _print('+-%s' % m)
                if m.fragment.direction == term.direction:
                    in_ex -= m.magnitude
                    _print(' -= %g' % m.magnitude)
                else:
                    in_ex += m.magnitude
                    _print(' -= %g' % m.magnitude)
                ios.remove(m)

            downstream_nw = node_weight / abs(in_ex)

            # this part is uncertain [wow, this function needs cleaned up]
            if self.balance_flow:
                # need to properly log our balance flow magnitude
                _print('%.3s %g re-setting balance' % (self.get_uuid(), _balance / in_ex), level=2)
                self._cache_balance_ev(_balance / in_ex, scenario)

            # then we add the results of the subfragment, either in aggregated or disaggregated form
            if term.descend:
                # if appending, we are traversing in situ, so do scale
                _print('descending', level=0)
                for i in subfrags:
                    i.scale(downstream_nw)
                ff.extend(subfrags)
            else:
                # if aggregating, we are only setting unit scores- so don't scale
                _print('aggregating', level=0)
                ff[0].term.aggregate_subfragments(subfrags)
                ff[0].node_weight = downstream_nw

            # next we traverse our own child flows, determining the exchange values from the subfrag traversal
            for f in self.child_flows:
                ev = 0.0
                matches = [j for j in ios if j.fragment.flow == f.flow]
                # exchange values are per unit- so don't scale
                for m in matches:
                    if m is ref_io:
                        _print('found it! %s' % m)

                    if m.fragment.direction == f.direction:
                        ev += m.magnitude
                    else:
                        ev -= m.magnitude
                    ios.remove(m)

                child_ff, cons = f.traverse(downstream_nw, scenario, observed=observed,
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
    def __init__(self, fragment, magnitude, exch_val, node_weight, term, is_conserved):
        self.fragment = fragment
        self.magnitude = magnitude
        self.exch_val = exch_val
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
        return '%.5s  %10.3g [%6s] %s %s' % (self.fragment.get_uuid(), self.node_weight, self.fragment.direction,
                                             term, self.fragment['Name'])

    def __add__(self, other):
        if isinstance(other, FragmentFlow):
            if other.fragment is not self.fragment:
                raise ValueError('Fragment flows do not belong to the same fragment')
            mag = other.magnitude
            nw = other.node_weight
        elif isinstance(other, DetailedLciaResult):
            if other.exchange.process is not self.fragment:
                raise ValueError('FragmentFlow and DetailedLciaResult do not belong to the same fragment')
            nw = other.exchange.value
            mag = nw
        elif isinstance(other, SummaryLciaResult):
            if other.entity is not self.fragment:
                raise ValueError('FragmentFlow and SummaryLciaResult do not belong to the same fragment')
            nw = other.node_weight
            mag = nw
        else:
            raise TypeError("Don't know how to add type %s to FragmentFlow\n %s\n to %s" % (type(other), other, self))
        # don't check unit scores-- ?????
        new = FragmentFlow(self.fragment, self.magnitude + mag, self.exch_val, self.node_weight + nw,
                           self.term, self.is_conserved)
        return new

    def __eq__(self, other):
        if not isinstance(other, FragmentFlow):
            return False
        return self.fragment == other.fragment and self.term == other.term and self.magnitude == other.magnitude

    def __hash__(self):
        return hash(self.fragment)

    def to_antelope(self, fragmentID, stageID):
        pass


class GhostFragmentFlow(object):
    def __init__(self, flow, direction):
        self.fragment = GhostFrag(flow, direction)
        self.magnitude = 1.0
        self.term = FlowTermination.null(self.fragment)
