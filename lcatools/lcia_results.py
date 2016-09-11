"""
This object replaces the LciaResult types spelled out in Antelope-- instead, it serializes to an LCIA result directly.

"""
from lcatools.exchanges import ExchangeValue, DissipationExchange
from lcatools.characterizations import Characterization
from lcatools.interfaces import to_uuid


def get_entity_uuid(item):
    if to_uuid(item) is not None:
        return item
    if hasattr(item, 'get_uuid'):
        return item.get_uuid()
    raise TypeError('Don\'t know how to get ID from %s' % type(item))


class InconsistentQuantity(Exception):
    pass


class InconsistentScenario(Exception):
    pass


class DuplicateResult(Exception):
    pass


def number(val):
    try:
        return '%10.3g' % val
    except TypeError:
        return '%10.10s' % '----'


class DetailedLciaResult(object):
    """
    Contains exchange, factor, result
    """
    def __init__(self, lc_result, exchange, factor, location):
        self.exchange = exchange
        self.factor = factor
        if location in factor.locations():
            self.location = location
        else:
            self.location = 'GLO'
        self._scale = lc_result.scale

    @property
    def flow(self):
        return self.exchange.flow.get_uuid()

    @property
    def direction(self):
        return self.exchange.direction

    @property
    def content(self):
        if isinstance(self.exchange, DissipationExchange):
            return self.exchange.content()
        return None

    @property
    def _value(self):
        if self.exchange.value is None:
            return 0.0
        return self.exchange.value * self._scale

    @property
    def result(self):
        if self.factor.is_null:
            return 0.0
        return self._value * (self.factor[self.location] or 0.0)

    def __hash__(self):
        return hash((self.exchange.process.get_uuid(), self.exchange.direction, self.factor.flow.get_uuid()))

    def __eq__(self, other):
        if not isinstance(other, DetailedLciaResult):
            return False
        return (self.exchange.process.get_uuid() == other.exchange.process.get_uuid() and
                self.factor.flow.get_uuid() == other.factor.flow.get_uuid())

    def __str__(self):
        return '%s x %-s = %-s [%s] %s' % (number(self._value), number(self.factor[self.location]),
                                           number(self.result),
                                           self.location,
                                           self.factor.flow)


class SummaryLciaResult(object):
    """
    like a DetailedLciaResult except omitting the exchange and factor information
    """
    def __init__(self, lc_result, entity, node_weight, unit_score):
        """
        :param lc_result: who "owns" you. scale report by their scale.
        entity_id must either have get_uuid() or be hashable
        :param entity: a hashable identifier
        :param node_weight: stand-in for exchange value
        :param unit_score: stand-in for factor value
        """
        self.entity = entity
        self.node_weight = node_weight
        self.unit_score = unit_score
        self._scale = lc_result.scale  # property

    @property
    def result(self):
        return self.node_weight * self.unit_score * self._scale

    def __hash__(self):
        try:
            h = self.entity.get_uuid()
        except AttributeError:
            h = self.entity
        return hash(h)

    def __eq__(self, other):
        if not isinstance(other, SummaryLciaResult):
            return False
        return self.entity == other.entity

    def __str__(self):
        return '%s x %-s = %-s %s' % (number(self.node_weight), number(self.unit_score),
                                      number(self.result),
                                      self.entity)


class AggregateLciaScore(object):
    """
    contains an entityId which should be either a process or a fragment (fragment stages show up as fragments??)
    constructed from
    """
    def __init__(self, entity):
        self.entity = entity
        self.LciaDetails = set()

    @property
    def cumulative_result(self):
        return sum([i.result for i in self.LciaDetails])

    def add_detailed_result(self, lc_result, exchange, factor, location):
        d = DetailedLciaResult(lc_result, exchange, factor, location)
        if d in self.LciaDetails:
            if factor[location] != 0:
                raise DuplicateResult('exchange: %s\n  factor: %s\nlocation: %s' % (exchange, factor, location))
        self.LciaDetails.add(d)

    def add_summary_result(self, lc_result, entity, node_weight, unit_score):
        d = SummaryLciaResult(lc_result, entity, node_weight, unit_score)
        if d in self.LciaDetails:
            raise DuplicateResult()
        self.LciaDetails.add(d)

    def show(self, **kwargs):
        self.show_detailed_result(**kwargs)

    def show_detailed_result(self, key=lambda x: x.result, show_all=False):
        for d in sorted(self.LciaDetails, key=key, reverse=True):
            if d.result != 0 or show_all:
                print('%s' % d)
        print('=' * 60)
        print('             Total score: %g ' % self.cumulative_result)

    def __str__(self):
        return '%s  %s' % (number(self.cumulative_result), self.entity)


def show_lcia(lcia_results):
    """
    Takes in a dict of uuids to lcia results, and summarizes them in a neat table
    :param lcia_results:
    :return:
    """
    print('LCIA Results\n%s' % ('-' * 60))
    for r in lcia_results.values():
        print('%10.5g %s' % (r.total(), r.quantity))


class LciaResult(object):
    """
    An LCIA result object contains a collection of LCIA results for a related set of entities under a common scenario.
    The exchanges and factors referenced in add_score should be stored post-scenario-lookup. (An LciaResult should be
    static)
    """
    @classmethod
    def from_cfs(cls, fragment, cfs, scenario=None, location=None):
        """
        for foreground-terminated elementary flows
        returns a dict of LciaResults
        :param fragment:
        :param cfs: a dict of q-uuid to cf sets???
        :param scenario:
        :param location:
        :return:
        """
        results = LciaResults(fragment)
        exch = ExchangeValue(fragment, fragment.flow, fragment.direction, value=1.0)
        for q, cf in cfs.items():
            if isinstance(cf, Characterization):
                results[q] = cls(cf.quantity, scenario=scenario)
                results[q].add_component(fragment.get_uuid(), entity=fragment)
                results[q].add_score(fragment.get_uuid(), exch, cf, location)

        return results

    def __init__(self, quantity, scenario=None, private=False, scale=1.0):
        """
        If private, the LciaResult will not return any unaggregated results
        :param quantity:
        :param scenario:
        :param private:
        """
        self.quantity = quantity
        self.scenario = scenario
        self._scale = scale
        self._LciaScores = dict()
        self._private = private

    @property
    def scale(self):
        return self._scale

    def set_scale(self, scale):
        """
        why is this a function?
        """
        self._scale = scale

    def _match_key(self, item):
        """
        whaaaaaaaaaat is going on here
        :param item:
        :return:
        """
        for k, v in self._LciaScores.items():
            if item == k:
                yield v
            elif item == v.entity:
                yield v
            elif k.startswith(item):
                yield v
            elif str(v.entity).startswith(item):
                yield v
            elif k.startswith(str(item)):
                yield v
            elif str(v.entity).startswith(str(item)):
                yield v

    def __getitem__(self, item):
        return next(self._match_key(item))

    '''
    def __getitem__(self, item):
        if isinstance(item, int):
            return
    '''

    def aggregate(self, key=lambda x: x.fragment['StageName']):
        """
        returns a new LciaResult object in which the components of the original LciaResult object are aggregated
        according to a key.  The key is a lambda expression that is applied to each AggregateLciaScore component's
        entity property (components where the lambda fails will all be grouped together).
        :param key: default: lambda x: x.fragment['StageName'] -- assuming the payload is a FragmentFlow
        :return:
        """
        agg_result = LciaResult(self.quantity, scenario=self.scenario, private=self._private, scale=self._scale)
        for v in self._LciaScores.values():
            keystring = 'other'
            try:
                keystring = key(v.entity)
            finally:
                agg_result.add_summary(keystring, v.entity, 1.0, v.cumulative_result)
        return agg_result

    def show_agg(self, **kwargs):
        self.aggregate(**kwargs).show_components()  # deliberately don't return anything- or should return grouped?

    @property
    def is_private(self):
        return self._private

    def total(self):
        return sum([i.cumulative_result for i in self._LciaScores.values()])

    def add_component(self, key, entity=None):
        if entity is None:
            entity = key
        if key not in self._LciaScores.keys():
            self._LciaScores[key] = AggregateLciaScore(entity)

    def add_score(self, key, exchange, factor, location):
        if factor.quantity.get_uuid() != self.quantity.get_uuid():
            raise InconsistentQuantity('%s\nfactor.quantity: %s\nself.quantity: %s' % (factor,
                                                                                       factor.quantity.get_uuid(),
                                                                                       self.quantity.get_uuid()))
        if key not in self._LciaScores.keys():
            self.add_component(key)
        self._LciaScores[key].add_detailed_result(self, exchange, factor, location)

    def add_summary(self, key, entity, node_weight, unit_score):
        if key not in self._LciaScores.keys():
            self.add_component(key)
        self._LciaScores[key].add_summary_result(self, entity, node_weight, unit_score)

    def keys(self):
        if self._private:
            return [None]
        return self._LciaScores.keys()

    def components(self):
        if self._private:
            return [None]
        return [k.entity for k in self._LciaScores.values()]

    def component(self, key):
        if self._private:
            return self
        return self._LciaScores[key]

    def _header(self):
        print('%s %s' % (self.quantity, self.quantity.reference_entity.unitstring()))
        print('-' * 60)
        if self._scale != 1.0:
            print('%60s: %10.4g' % ('scale', self._scale))

    def show(self):
        self._header()
        print('%s' % self)

    def show_components(self):
        self._header()
        if not self._private:
            for v in sorted(self._LciaScores.values(), key=lambda x: x.cumulative_result, reverse=True):
                print('%s' % v)
            print('==========')
        print('%s' % self)

    def show_details(self, key=None, **kwargs):
        self._header()
        if not self._private:
            if key is None:
                for e in self._LciaScores.keys():
                    self._LciaScores[e].show_detailed_result(**kwargs)
            else:
                self._LciaScores[key].show_detailed_result(**kwargs)
        print('%s' % self)

    def __add__(self, other):
        if self.quantity != other.quantity:
            raise InconsistentQuantity
        if self.scenario != other.scenario:
            raise InconsistentScenario
        s = LciaResult(self.quantity, self.scenario)
        for k, v in self._LciaScores.items():
            s._LciaScores[k] = v
        for k, v in other._LciaScores.items():
            s._LciaScores[k] = v
        return s

    def __str__(self):
        return '%s %s' % (number(self.total()), self.quantity)


class LciaResults(dict):
    """
    A dict of LciaResult objects, with some useful attachments
    """
    def __init__(self, entity, *args, **kwargs):
        super(LciaResults, self).__init__(*args, **kwargs)
        self.entity = entity
        self._scale = 1.0
        self._indices = []

    def __getitem__(self, item):
        try:
            int(item)
            return super(LciaResults, self).__getitem__(self._indices[item])
        except (ValueError, TypeError):
            return super(LciaResults, self).__getitem__(next(k for k in self.keys() if k.startswith(item)))

    def __setitem__(self, key, value):
        value.set_scale(self._scale)
        super(LciaResults, self).__setitem__(key, value)
        self._indices = list(self.keys())

    def scale(self, factor):
        if factor == self._scale:
            return
        self._scale = factor
        for k in self._indices:
            self[k].set_scale(factor)

    def show(self):
        print('LCIA Results\n%s\n%s' % (self.entity, '-' * 60))
        if self._scale != 1.0:
            print('%60s: %10.4g' % ('scale', self._scale))
        for i, q in enumerate(self._indices):
            r = self[q]
            r.set_scale(self._scale)
            print('[%2d] %.3s  %10.5g %s' % (i, q, r.total(), r.quantity))

    def clear(self):
        super(LciaResults, self).clear()
        self._indices = []

    def update(self, *args, **kwargs):
        super(LciaResults, self).update(*args, **kwargs)
        self._indices = list(self.keys())
