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
    def __init__(self, exchange, factor, location):
        self.exchange = exchange
        self.factor = factor
        if location in factor.locations():
            self.location = location
        else:
            self.location = 'GLO'

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
    def result(self):
        if self.factor.is_null:
            return 0.0
        return (self.exchange.value or 0.0) * (self.factor[self.location] or 0.0)

    def __hash__(self):
        return hash((self.exchange.process.get_uuid(), self.factor.flow.get_uuid()))

    def __eq__(self, other):
        if not isinstance(other, DetailedLciaResult):
            return False
        return (self.exchange.process.get_uuid() == other.exchange.process.get_uuid() and
                self.factor.flow.get_uuid() == other.factor.flow.get_uuid())

    def __str__(self):
        return '%s x %-s = %-s [%s] %s' % (number(self.exchange.value), number(self.factor[self.location]),
                                           number(self.result),
                                           self.location,
                                           self.factor.flow)


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

    def add_detailed_result(self, exchange, factor, location):
        d = DetailedLciaResult(exchange, factor, location)
        if d in self.LciaDetails:
            raise DuplicateResult()
        self.LciaDetails.add(d)

    def show_detailed_result(self, key=lambda x: x.result, show_all=False):
        for d in sorted(self.LciaDetails, key=key, reverse=True):
            if d.result != 0 or show_all:
                print('%s' % d)
        print('=' * 60)
        print('             Total score: %g ' % self.cumulative_result)

    def __str__(self):
        return '%s %s' % (number(self.cumulative_result), self.entity)


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
        results = dict()
        exch = ExchangeValue(fragment, fragment.flow, fragment.direction, value=1.0)
        for q, cf in cfs.items():
            results[q] = cls(cf.quantity, scenario=scenario)
            results[q].add_component(fragment.get_uuid(), entity=fragment)
            results[q].add_detailed_result(exch, cf, location)

        return results

    def __init__(self, quantity, scenario=None, private=False):
        """
        If private, the LciaResult will not return any unaggregated results
        :param quantity:
        :param scenario:
        :param private:
        """
        self.quantity = quantity
        self.scenario = scenario
        self._LciaScores = dict()
        self._private = private

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
        if factor.quantity != self.quantity:
            raise InconsistentQuantity('%s' % factor)
        if key not in self._LciaScores.keys():
            self.add_component(key)
        self._LciaScores[key].add_detailed_result(exchange, factor, location)

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





