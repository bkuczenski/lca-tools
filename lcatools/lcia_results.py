"""
This object replaces the LciaResult types spelled out in Antelope-- instead, it serializes to an LCIA result directly.

"""
from lcatools.exchanges import DissipationExchange
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
        return hash((self.exchange.flow.get_uuid(), self.factor.quantity.get_uuid()))

    def __eq__(self, other):
        if not isinstance(other, DetailedLciaResult):
            return False
        return (self.exchange.flow.get_uuid() == other.exchange.flow.get_uuid() and
                self.factor.quantity.get_uuid() == other.factor.quantity.get_uuid() and
                self.result == other.result)

    def __str__(self):
        return '%s x %-s = %-s %s' % (number(self.exchange.value), number(self.factor.value), number(self.result),
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
        self.LciaDetails.add(DetailedLciaResult(exchange, factor, location))

    def show_detailed_result(self, key=lambda x: x.result, show_all=False):
        for d in sorted(self.LciaDetails, key=key):
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

    def add_entity(self, entity):
        if entity.get_uuid() not in self._LciaScores.keys():
            self._LciaScores[entity.get_uuid()] = AggregateLciaScore(entity)

    def add_score(self, entity, exchange, factor, location):
        self.add_entity(entity)
        self._LciaScores[entity.get_uuid()].add_detailed_result(exchange, factor, location)

    def components(self):
        if self._private:
            return [None]
        return [k.entity for k in self._LciaScores.values()]

    def _header(self):
        print('%s %s' % (self.quantity, self.quantity.reference_entity.unitstring()))
        print('-' * 60)

    def show_components(self):
        self._header()
        if not self._private:
            for v in self._LciaScores.values():
                print('%s' % v)
            print('==========')
        print('%s' % self)

    def show_details(self, entity, **kwargs):
        self._header()
        if self._private:
            print('%s' % self)
        else:
            self._LciaScores[get_entity_uuid(entity)].show_detailed_result(**kwargs)

    def __add__(self, other):
        if self.quantity != other.quantity:
            raise InconsistentQuantity
        if self.scenario != other.scenario:
            raise InconsistentScenario
        s = LciaResult(self.quantity, self.scenario)
        for k, v in self._LciaScores.items():
            s._LciaScores[k] = v
        for k, v in other.LciaScores.items():
            s._LciaScores[k] = v
        return s

    def __str__(self):
        return '%s %s' % (number(self.total()), self.quantity)
