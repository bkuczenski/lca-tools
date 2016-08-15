"""
This object replaces the LciaResult types spelled out in Antelope-- instead, it serializes to an LCIA result directly.

"""
from lcatools.exchanges import DissipationExchange


class InconsistentQuantity(Exception):
    pass


class InconsistentScenario(Exception):
    pass


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


class LciaResult(object):
    """
    An LCIA result object contains a collection of LCIA results for a related set of entities under a common scenario.
    The exchanges and factors referenced in add_score should be stored post-scenario-lookup. (An LciaResult should be
    static)
    """
    def __init__(self, quantity, scenario=None):
        self.quantity = quantity
        self.scenario = scenario
        self.LciaScores = dict()

    def total(self):
        return sum([i.cumulative_result for i in self.LciaScores.values()])

    def add_entity(self, entity):
        if entity.get_uuid() not in self.LciaScores.keys():
            self.LciaScores[entity.get_uuid()] = AggregateLciaScore(entity)

    def add_score(self, entity, exchange, factor, location):
        self.add_entity(entity)
        self.LciaScores[entity.get_uuid()].add_detailed_result(exchange, factor, location)

    def __add__(self, other):
        if self.quantity != other.quantity:
            raise InconsistentQuantity
        if self.scenario != other.scenario:
            raise InconsistentScenario
        s = LciaResult(self.quantity, self.scenario)
        for k, v in self.LciaScores.items():
            s.LciaScores[k] = v
        for k, v in other.LciaScores.items():
            s.LciaScores[k] = v
        return s
