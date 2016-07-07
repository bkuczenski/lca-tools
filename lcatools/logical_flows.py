from __future__ import print_function, unicode_literals

from collections import namedtuple
# from lcatools.exchanges import Exchange

ExchangeRef = namedtuple('ExchangeRef', ('index', 'exchange'))
CharacterizationRef = namedtuple('FactorRef', ('index', 'characterization'))


class LogicalFlow(object):
    """
    A LogicalFlow is a notional "indefinite" flow that may correspond to flow instances in several catalogs.
    """
    @classmethod
    def create(cls, ref):
        logical_flow = cls(ref.catalog)
        logical_flow.add_ref(ref)
        return logical_flow

    def __init__(self, catalog):
        """
        just creates an empty flow
        """
        self._catalog = catalog
        self._entities = []  # an array of CatalogRef namedtuples
        self._exchanges = set()  # an unordered set of exchanges

        self.name = None

    def flows(self):
        for i in self._entities:
            yield self._catalog[i.archive][i.id]

    def add_ref(self, catalog_ref):
        """
        associates a particular entity (referenced via CatalogRef namedtuple) with the logical flow.
        Does not automatically populate the exchange list, as that is cpu-intensive.
        To do so manually, call self.pull_exchanges()
        :param catalog_ref:
        :return: bool - True if ref is new and added; False if ref already exists
        """
        if catalog_ref in self._entities:
            # print('%s' % catalog_ref)
            # raise KeyError('Entity already exists')
            return False
        if catalog_ref.entity_type() != 'flow':
            raise TypeError('Reference %s is not a flow entity!' % catalog_ref.entity_type())
        catalog_ref.validate(self._catalog)
        self._entities.append(catalog_ref)
        return True

    def add_exchange(self, cat_ref, exch):
        """
        cat_ref should be in the flow's ref list
        :param cat_ref:
        :param exch:
        :return:
        """
        assert exch.entity_type == 'exchange', 'Not an exchange!'
        if cat_ref in self._entities:
            self._exchanges.add(ExchangeRef(cat_ref.index, exch))

    def exchanges(self):
        for exch in sorted(self._exchanges, key=lambda x: (x.index, x.exchange.direction)):
            yield exch

    def characterizations(self):
        for flow in self._entities:
            for char in flow.entity().characterizations():
                yield CharacterizationRef(flow.index, char)
