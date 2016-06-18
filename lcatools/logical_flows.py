from __future__ import print_function, unicode_literals

from collections import namedtuple
from lcatools.exchanges import Exchange


class LogicalFlow(object):
    """
    A LogicalFlow is a notional "indefinite" flow that may correspond to flow instances in several catalogs.
    """
    @classmethod
    def create(cls, catalog, ref):
        logical_flow = cls(catalog)
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
        :return:
        """
        if catalog_ref in self._entities:
            raise KeyError('Entity already exists')
        self._entities.append(catalog_ref)

    def add_exchange(self, cat_ref, exch):
        """
        cat_ref should be in the flow's ref list
        :param cat_ref:
        :param exch:
        :return:
        """
        if cat_ref in self._entities:
            self._exchanges.add((cat_ref.archive, exch))

    def exchanges(self):
        for exch in sorted(self._exchanges, key=lambda x: (x[0], x[1].direction)):
            yield exch
