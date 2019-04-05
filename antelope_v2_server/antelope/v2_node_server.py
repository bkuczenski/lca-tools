from antelope_catalog.lc_resource import LcResource


class AntelopeV2Server(object):
    def __init__(self, query, catalog=None):
        """
        Initialize this server with a query object that will be used to answer queries.  Must provide a catalog
        in order to answer LCIA queries for quantities not known to the query, or if the query does not implement
        the quantity interface.
        :param query:
        :param catalog: [None] required to resolve lcia queries
        """
        self._query = query
        self._cat = catalog

