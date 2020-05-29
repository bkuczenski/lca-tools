"""
Query Interface -- used to operate catalog refs
"""

from lcatools.interfaces import (IndexInterface, BackgroundInterface, ExchangeInterface, QuantityInterface,
                                 ForegroundInterface, EntityNotFound, IndexRequired, PropertyExists)

INTERFACE_TYPES = {'basic', 'index', 'inventory', 'background', 'quantity', 'foreground'}
READONLY_INTERFACE_TYPES = {'basic', 'index', 'inventory', 'background', 'quantity'}


class NoCatalog(Exception):
    pass


class CatalogQuery(IndexInterface, BackgroundInterface, ExchangeInterface, QuantityInterface, ForegroundInterface):
    """
    A CatalogQuery is a class that performs any supported query against a supplied catalog.
    Supported queries are defined in the lcatools.interfaces, which are all abstract.
    Implementations also subclass the abstract classes.

    This reduces code duplication (all the catalog needs to do is provide interfaces) and ensures consistent signatures.

    The arguments to a query should always be text strings, not entities.  When in doubt, use the external_ref.

    The EXCEPTION is the bg_lcia routine, which works best (auto-loads characterization factors) if the query quantity
    is a catalog ref.

    The catalog's resolver performs fuzzy matching, meaning that a generic query (such as 'local.ecoinvent') will return
    both exact resources and resources with greater semantic specificity (such as 'local.ecoinvent.3.2.apos').
    All queries accept the "strict=" keyword: set to True to only accept exact matches.
    """
    _recursing = False
    def __init__(self, origin, catalog=None, debug=False):
        self._origin = origin
        self._catalog = catalog
        self._dbg = debug

        self._entity_cache = dict()
        self._iface_cache = dict()

    @property
    def origin(self):
        return self._origin

    @property
    def _tm(self):
        return self._catalog.lcia_engine

    def cascade(self, origin):
        """
        Generate a new query for the specified origin.
        Enables the query to follow the origins of foreign objects found locally.
        :param origin:
        :return:
        """
        return self._grounded_query(origin)

    def _grounded_query(self, origin):
        if origin is None or origin == self._origin:
            return self
        return self._catalog.query(origin)

    def __str__(self):
        return '%s for %s (catalog: %s)' % (self.__class__.__name__, self.origin, self._catalog.root)

    def _iface(self, itype, strict=False):
        self._debug('Origin: %s' % self.origin)
        if self._catalog is None:
            raise NoCatalog
        if itype in self._iface_cache:
            self._debug('Returning cached iface')
            yield self._iface_cache[itype]
        for i in self._catalog.gen_interfaces(self._origin, itype, strict=strict):
            self._debug('yielding %s' % i)
            self._iface_cache[itype] = i  # only cache the most recent iface
            yield i

    def resolve(self, itype=INTERFACE_TYPES, strict=False):
        """
        Secure access to all known resources but do not answer any query
        :param itype: default: all interfaces
        :param strict: [False]
        :return:
        """
        for k in self._iface(itype, strict=strict):
            print('%s' % k)

    def get(self, eid, **kwargs):
        """
        Retrieve entity by external Id. This will take any interface and should keep trying until it finds a match.
        :param eid: an external Id
        :return:
        """
        if eid not in self._entity_cache:
            entity = self._perform_query(None, 'get', EntityNotFound('%s/%s' % (self.origin, eid)), eid,
                                         **kwargs)
            self._entity_cache[eid] = self.make_ref(entity)
        return self._entity_cache[eid]

    '''
    LCIA Support
    get_canonical(quantity)
    catch get_canonical calls to return the query from the local Qdb; fetch if absent and load its characterizations
    (using super ==> _perform_query)
    '''
    def get_canonical(self, quantity, **kwargs):
        try:
            # print('Gone canonical')
            q_can = self._tm.get_canonical(quantity)
        except EntityNotFound:
            if hasattr(quantity, 'entity_type') and quantity.entity_type == 'quantity':
                print('Missing canonical quantity-- adding to LciaDb')
                self._catalog.register_quantity_ref(quantity)
                q_can = self._tm.get_canonical(quantity)
                # print('Retrieving canonical %s' % q_can)
            else:
                raise
        return q_can

    def make_ref(self, entity):
        if entity.entity_type == 'fragment':
            # TODO: create a new ForegroundQuery to eliminate the need for this hack
            return entity  # don't make references for fragments just now
        e_ref = super(CatalogQuery, self).make_ref(entity)
        if entity.entity_type == 'quantity':
            # print('Going canonical')
            return self.get_canonical(e_ref)
        return e_ref
