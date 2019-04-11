"""
Query Interface -- used to operate catalog refs
"""

from lcatools.interfaces import (IndexInterface, BackgroundInterface, InventoryInterface, QuantityInterface,
                                 EntityNotFound, IndexRequired, PropertyExists)

INTERFACE_TYPES = {'basic', 'index', 'inventory', 'background', 'quantity', 'foreground'}
READONLY_INTERFACE_TYPES = {'basic', 'index', 'inventory', 'background', 'quantity'}


class NoCatalog(Exception):
    pass


class CatalogQuery(IndexInterface, BackgroundInterface, InventoryInterface, QuantityInterface):
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
        self._debug = debug

        self._entity_cache = dict()

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
        if self._debug:
            print('Origin: %s' % self.origin)
        if self._catalog is None:
            raise NoCatalog
        for i in self._catalog.gen_interfaces(self._origin, itype, strict=strict):
            if self._debug:
                print('yielding %s' % i)
            if isinstance(i, BackgroundInterface):
                if self._debug:
                    print('Setting up background interface')
                try:
                    i.setup_bm(self)
                except StopIteration:
                    raise IndexRequired('Background engine requires index interface')
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

    def get_item(self, external_ref, item):
        """
        access an entity's dictionary items
        :param external_ref:
        :param item:
        :return:
        """
        return self._perform_query(None, 'get_item', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref, item)

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
            q_can = self._tm.get_canonical(quantity)
        except EntityNotFound:
            if self._recursing is True:
                raise ZeroDivisionError('ggggg')
            print('Missing canonical quantity-- retrieving from original source')
            self._recursing = True
            q_ext = self._perform_query('quantity', 'get_canonical', PropertyExists('mmnnh'), quantity, **kwargs)
            print('Adding locally: %s' % q_ext)
            self._tm.add_quantity(q_ext)
            self._tm.import_cfs(q_ext)
            q_can = self._tm.get_canonical(q_ext)
            print('Retrieving canonical %s' % q_can)
        return q_can

    def make_ref(self, entity):
        if entity.entity_type == 'quantity':
            entity = self.get_canonical(entity)
        return super(CatalogQuery, self).make_ref(entity)
