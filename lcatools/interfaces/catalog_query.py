"""
Query Interface -- used to operate catalog refs
"""

from .iindex import IndexInterface, IndexRequired
from .ibackground import BackgroundInterface
from .iinventory import InventoryInterface
from .iquantity import QuantityInterface

INTERFACE_TYPES = {'basic', 'index', 'inventory', 'background', 'quantity', 'foreground'}
READONLY_INTERFACE_TYPES = {'basic', 'index', 'inventory', 'background', 'quantity'}


class NoCatalog(Exception):
    pass


class EntityNotFound(Exception):
    pass


class PrivateArchive(Exception):
    pass


class CatalogQuery(IndexInterface, BackgroundInterface, InventoryInterface, QuantityInterface):
    """
    A CatalogQuery is a class that performs any supported query against a supplied catalog.
    Supported queries are defined in the different kinds of interfaces, which are all abstract.
    Implementations also subclass the abstract classes.

    This reduces code duplication (all the catalog needs to do is provide interfaces) and ensures consistent signatures.

    The arguments to a query should always be text strings, not entities.  When in doubt, use the external_ref.

    The EXCEPTION is the bg_lcia routine, which works best (auto-loads characterization factors) if the query quantity
    is a catalog ref.

    The catalog's resolver performs fuzzy matching, meaning that a generic query (such as 'local.ecoinvent') will return
    both exact resources and resources with greater semantic specificity (such as 'local.ecoinvent.3.2.apos').
    All queries accept the "strict=" keyword: set to True to only accept exact matches.
    """
    def __init__(self, origin, catalog=None, debug=False):
        self._origin = origin
        self._catalog = catalog
        self._debug = debug

        self._entity_cache = dict()

    @property
    def origin(self):
        return self._origin

    def cascade(self, origin):
        if origin == self.origin:
            return self
        return self._catalog.query(origin)

    def _grounded_query(self, origin):
        if origin is None or origin == self._origin:
            return self
        return self._catalog.query(origin)

    def ensure_lcia_factors(self, quantity_ref):
        self._catalog.load_lcia_factors(quantity_ref)

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

    def is_elementary(self, obj):
        """
        accesses the catalog's qdb to detect elementary compartment. Should work on flows or exchanges.
        :param obj:
        :return:
        """
        if obj.entity_type == 'flow':
            return self._catalog.is_elementary(obj)
        elif hasattr(obj, 'flow'):
            return self._catalog.is_elementary(obj.flow)
        raise TypeError('Don\'t know how to check elementarity of this: %s' % type(obj))

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

    def get_reference(self, external_ref):
        return self._perform_query(None, 'get_reference', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref)

    def get_uuid(self, external_ref):
        return self._perform_query(None, 'get_uuid', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref)

    def get(self, eid, **kwargs):
        """
        Retrieve entity by external Id. This will take any interface and should keep trying until it finds a match.
        If the full quantitative dataset is required, use the catalog 'fetch' method.
        :param eid: an external Id
        :return:
        """
        if eid not in self._entity_cache:
            entity = self._perform_query(None, 'get', EntityNotFound('%s/%s' % (self.origin, eid)), eid,
                                         **kwargs)
            self._entity_cache[eid] = self.make_ref(entity)
        return self._entity_cache[eid]

    def do_lcia(self, inventory, quantity_ref, **kwargs):
        self.ensure_lcia_factors(quantity_ref)
        return self._catalog.qdb.do_lcia(quantity_ref, inventory, **kwargs)
