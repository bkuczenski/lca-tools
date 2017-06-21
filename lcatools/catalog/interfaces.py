"""
An interface is a standard way for accessing resources.  These interfaces provide different forms of information
access to LcArchive information.  The interfaces provide a basis for exposing LcArchive information over the web, and
also function as a control point for introducing access control and privacy protection.

The catalog adds a semantic reference to the physical data source referred to in the archive.  When the catalog
receives a request, it maps the semantic reference to a physical data resource, and then supplies an interface to
the resource using a subclass of this query class.

At the moment the interfaces only deal with elementary LcEntities-- but once a scenario management framework is in
place it is feasible to imagine them used for fragment access as well.
"""

INTERFACE_TYPES = {'index', 'inventory', 'background', 'quantity'}


class NoCatalog(Exception):
    pass


class NoInterface(Exception):
    pass


class EntityNotFound(Exception):
    pass


class CatalogRequired(Exception):
    pass


class InventoryRequired(Exception):
    pass


class BackgroundRequired(Exception):
    pass


class QuantityRequired(Exception):
    pass


class QueryInterface(object):
    """
    A QueryInterface is a base class that describes the signature of available catalog queries.  Subclasses of
    QueryInterface, which override the methods specified here,  are themselves used to answer the queries.  This
    reduces code duplication (all the catalog needs to do is provide interfaces) and ensures consistent signatures.

    The arguments to a query should always be text strings, not entities.  When in doubt, use the external_ref.

    The EXCEPTION is the bg_lcia routine, which works best (auto-loads characterization factors) if the query quantity
    is a catalog ref.

    The resolver performs fuzzy matching, meaning that a generic query (such as 'local.ecoinvent') will return both
    exact resources and resources with greater semantic specificity (such as 'local.ecoinvent.3.2.apos').
    All queries accept the "strict=" keyword: set to True to only accept exact matches.
    """
    def __init__(self, origin, catalog=None, debug=False):
        self._origin = origin
        self._catalog = catalog
        self._debug = debug

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

    @property
    def fetch(self):
        """
        returns the catalog's fetch method. this is wonky a bit.
        :return:
        """
        return self._catalog.fetch

    @property
    def origin(self):
        return self._origin

    def get_privacy(self, origin=None):
        if origin is None:
            return self._catalog.privacy(self._origin)
        return self._catalog.privacy(origin)

    def __str__(self):
        return '%s for %s (catalog: %s)' % (self.__class__.__name__, self.origin, self._catalog.root)

    def on_debug(self):
        self._debug = True

    def off_debug(self):
        self._debug = False

    def resolve(self, itype=INTERFACE_TYPES, strict=False):
        """
        Secure access to all known resources but do not answer any query
        :param itype: default: all interfaces
        :param strict: [False]
        :return:
        """
        for k in self._iface(itype, strict=strict):
            print('%s' % k)

    def _iface(self, itype, strict=False):
        if self._catalog is None:
            raise NoCatalog
        for i in self._catalog.get_interface(self._origin, itype):
            if strict:
                if i.origin != self.origin:
                    if self._debug:
                        print('strict skipping %s' % i)
                    continue
            if self._debug:
                print('yielding %s' % i)
            yield i

    def _perform_query(self, itype, attrname, exc, *args, strict=False, **kwargs):
        if self._debug:
            print('Performing %s query, iface %s (%s)' % (attrname, itype, self.origin))
        for arch in self._iface(itype, strict=strict):
            try:
                result = getattr(arch, attrname)(*args, **kwargs)
            except NotImplementedError:
                continue
            except type(exc):
                continue
            if result is not None:
                return result
        raise exc

    def get_item(self, external_ref, item):
        """
        access an entity's dictionary items
        :param external_ref:
        :param item:
        :return:
        """
        return self._perform_query(INTERFACE_TYPES, 'get_item', EntityNotFound('%s' % external_ref), external_ref, item)

    def get_reference(self, external_ref):
        return self._perform_query(INTERFACE_TYPES, 'get_reference', EntityNotFound('%s' % external_ref), external_ref)

    def get_uuid(self, external_ref):
        return self._perform_query(INTERFACE_TYPES, 'get_uuid', EntityNotFound('%s' % external_ref), external_ref)

    """
    CatalogInterface core methods
    These are the main tools for describing information about the contents of the archive
    """
    def processes(self, **kwargs):
        """
        Generate process entities (reference exchanges only)
        :param kwargs: keyword search
        :return:
        """
        return self._perform_query('index', 'processes', CatalogRequired('Catalog access required'), **kwargs)

    def flows(self, **kwargs):
        """
        Generate flow entities (reference quantity only)
        :param kwargs: keyword search
        :return:
        """
        return self._perform_query('index', 'flows', CatalogRequired('Catalog access required'), **kwargs)

    def quantities(self, **kwargs):
        """
        Generate quantities
        :param kwargs: keyword search
        :return:
        """
        try:
            return self._perform_query('index', 'quantities', CatalogRequired('Catalog access required'), **kwargs)
        except CatalogRequired:
            return self._perform_query('quantity', 'quantities', CatalogRequired('Catalog or Quantity access required'),
                                       **kwargs)

    def get(self, eid, **kwargs):
        """
        Retrieve entity by external Id. This will take any interface and should keep trying until it finds a match.
        If the full quantitative dataset is required, use the catalog 'fetch' method.
        :param eid: an external Id
        :return:
        """
        return self._perform_query(INTERFACE_TYPES, 'get', CatalogRequired('Catalog access required'), eid, **kwargs)

    """
    API functions- entity-specific -- get accessed by catalog ref
    index interface
    """
    def terminate(self, flow, direction=None, **kwargs):
        """
        Find processes that match the given flow and have a complementary direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        return self._perform_query('index', 'terminate', CatalogRequired('Catalog access required'),
                                   flow, direction=direction, **kwargs)

    def originate(self, flow, direction=None, **kwargs):
        """
        Find processes that match the given flow and have the same direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        return self._perform_query('index', 'originate', CatalogRequired('Catalog access required'),
                                   flow, direction=direction, **kwargs)

    def mix(self, flow, direction, **kwargs):
        """
        Create a mixer process whose inputs are all processes that terminate the given flow and direction
        :param flow:
        :param direction:
        :return:
        """
        return self._perform_query('index', 'mix', CatalogRequired('Catalog access required'),
                                   flow, direction, **kwargs)

    """
    InventoryInterface core methods: individual processes, quantitative data.
    """
    def exchanges(self, process, **kwargs):
        """
        Retrieve process's full exchange list, without values
        :param process:
        :return:
        """
        return self._perform_query('inventory', 'exchanges',
                                   InventoryRequired('No access to exchange data'), process, **kwargs)

    def exchange_values(self, process, flow, direction, termination=None, **kwargs):
        """
        Return a list of exchanges with values matching the specification
        :param process:
        :param flow:
        :param direction:
        :param termination: [None] if none, return all terminations
        :return:
        """
        return self._perform_query(['inventory', 'background'], 'exchange_values',
                                   InventoryRequired('No access to exchange data'),
                                   process, flow, direction, termination=termination, **kwargs)

    def inventory(self, process, ref_flow=None, **kwargs):
        """
        Return a list of exchanges with values. If no reference is supplied, return all unallocated exchanges, including
        reference exchanges. If a reference is supplied, return allocated (but not normalized) exchanges, excluding
        reference exchanges.
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query('inventory', 'inventory', InventoryRequired('No access to exchange data'),
                                   process, ref_flow=ref_flow, **kwargs)

    def exchange_relation(self, process, ref_flow, exch_flow, direction, termination=None, **kwargs):
        """
        Always returns a single float.

        :param process:
        :param ref_flow:
        :param exch_flow:
        :param direction:
        :param termination:
        :return:
        """
        return self._perform_query('inventory', 'exchange_relation', InventoryRequired('No access to exchange data'),
                                   process, ref_flow, exch_flow, direction, termination=termination, **kwargs)

    """
    BackgroundInterface core methods: disabled at this level; provided by use of a BackgroundManager
    """
    def foreground_flows(self, search=None, **kwargs):
        """
        Yield a list of ProductFlows, which serialize to: origin, process external ref, reference flow external ref,
          direction.

        :param search:
        :return: ProductFlows
        """
        return self._perform_query('background', 'foreground_flows', BackgroundRequired('No knowledge of background'),
                                   search=search, **kwargs)

    def background_flows(self, search=None, **kwargs):
        """
        Yield a list of ProductFlows, which serialize to: origin, process external ref, reference flow external ref,
          direction.

        :param search:
        :return: ProductFlows
        """
        return self._perform_query('background', 'background_flows', BackgroundRequired('No knowledge of background'),
                                   search=search, **kwargs)

    def exterior_flows(self, direction=None, search=None, **kwargs):
        """
        Yield a list of ExteriorFlows or cutoff flows, which serialize to flow, direction

        :param direction:
        :param search:
        :return: ExteriorFlows
        """
        return self._perform_query('background', 'exterior_flows', BackgroundRequired('No knowledge of background'),
                                   search=search, **kwargs)

    def cutoffs(self, direction=None, search=None, **kwargs):
        """
        Exterior Intermediate Flows

        :param direction:
        :param search:
        :return:
        """
        return self._perform_query('background', 'cutoffs', BackgroundRequired('No knowledge of background'),
                                   search=search, **kwargs)

    def emissions(self, direction=None, search=None, **kwargs):
        """
        Exterior Elementary Flows

        :param direction:
        :param search:
        :return:
        """
        return self._perform_query('background', 'emissions', BackgroundRequired('No knowledge of background'),
                                   search=search, **kwargs)

    def foreground(self, process, ref_flow=None, **kwargs):
        """
        Returns an ordered list of exchanges- the first being the named process + reference flow, and every successive
        one having a named termination, so that the exchanges could be linked into a fragment tree.
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query('background', 'foreground', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def is_background(self, process, ref_flow=None, **kwargs):
        """

        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        return self._perform_query('background', 'is_background', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def ad(self, process, ref_flow=None, **kwargs):
        """
        returns background dependencies as a list of exchanges
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query('background', 'ad', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def bf(self, process, ref_flow=None, **kwargs):
        """
        returns foreground emissions as a list of exchanges
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query('background', 'bf', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def lci(self, process, ref_flow=None, **kwargs):
        """
        returns aggregated LCI as a list of exchanges (privacy permitting)
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query('background', 'lci', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def bg_lcia(self, process, query_qty, ref_flow=None, **kwargs):
        """
        returns an LciaResult object, aggregated as appropriate depending on the interface's privacy level.
        :param process:
        :param query_qty: if this is a catalog ref, the Qdb will auto-load characterization factors.  If the
        characterization factors are already loaded, a string reference will suffice.
        :param ref_flow:
        :param kwargs:
        :return:
        """
        return self._perform_query('background', 'bg_lcia', BackgroundRequired('No knowledge of background'),
                                   process, query_qty, ref_flow=ref_flow, **kwargs)

    """
    QuantityInterface
    """
    def lcia_methods(self, **kwargs):
        """
        Retrieve a list of known LCIA methods.  This is going to be implementation-dependent
        :return:
        """
        return self._perform_query('quantity', 'lcia_methods', QuantityRequired('Must have quantity interface'),
                                   **kwargs)

    def get_quantity(self, quantity, **kwargs):
        """
        Retrieve a canonical quantity from a qdb
        :param quantity: external_id of quantity
        :return: quantity entity
        """
        return self._perform_query('quantity', 'get_quantity', QuantityRequired('Quantity interface required'),
                                   quantity, **kwargs)

    def synonyms(self, item, **kwargs):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment
        :param item:
        :return: list of strings
        """
        return self._perform_query('quantity', 'synonyms', QuantityRequired('Quantity interface required'), item,
                                   ** kwargs)

    def flowables(self, quantity=None, compartment=None, **kwargs):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment:
        :return: list of pairs: CAS number, name
        """
        return self._perform_query('quantity', 'flowables', QuantityRequired('Quantity interface required'),
                                   quantity=quantity, compartment=compartment, **kwargs)

    def compartments(self, quantity=None, flowable=None, **kwargs):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        return self._perform_query('quantity', 'compartments', QuantityRequired('Quantity interface required'),
                                   quantity=quantity, flowable=flowable, **kwargs)

    def factors(self, quantity, flowable=None, compartment=None, **kwargs):
        """
        Return characterization factors for the given quantity, subject to optional flowable and compartment
        filter constraints. This is ill-defined because the reference unit is not explicitly reported in current
        serialization for characterizations (it is implicit in the flow)-- but it can be added to a web service layer.
        :param quantity:
        :param flowable:
        :param compartment:
        :return:
        """
        return self._perform_query('quantity', 'factors', QuantityRequired('Quantity interface required'),
                                   quantity, flowable=flowable, compartment=compartment, **kwargs)

    def quantity_relation(self, ref_quantity, flowable, compartment, query_quantity, locale='GLO', **kwargs):
        """
        Return a single number that converts the a unit of the reference quantity into the query quantity for the
        given flowable, compartment, and locale (default 'GLO').  If no locale is found, this would be a great place
        to run a spatial best-match algorithm.
        :param ref_quantity:
        :param flowable:
        :param compartment:
        :param query_quantity:
        :param locale:
        :return:
        """
        return self._perform_query('quantity', 'quantity_relation', QuantityRequired('Quantity interface required'),
                                   ref_quantity, flowable, compartment, query_quantity, locale=locale, **kwargs)
