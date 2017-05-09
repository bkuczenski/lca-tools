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

INTERFACE_TYPES = {'entity', 'foreground', 'background', 'quantity'}


class NoCatalog(Exception):
    pass


class NoInterface(Exception):
    pass


class CatalogRequired(Exception):
    pass


class ForegroundRequired(Exception):
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
    """
    def __init__(self, origin, catalog=None):
        self._origin = origin
        self._catalog = catalog

    def _iface(self, itype):
        if self._catalog is None:
            raise NoCatalog
        for i in self._catalog.get_interface(self._origin, itype):
            yield i

    def _perform_query(self, itype, attrname, exc, *args, **kwargs):
        for arch in self._iface(itype):
            try:
                return getattr(arch, attrname)(*args, **kwargs)
            except NotImplemented:
                continue
            except type(exc):
                continue
        raise exc

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
        return self._perform_query('entity', 'processes', CatalogRequired('Catalog access required'), **kwargs)

    def flows(self, **kwargs):
        """
        Generate flow entities (reference quantity only)
        :param kwargs: keyword search
        :return:
        """
        return self._perform_query('entity', 'flows', CatalogRequired('Catalog access required'), **kwargs)

    def quantities(self, **kwargs):
        """
        Generate quantities
        :param kwargs: keyword search
        :return:
        """
        try:
            return self._perform_query('entity', 'quantities', CatalogRequired('Catalog access required'), **kwargs)
        except CatalogRequired:
            return self._perform_query('quantity', 'quantities', CatalogRequired('Catalog or Quantity access required'),
                                       **kwargs)

    """
    API functions- entity-specific -- get accessed by catalog ref
    entity interface
    """
    def get(self, eid):
        """
        Retrieve entity by external Id
        :param eid: an external Id
        :return:
        """
        return self._perform_query('entity', 'get', CatalogRequired('Catalog access required'), eid)

    def reference(self, eid):
        """
        Retrieve entity's reference(s)
        :param eid:
        :return:
        """
        ent = self.get(eid)
        return ent.reference_entity

    def terminate(self, flow, direction=None):
        """
        Find processes that match the given flow and have a complementary direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        return self._perform_query('entity', 'terminate', CatalogRequired('Catalog access required'),
                                   flow, direction=direction)

    def originate(self, flow, direction=None):
        """
        Find processes that match the given flow and have the same direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        return self._perform_query('entity', 'originate', CatalogRequired('Catalog access required'),
                                   flow, direction=direction)

    def mix(self, flow, direction):
        """
        Create a mixer process whose inputs are all processes that terminate the given flow and direction
        :param flow:
        :param direction:
        :return:
        """
        return self._perform_query('entity', 'mix', CatalogRequired('Catalog access required'),
                                   flow, direction)

    """
    ForegroundInterface core methods: individual processes, quantitative data.
    """
    def fetch(self, eid):
        """
        Retrieve entity by external Id, or fetch it from an archive.
        :param eid:
        :return:
        """
        return self._perform_query('foreground', 'fetch', ForegroundRequired('Foreground access required'), eid)

    def exchanges(self, process):
        """
        Retrieve process's full exchange list, without values
        :param process:
        :return:
        """
        return self._perform_query('foreground', 'exchanges', ForegroundRequired('No access to exchange data'), process)

    def exchange_values(self, process, flow, direction, termination=None):
        """
        Return a list of exchanges with values matching the specification
        :param process:
        :param flow:
        :param direction:
        :param termination: [None] if none, return all terminations
        :return:
        """
        return self._perform_query('foreground', 'exchange_values', ForegroundRequired('No access to exchange data'),
                                   process, flow, direction, termination=termination)

    def exchange_relation(self, process, ref_flow, exch_flow, direction, termination=None):
        """
        Always returns a single float.

        :param process:
        :param ref_flow:
        :param exch_flow:
        :param direction:
        :param termination:
        :return:
        """
        return self._perform_query('foreground', 'exchange_relation', ForegroundRequired('No access to exchange data'),
                                   process, ref_flow, exch_flow, direction, termination=termination)

    """
    BackgroundInterface core methods: disabled at this level; provided by use of a BackgroundManager
    """
    def foreground_flows(self, search=None):
        """

        :param search:
        :return:
        """
        return self._perform_query('background', 'foreground_flows', BackgroundRequired('No knowledge of background'),
                                   search=search)

    def background_flows(self, search=None):
        """

        :param search:
        :return:
        """
        return self._perform_query('background', 'background_flows', BackgroundRequired('No knowledge of background'),
                                   search=search)

    def exterior_flows(self, direction=None, search=None):
        """

        :param direction:
        :param search:
        :return:
        """
        return self._perform_query('background', 'exterior_flows', BackgroundRequired('No knowledge of background'),
                                   search=search)

    def cutoffs(self, direction=None, search=None):
        """

        :param direction:
        :param search:
        :return:
        """
        return self._perform_query('background', 'cutoffs', BackgroundRequired('No knowledge of background'),
                                   search=search)

    def emissions(self, direction=None, search=None):
        """

        :param direction:
        :param search:
        :return:
        """
        return self._perform_query('background', 'emissions', BackgroundRequired('No knowledge of background'),
                                   search=search)

    def foreground(self, process, ref_flow=None):
        return self._perform_query('background', 'foreground', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow)

    def ad(self, process, ref_flow=None):
        return self._perform_query('background', 'ad', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow)

    def bf(self, process, ref_flow=None):
        return self._perform_query('background', 'bf', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow)

    def lci(self, process, ref_flow=None):
        return self._perform_query('background', 'lci', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow)

    def lcia(self, process, query_qty, ref_flow=None, **kwargs):
        return self._perform_query('background', 'lcia', BackgroundRequired('No knowledge of background'),
                                   process, query_qty, ref_flow=ref_flow)

    """
    QuantityInterface
    """
    def get_quantity(self, quantity):
        """
        Retrieve a canonical quantity from a qdb
        :param quantity: external_id of quantity
        :return: quantity entity
        """
        return self._perform_query('quantity', 'get_quantity', QuantityRequired('Quantity interface required'),
                                   quantity)

    def synonyms(self, item):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment
        :param item:
        :return: list of strings
        """
        return self._perform_query('quantity', 'synonyms', QuantityRequired('Quantity interface required'), item)

    def flowables(self, quantity=None, compartment=None):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment:
        :return: list of strings
        """
        return self._perform_query('quantity', 'flowables', QuantityRequired('Quantity interface required'),
                                   quantity=quantity, compartment=compartment)

    def compartments(self, quantity=None, flowable=None):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        return self._perform_query('quantity', 'compartments', QuantityRequired('Quantity interface required'),
                                   quantity=quantity, flowable=flowable)

    def factors(self, quantity, flowable=None, compartment=None):
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
                                   quantity, flowable=flowable, compartment=compartment)

    def quantity_relation(self, ref_quantity, flowable, compartment, query_quantity, locale='GLO'):
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
                                   ref_quantity, flowable, compartment, query_quantity, locale=locale)


