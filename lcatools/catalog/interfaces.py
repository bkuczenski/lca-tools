"""
An interface is a standard way for accessing resources.  These interfaces provide three levels of information
access to LcArchive information.  The interfaces provide a basis for exposing LcArchive information over the web, and
also function as a control point for introducing access control and privacy protection.

The interface adds a semantic reference to the physical data source referred to in the archive.  When the catalog
interface returns an entity, it translates the entity's physical origin to be semantic origin.  This also has the
effect of abstracting the upstream mechanism in the archives.  In other words: upstreaming is for combining physical
sources within the same semantic context, not for mixing semantic sources (you need a catalog for that).

At the moment the interfaces only deal with elementary LcEntities-- but once a scenario management framework is in
place it is feasible to imagine them used for fragment access as well.
"""
class CatalogRequired(Exception):
    pass


class ForegroundRequired(Exception):
    pass


class BackgroundRequired(Exception):
    pass


class QueryInterface(object):
    """
    A QueryInterface is an abstract base class that describes the signature of available catalog queries.
    """
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
        raise CatalogRequired('Catalog access required')

    def flows(self, **kwargs):
        """
        Generate flow entities (reference quantity only)
        :param kwargs: keyword search
        :return:
        """
        raise CatalogRequired('Catalog access required')

    def quantities(self, **kwargs):
        """
        Generate quantities
        :param kwargs: keyword search
        :return:
        """
        raise CatalogRequired('Catalog access required')

    def get(self, eid):
        """
        Retrieve entity by external Id
        :param eid:
        :return:
        """
        raise CatalogRequired('Catalog access required')

    def reference(self, eid):
        """
        Retrieve entity's reference(s)
        :param eid:
        :return:
        """
        raise CatalogRequired('Catalog access required')

    def terminate(self, flow, direction=None):
        """
        Find processes that match the given flow and have a complementary direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        raise CatalogRequired('Catalog access required')

    def originate(self, flow, direction=None):
        """
        Find processes that match the given flow and have the same direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        raise CatalogRequired('Catalog access required')

    def mix(self, flow, direction):
        """
        Create a mixer process whose inputs are all processes that terminate the given flow and direction
        :param flow:
        :param direction:
        :return:
        """
        raise CatalogRequired('Catalog access required')

    """
    ForegroundInterface core methods: disabled at this level
    """
    def exchanges(self, process):
        """
        Retrieve process's full exchange list, without values
        :param process:
        :return:
        """
        raise ForegroundRequired('No access to exchange data')

    def exchange_value(self, process, flow, direction, termination=None):
        """
        Return a list of exchanges matching the specification
        :param process:
        :param flow:
        :param direction:
        :param termination: [None] if none, return all terminations
        :return:
        """
        raise ForegroundRequired('No access to exchange data')

    def exchange_relation(self, process, ref_flow, exch_flow, direction, termination=None):
        """

        :param process:
        :param ref_flow:
        :param exch_flow:
        :param direction:
        :param termination:
        :return:
        """
        raise ForegroundRequired('No access to exchange data')

    """
    BackgroundInterface core methods: disabled at this level; provided by use of a BackgroundManager
    """
    def foreground(self, process, ref_flow=None):
        raise BackgroundRequired('No knowledge of background')

    def foreground_flows(self, search=None):
        raise BackgroundRequired('No knowledge of background')

    def background_flows(self, search=None):
        raise BackgroundRequired('No knowledge of background')

    def exterior_flows(self, direction=None, search=None):
        raise BackgroundRequired('No knowledge of exterior flows')

    def cutoffs(self, direction=None, search=None):
        raise BackgroundRequired('No knowledge of cutoff flows')

    def emissions(self):
        raise BackgroundRequired('No knowledge of elementary compartments')

    def lci(self, process):
        raise BackgroundRequired('No knowledge of background system')

    def ref_lci(self, process, ref_flow):
        raise BackgroundRequired('No knowledge of background system')

    def ad(self, process):
        raise BackgroundRequired('No knowledge of background dependencies')

    def ref_ad(self, process, ref_flow):
        raise BackgroundRequired('No knowledge of background dependencies')

    def bf(self, process):
        raise BackgroundRequired('No knowledge of foreground emissions')

    def ref_bf(self, process, ref_flow):
        raise BackgroundRequired('No knowledge of foreground emissions')

    def lcia(self, process, query_qty):
        raise BackgroundRequired('No knowledge of background system')

    def ref_lcia(self, process, ref_flow, query_qty):
        raise BackgroundRequired('No knowledge of background system')
