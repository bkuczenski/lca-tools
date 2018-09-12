"""
The Background interface is a hybrid interface that can be produced from the combination of  a complete index and
inventory interface for a self-contained database (terminate() and inventory() are required, and the resulting matrix
must be invertible)

The default implementation is a dumb proxy, for use when archives provide LCI information over an inventory interface.
 Here the proxy just 'masquerades' the contents to answer background instead of inventory queries.  Though it does not
 thrown an error, it is a conceptual violation for one data source to supply both inventory and background interfaces
 using the proxy implementation.

The LcBackground class, added with the antelope_lcbackground plugin, provides an engine that partially orders the
datasets in an ecoinvent-style unit process database and stores the results in a static set of scipy sparse arrays, in
a manner consistent with the JIE disclosure paper (augmented with the full LCIDB).

The (as yet hypothetical) antelope_brightway2 plugin can provide index, inventory, and background data for a bw2
database.

More plugins are yet imagined.
"""

from .abstract_query import AbstractQuery


class BackgroundRequired(Exception):
    pass


_interface = 'background'


class BackgroundInterface(AbstractQuery):
    """
    BackgroundInterface core methods
    """
    def setup_bm(self, index=None):
        """
        allows a background implementation to obtain an index interface from the catalog
        :param index:
        :return:
        """
        pass

    def foreground_flows(self, search=None, **kwargs):
        """
        Yield a list of ProductFlows, which serialize to: origin, process external ref, reference flow external ref,
          direction.

        :param search:
        :return: ProductFlows (should be ref-ized somehow)
        """
        return self._perform_query(_interface, 'foreground_flows', BackgroundRequired('No knowledge of background'),
                                   search=search, **kwargs)

    def background_flows(self, search=None, **kwargs):
        """
        Yield a list of ProductFlows, which serialize to: origin, process external ref, reference flow external ref,
          direction.

        :param search:
        :return: ProductFlows (should be ref-ized somehow)
        """
        return self._perform_query(_interface, 'background_flows', BackgroundRequired('No knowledge of background'),
                                   search=search, **kwargs)

    def exterior_flows(self, direction=None, search=None, **kwargs):
        """
        Yield a list of ExteriorFlows or cutoff flows, which serialize to flow, direction

        :param direction:
        :param search:
        :return: ExteriorFlows
        """
        return self._perform_query(_interface, 'exterior_flows', BackgroundRequired('No knowledge of background'),
                                   search=search, **kwargs)

    def cutoffs(self, direction=None, search=None, **kwargs):
        """
        Exterior Intermediate Flows

        :param direction:
        :param search:
        :return:
        """
        for i in self.exterior_flows(direction=direction, search=search, **kwargs):
            if i.termination is None:
                yield i

    def consumers(self, process, ref_flow=None, **kwargs):
        """
        Generate ProductFlows that include the identified process/ref_flow as a dependency
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'consumers', BackgroundRequired('Background matrix required'),
                                   process, ref_flow=ref_flow, **kwargs)

    def dependencies(self, process, ref_flow=None, **kwargs):
        """
        Interior background exchanges for a given node

        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query(_interface, 'dependencies', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def emissions(self, process, ref_flow=None, **kwargs):
        """
        Exterior exchanges for a given node

        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query(_interface, 'emissions', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def foreground(self, process, ref_flow=None, **kwargs):
        """
        Returns an ordered list of exchanges for the foreground matrix Af for the given process and reference flow-
        the first being the named process + reference flow, and every successive one having a named termination, so
        that the exchanges could be linked into a fragment tree.
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query(_interface, 'foreground', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def is_in_scc(self, process, ref_flow=None, **kwargs):
        """
        Returns True if the identified productflow is part of a strongly connected component (including the background)
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'is_in_scc', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def is_in_background(self, process, ref_flow=None, **kwargs):
        """

        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'is_in_background', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def ad(self, process, ref_flow=None, **kwargs):
        """
        returns background dependencies as a list of exchanges
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query(_interface, 'ad', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def bf(self, process, ref_flow=None, **kwargs):
        """
        returns foreground emissions as a list of exchanges
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query(_interface, 'bf', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def lci(self, process, ref_flow=None, **kwargs):
        """
        returns aggregated LCI as a list of exchanges (privacy permitting)
        :param process:
        :param ref_flow:
        :return:
        """
        return self._perform_query(_interface, 'lci', BackgroundRequired('No knowledge of background'),
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
        return self._perform_query(_interface, 'bg_lcia', BackgroundRequired('No knowledge of background'),
                                   process, query_qty, ref_flow=ref_flow, **kwargs)
