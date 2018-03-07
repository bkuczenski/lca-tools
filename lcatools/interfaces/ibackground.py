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
            if not self.is_elementary(i):
                yield i

    def dependencies(self, process, ref_flow=None, **kwargs):
        """
        Interior background exchanges for a given node

        :param direction:
        :param search:
        :return:
        """
        return self._perform_query(_interface, 'dependencies', BackgroundRequired('No knowledge of background'),
                                   process, ref_flow=ref_flow, **kwargs)

    def emissions(self, process, ref_flow=None, **kwargs):
        """
        Exterior exchanges for a given node

        :param direction:
        :param search:
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
