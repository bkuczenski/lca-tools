import re
from lcatools.background.background_manager import BackgroundManager
from lcatools.background.proxy import BackgroundProxy
from lcatools.background.product_flow import ProductFlow
# from lcatools.background.emission import Emission
from lcatools.catalog.basic import BasicImplementation, PrivateArchive
from lcatools.interfaces.ibackground import BackgroundInterface


class NonStaticBackground(Exception):
    pass


class BackgroundImplementation(BasicImplementation, BackgroundInterface):
    """
    The BackgroundInterface exposes LCI computation with matrix ordering and inversion, and LCIA computation with
    enclosed private access to a quantity db.
    """
    def __init__(self, catalog, archive, **kwargs):
        super(BackgroundImplementation, self).__init__(catalog, archive, **kwargs)

        self._bm = None

    def _make_pf_ref(self, product_flow):
        return ProductFlow(None, self.make_ref(product_flow.process), product_flow.flow)

    @property
    def _bg(self):
        """
        The background is provided either by a BackgroundManager (wrapper for BackgroundEngine matrix inverter)
        or by a BackgroundProxy (assumes archive is already aggregated)
        :return:
        """
        if self._bm is None:
            if self._archive.static:
                # perform costly operations only when/if required
                if not hasattr(self._archive, 'bm'):
                    self._archive.bm = BackgroundManager(self._archive)
                self._bm = self._archive.bm
            else:
                # non-static interfaces implement foreground-as-background
                self._bm = BackgroundProxy(self._archive)
        return self._bm

    '''
    foreground compat methods
    '''

    def exchange_values(self, process, flow, direction, termination=None, **kwargs):
        """
        Just yield reference exchanges through the foreground interface. TODO: also yield LCI results
        Some design compromises here because thinkstep ILCD datasets (a) are private and (b) don't specify their
         reference flows! fuckin thinkstep, man. and (c) are required to reproduce the used oil study.

        note that (d) not ALL thinkstep ILCD datasets don't specify reference flows.

        Also note that (e) LCI provided by proxy bg (with unknown ref_flow) will INCLUDE reference exchanges in the lci,
        but LCI provided by matrix-inversion bg will only include exterior exchanges in the lci (and.  Hence the set
        membership test before the yield.
        :param process:
        :param flow:
        :param direction:
        :param termination:
        :return:
        """
        # TODO: make this use the _bg interface below instead
        if termination is not None:
            raise TypeError('Reference exchanges cannot be terminated')
        p = self._archive.retrieve_or_fetch_entity(process)
        sent = set()
        for x in p.references():
            if x.flow.external_ref == flow and x.direction == direction and x not in sent:
                sent.add(x)
                yield x
        for x in self.lci(process):
            if x.flow.external_ref == flow and x.direction == direction and x not in sent:
                sent.add(x)
                yield x

    def _ensure_ref_flow(self, ref_flow):
        if ref_flow is not None:
            if isinstance(ref_flow, str) or isinstance(ref_flow, int):
                ref_flow = self._archive.retrieve_or_fetch_entity(ref_flow)
        return ref_flow

    '''
    background managed methods
    '''
    def foreground(self, process, ref_flow=None, **kwargs):
        # TODO: make this privacy-sensitive
        p = self._archive.retrieve_or_fetch_entity(process)
        ref_flow = self._ensure_ref_flow(ref_flow)
        return self._bg.foreground(p, ref_flow=ref_flow)

    def is_background(self, process, ref_flow=None, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        ref_flow = self._ensure_ref_flow(ref_flow)
        return self._bg.is_background(p, ref_flow=ref_flow)

    def foreground_flows(self, search=None, **kwargs):
        for k in self._bg.foreground_flows:
            if search is None:
                yield self._make_pf_ref(k)
            else:
                if bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    yield self._make_pf_ref(k)

    def background_flows(self, search=None, **kwargs):
        for k in self._bg.background_flows:
            if search is None:
                yield self._make_pf_ref(k)
            else:
                if bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    yield self._make_pf_ref(k)

    def exterior_flows(self, direction=None, search=None, **kwargs):
        for k in self._bg.exterior_flows:
            if direction is not None:
                if k.direction != direction:
                    continue
            if search is not None:
                if not bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    continue
            yield k

    def cutoffs(self, direction=None, search=None, **kwargs):
        for k in self._bg.exterior_flows:
            if self._catalog.is_elementary(k):
                continue
            if direction is not None:
                if k.direction != direction:
                    continue
            if search is not None:
                if not bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    continue
            yield k

    def emissions(self, direction=None, search=None, **kwargs):
        for k in self._bg.exterior_flows:
            if not self._catalog.is_elementary(k):
                continue
            if direction is not None:
                if k.direction != direction:
                    continue
            if search is not None:
                if not bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    continue
            yield k

    def lci(self, process, ref_flow=None, **kwargs):
        # TODO: make this privacy-sensitive: private only returns cutoffs
        p = self._archive.retrieve_or_fetch_entity(process)
        ref_flow = self._ensure_ref_flow(ref_flow)
        return self._bg.lci(p, ref_flow=ref_flow, **kwargs)

    def ad(self, process, ref_flow=None, **kwargs):
        if self.privacy > 0:
            raise PrivateArchive('Dependency data is protected')
        ref_flow = self._ensure_ref_flow(ref_flow)
        return self._bg.ad_tilde(process, ref_flow=ref_flow, **kwargs)

    def bf(self, process, ref_flow=None, **kwargs):
        if self.privacy > 0:
            raise PrivateArchive('Foreground data is protected')
        ref_flow = self._ensure_ref_flow(ref_flow)
        return self._bg.bf_tilde(process, ref_flow=ref_flow, **kwargs)

    def bg_lcia(self, process, query_qty, ref_flow=None, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        ref_flow = self._ensure_ref_flow(ref_flow)
        lci = self._bg.lci(p, ref_flow=ref_flow)
        res = self._catalog.qdb.do_lcia(query_qty, lci, locale=p['SpatialScope'], **kwargs)
        if self.privacy > 0:
            return res.aggregate('*')
        return res
