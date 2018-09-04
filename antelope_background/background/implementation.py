from lcatools.implementations import BackgroundImplementation
from lcatools.interfaces import ProductFlow, ExteriorFlow
from lcatools.exchanges import ExchangeValue

from .flat_background import FlatBackground


class TarjanBackgroundImplementation(BackgroundImplementation):
    """
    basic implementation overrides
    """
    def __getitem__(self, item):
        return self._index.get(item)

    def _fetch(self, external_ref, **kwargs):
        return self._index.get(external_ref, **kwargs)

    """
    background implementation
    """
    def __init__(self, *args, **kwargs):
        super(TarjanBackgroundImplementation, self).__init__(*args, **kwargs)

        self._flat = None

    def setup_bm(self, index=None):
        if self._index is None:
            super(TarjanBackgroundImplementation, self).setup_bm(index)
            if hasattr(self._archive, 'create_flat_background'):
                self._flat = self._archive.create_flat_background(index)
            else:
                self._flat = FlatBackground.from_index(index)

    def foreground_flows(self, search=None, **kwargs):
        for fg in self._flat.fg:
            p = self[fg.term_ref]
            f = self[fg.flow_ref]
            yield ProductFlow(self.origin, f, fg.direction, p, fg.scc_id)

    def background_flows(self, search=None, **kwargs):
        for bg in self._flat.bg:
            p = self[bg.term_ref]
            f = self[bg.flow_ref]
            yield ProductFlow(self.origin, f, bg.direction, p, bg.scc_id)

    def exterior_flows(self, search=None, **kwargs):
        for ex in self._flat.ex:
            c = ex.term_ref
            f = self[ex.flow_ref]
            yield ExteriorFlow(self.origin, f, ex.direction, c)

    def is_in_scc(self, process, ref_flow=None, **kwargs):
        return self._flat.is_in_scc(process, ref_flow)

    def is_in_background(self, process, ref_flow=None, **kwargs):
        # ref_flow = self._ensure_ref_flow(ref_flow)
        return self._flat.is_in_background(process, ref_flow)

    def foreground(self, process, ref_flow=None, **kwargs):
        # ref_flow = self._ensure_ref_flow(ref_flow)
        for x in self._flat.foreground(process, ref_flow):
            yield ExchangeValue(self[x.process], self[x.flow], x.direction, termination=x.term, value=x.value)

    def _direct_exchanges(self, node, x_iter):
        for x in x_iter:
            yield ExchangeValue(node, self[x.flow], x.direction, termination=x.term, value=x.value)

    def dependencies(self, process, ref_flow=None, **kwargs):
        # ref_flow = self._ensure_ref_flow(ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.dependencies(process, ref_flow)):
            yield x

    def emissions(self, process, ref_flow=None, **kwargs):
        # ref_flow = self._ensure_ref_flow(ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.emissions(process, ref_flow)):
            yield x

    def ad(self, process, ref_flow=None, **kwargs):
        # ref_flow = self._ensure_ref_flow(ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.ad(process, ref_flow)):
            yield x

    def bf(self, process, ref_flow=None, **kwargs):
        # ref_flow = self._ensure_ref_flow(ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.bf(process, ref_flow)):
            yield x

    def lci(self, process, ref_flow=None, **kwargs):
        # ref_flow = self._ensure_ref_flow(ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.lci(process, ref_flow, **kwargs)):
            yield x
