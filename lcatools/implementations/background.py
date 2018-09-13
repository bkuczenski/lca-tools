import re

from .basic import BasicImplementation
from ..interfaces import BackgroundInterface, ProductFlow, ExteriorFlow, EntityNotFound


class NonStaticBackground(Exception):
    pass


def search_skip(entity, search):
    if search is None:
        return False
    return not bool(re.search(search, str(entity), flags=re.IGNORECASE))


class BackgroundImplementation(BasicImplementation, BackgroundInterface):
    """
    The default Background Implementation exposes an ordinary inventory database as a collection of LCI results.
    Because it does not perform any ordering, there is no way to distinguish between foreground and background
    elements in a database using the proxy. It is thus inconsistent for the same resource to implement both
    inventory and [proxy] background interfaces from the same data archive.
    """
    def __init__(self, *args, **kwargs):
        super(BackgroundImplementation, self).__init__(*args, **kwargs)

        self._index = None

    def setup_bm(self, index=None):
        """
        Requires an index interface or catalog query <-- preferred
        :param index:
        :return:
        """
        if self._index is None:
            if index is None:
                self._index = self._archive.make_interface('index')
            else:
                self._index = index

    def _ensure_ref_flow(self, ref_flow):
        if ref_flow is not None:
            if isinstance(ref_flow, str) or isinstance(ref_flow, int):
                ref_flow = self._archive.retrieve_or_fetch_entity(ref_flow)
        return ref_flow

    def foreground_flows(self, search=None, **kwargs):
        """
        No foreground flows in the proxy background
        :param search:
        :param kwargs:
        :return:
        """
        for i in []:
            yield i

    def background_flows(self, search=None, **kwargs):
        """
        all process reference flows are background flows
        :param search:
        :param kwargs:
        :return:
        """
        self.setup_bm()
        for p in self._index.processes():
            for rx in p.references():
                if search_skip(p, search):
                    continue
                yield ProductFlow(self._archive.ref, rx.flow, rx.direction, p, None)

    def exterior_flows(self, direction=None, search=None, **kwargs):
        """
        Since contexts are still in limbo, we need a default directionality (or some way to establish directionality
        for compartments..) but for now let's just use default 'output' for all exterior flows
        :param direction:
        :param search:
        :param kwargs:
        :return:
        """
        self.setup_bm()
        for f in self._index.flows():
            if search_skip(f, search):
                continue
            try:
                next(self._index.terminate(f.external_ref, direction=direction))
            except StopIteration:
                if self.is_elementary(f):
                    yield ExteriorFlow(self._archive.ref, f, 'Output', f['Compartment'])
                else:
                    yield ExteriorFlow(self._archive.ref, f, 'Output', None)

    def consumers(self, process, ref_flow=None, **kwargs):
        """
        Not supported for trivial backgrounds
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        for i in []:
            yield i

    def dependencies(self, process, ref_flow=None, **kwargs):
        """
        All processes are LCI, so they have no dependencies
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        for i in []:
            yield i

    def emissions(self, process, ref_flow=None, **kwargs):
        """
        All processes are LCI, so they have no emissions
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        for i in []:
            yield i

    def foreground(self, process, ref_flow=None, **kwargs):
        self.setup_bm()
        ref_flow = self._ensure_ref_flow(ref_flow)
        p = self._index.get(process)
        for rx in p.reference(ref_flow):
            yield rx  # should be just one exchange

    def is_in_scc(self, process, ref_flow=None, **kwargs):
        """
        Distinction between is_in_background and is_in_scc will reveal the proxy nature of the interface
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        return False  # proxy has no knowledge of SCCs

    def is_in_background(self, process, ref_flow=None, **kwargs):
        self.setup_bm()
        try:
            self._index.get(process)
        except EntityNotFound:
            return False
        return True

    def ad(self, process, ref_flow=None, **kwargs):
        for i in []:
            yield i

    def bf(self, process, ref_flow=None, **kwargs):
        for i in []:
            yield i

    def lci(self, process, ref_flow=None, **kwargs):
        self.setup_bm()
        ref_flow = self._ensure_ref_flow(ref_flow)
        p = self._index.get(process)
        for x in p.inventory(ref_flow=ref_flow):
            yield x

    def bg_lcia(self, process, query_qty, ref_flow=None, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        lci = self.lci(p, ref_flow=ref_flow)
        res = query_qty.do_lcia(lci, locale=p['SpatialScope'], **kwargs)
        """
        if self.privacy > 0:
            return res.aggregate('*', entity_id=p.link)
        """
        return res
