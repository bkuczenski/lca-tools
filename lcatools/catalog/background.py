import re
from lcatools.catalog.interfaces import BasicInterface


class BackgroundInterface(BasicInterface):
    """
    The BackgroundInterface exposes LCI computation with matrix ordering and inversion, and LCIA computation with
    enclosed private access to a quantity db.
    """
    def __init__(self, archive, qdb, **kwargs):
        super(BackgroundInterface, self).__init__(archive, **kwargs)
        self._qdb = qdb

    def foreground(self, process, ref_flow=None):
        return self._archive.bg.foreground(process, ref_flow=ref_flow)

    def foreground_flows(self, search=None):
        for k in self._archive.bg.foreground_flows:
            if search is None:
                yield k
            else:
                if bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    yield k

    def background_flows(self, search=None):
        for k in self._archive.bg.background_flows:
            if search is None:
                yield k
            else:
                if bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    yield k

    def exterior_flows(self, direction=None, search=None):
        for k in self._archive.bg.exterior_flows:
            if direction is not None:
                if k.direction != direction:
                    continue
            if search is not None:
                if not bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    continue
            yield k

    def cutoffs(self, direction=None, search=None):
        for k in self._archive.bg.exterior_flows:
            if self._qdb.is_elementary(k):
                continue
            if direction is not None:
                if k.direction != direction:
                    continue
            if search is not None:
                if not bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    continue
            yield k

    def emissions(self, direction=None, search=None):
        for k in self._archive.bg.exterior_flows:
            if not self._qdb.is_elementary(k):
                continue
            if direction is not None:
                if k.direction != direction:
                    continue
            if search is not None:
                if not bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    continue
            yield k

    def lci(self, process, ref_flow=None):
        return self._archive.bg.lci(process, ref_flow=ref_flow)

    def ad(self, process, ref_flow=None):
        return self._archive.bg.ad_tilde(process, ref_flow=ref_flow)

    def bf(self, process, ref_flow=None):
        return self._archive.bg.bf_tilde(process, ref_flow=ref_flow)

    def lcia(self, process, query_qty, ref_flow=None, **kwargs):
        q = self._qdb.get(query_qty)  # get canonical
        if not self._archive.is_characterized(q):
            self._archive.characterize(self._qdb, q, **kwargs)
        return self._archive.bg.lcia(process, query_qty, ref_flow=ref_flow)
