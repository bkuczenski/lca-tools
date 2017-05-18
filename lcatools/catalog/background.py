import re
from lcatools.background.background_manager import BackgroundManager
from lcatools.catalog.basic import BasicInterface


class NonStaticBackground(Exception):
    pass


class BackgroundInterface(BasicInterface):
    """
    The BackgroundInterface exposes LCI computation with matrix ordering and inversion, and LCIA computation with
    enclosed private access to a quantity db.
    """
    def __init__(self, archive, qdb, **kwargs):
        super(BackgroundInterface, self).__init__(archive, **kwargs)
        self._qdb = qdb

        self._bm = None

    @property
    def _bg(self):
        if self._bm is None:
            if self._archive.static:
                # perform costly operations only when/if required
                self._bm = BackgroundManager(self._archive)  # resources only accessible from a BackgroundInterface
            else:
                # non-static interfaces need to implement their own background methods
                self._bm = self._archive
        return self._bm

    def get(self, eid):
        return self.make_ref(self._archive.retrieve_or_fetch_entity(eid))

    def foreground(self, process, ref_flow=None):
        return self._bg.foreground(process, ref_flow=ref_flow)

    def foreground_flows(self, search=None):
        for k in self._bg.foreground_flows:
            if search is None:
                yield k
            else:
                if bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    yield k

    def background_flows(self, search=None):
        for k in self._bg.background_flows:
            if search is None:
                yield k
            else:
                if bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    yield k

    def exterior_flows(self, direction=None, search=None):
        for k in self._bg.exterior_flows:
            if direction is not None:
                if k.direction != direction:
                    continue
            if search is not None:
                if not bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    continue
            yield k

    def cutoffs(self, direction=None, search=None):
        for k in self._bg.exterior_flows:
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
        for k in self._bg.exterior_flows:
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
        return self._bg.lci(process, ref_flow=ref_flow)

    def ad(self, process, ref_flow=None):
        return self._bg.ad_tilde(process, ref_flow=ref_flow)

    def bf(self, process, ref_flow=None):
        return self._bg.bf_tilde(process, ref_flow=ref_flow)

    def lcia(self, process, query_qty, ref_flow=None, **kwargs):
        q = self._qdb.get(query_qty)  # get canonical
        if not self.is_characterized(q):
            if self._archive.static:
                self.characterize(self._qdb, q, **kwargs)
            else:
                raise NonStaticBackground('Not characterized for %s' % q)
        return self._bg.lcia(process, query_qty, ref_flow=ref_flow)
