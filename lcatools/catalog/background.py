from lcatools.catalog.foreground import ForegroundInterface


class BackgroundInterface(ForegroundInterface):
    """
    The BackgroundInterface exposes LCI computation with matrix ordering and inversion, and LCIA computation with
    enclosed private access to a quantity db (optional).
    """
    def foreground(self, process, ref_flow=None):
        return self._archive.bg.foreground(process, ref_flow)

    def background(self):
        raise BackgroundRequired('No knowledge of background')

    def exterior(self):
        raise BackgroundRequired('No knowledge of exterior flows')

    def cutoff(self):
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

