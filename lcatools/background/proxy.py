from lcatools.background.product_flow import ProductFlow


class BackgroundProxy(object):
    """
    A stand-in for background manager, assuming the archive data sets are already aggregated and no matrix
    construction / inversion needs to be performed
    """
    def __init__(self, index):
        self._index = index

    def re_index(self):
        self._index.re_index()

    '''
    Required methods
    '''
    @property
    def foreground_flows(self):
        """
        No foreground flows for proxy BG archives
        :return:
        """
        for x in []:
            yield x

    @property
    def background_flows(self):
        for p in self._index.processes():
            for rx in p.reference_entity:
                yield ProductFlow(None, p, rx.flow)

    @property
    def exterior_flows(self):
        for x in []:
            yield x

    def foreground(self, process, ref_flow=None):
        """
        for proxy BG archives, the foreground is just the process's reference exchange
        :param process:
        :param ref_flow:
        :return:
        """
        return process.reference(flow=ref_flow)

    def is_in_background(self, process, ref_flow=None):
        return False

    def lci(self, process, ref_flow=None):
        for x in process.inventory(ref_flow=ref_flow):
            yield x
