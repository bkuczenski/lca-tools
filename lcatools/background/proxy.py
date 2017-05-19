

class BackgroundProxy(object):
    """
    A stand-in for background manager, assuming the archive data sets are already aggregated and no matrix
    construction / inversion needs to be performed
    """
    def __init__(self, archive):
        self._archive = archive

    '''
    Required methods
    '''
    @property
    def foreground_flows(self):
        pass

    @property
    def background_flows(self):
        pass

    @property
    def exterior_flows(self):
        pass

    def foreground(self, process, ref_flow=None):
        pass

    def lci(self, process, ref_flow=None):
        pass

