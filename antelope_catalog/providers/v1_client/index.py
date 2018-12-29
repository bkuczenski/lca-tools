from lcatools.implementations import IndexImplementation


class AntelopeIndexImplementation(IndexImplementation):
    def re_index(self, cutoffs=False):
        self._archive.ti = {}

    def terminate(self, flow_ref, direction=None, **kwargs):
        raise NotImplementedError

    def count(self, entity_type, **kwargs):
        raise NotImplementedError
