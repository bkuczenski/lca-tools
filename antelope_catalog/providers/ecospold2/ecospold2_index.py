from lcatools.implementations import IndexImplementation


class EcoSpold2IndexImplementation(IndexImplementation):
    def re_index(self, cutoffs=False):
        """
        do nothing here as termination index is not subject to change.
        :param cutoffs:
        :return:
        """
        pass

    def terminate(self, flow, **kwargs):
        for p in self._terminations[flow]:
            yield self._archive.retrieve_or_fetch_entity(p)

    def processes(self, literal=False, **kwargs):
        for p in self._archive.processes(**kwargs):
            yield p
