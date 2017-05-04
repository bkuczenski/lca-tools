from lcatools.catalog.basic import BasicInterface


class EntityInterface(BasicInterface):
    """
    A CatalogInterface provides basic-level semantic data about entities
    """

    """
    CatalogInterface core methods
    These are the main tools for describing information about the contents of the archive
    """

    def processes(self, **kwargs):
        for p in self._archive.processes(**kwargs):
            yield p.trim()

    def flows(self, **kwargs):
        for f in self._archive.flows(**kwargs):
            yield f.trim()

    def quantities(self, **kwargs):
        for q in self._archive.quantities(**kwargs):
            yield q

    def get(self, eid):
        return self._archive[eid].trim()

    def reference(self, eid):
        return self.get(eid).reference_entity

    def terminate(self, flow, direction=None):
        return self._archive.terminate(flow, direction=direction)

    def originate(self, flow, direction=None):
        return self._archive.originate(flow, direction=direction)

    def mix(self, flow, direction):
        return self._archive.mix(flow, direction)
