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
            yield self.make_ref(p)

    def flows(self, **kwargs):
        for f in self._archive.flows(**kwargs):
            yield self.make_ref(f)

    def quantities(self, **kwargs):
        for q in self._archive.quantities(**kwargs):
            yield self.make_ref(q)

    def get(self, eid):
        return self.make_ref(self._archive.retrieve_or_fetch_entity(eid))

    def reference(self, eid):
        return self._archive.retrieve_or_fetch_entity(eid).reference_entity

    def terminate(self, flow, direction=None):
        for p in self._archive.terminate(flow, direction=direction):
            yield self.make_ref(p)

    def originate(self, flow, direction=None):
        for p in self._archive.originate(flow, direction=direction):
            yield self.make_ref(p)

    def mix(self, flow, direction):
        return self._archive.mix(flow, direction)
