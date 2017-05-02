from lcatools.catalog.interfaces import QueryInterface


PRIVACY_DICT = {
    None: 0,
    False: 0,
    'None': 0,
    0: 0,
    'values': 1,
    'Values': 1,
    1: 1,
    'all': 2,
    'All': 2,
    True: 2
}


class EntityInterface(QueryInterface):
    """
    A CatalogInterface provides basic-level semantic data about entities
    """

    def __init__(self, archive, privacy=None):
        """
        Creates a semantic catalog from the specified archive.  Uses archive.get_names() to map data sources to
        semantic references.
        :param archive: a StaticArchive.  Foreground and background information
        :param privacy: [None]
          None | False | 'None' | 0 : full access
          'values' | 1 : exchange lists public, but exchange values private
          'all' | True | 2: exchange lists and values are private
        """
        self._archive = archive
        self._privacy = privacy

    @property
    def privacy(self):
        return PRIVACY_DICT[self._privacy]

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
