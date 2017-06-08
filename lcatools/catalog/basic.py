from lcatools.catalog.interfaces import QueryInterface
from lcatools.catalog_ref import CatalogRef


class PrivateArchive(Exception):
    pass


class BasicInterface(QueryInterface):
    def __init__(self, archive, privacy=None, **kwargs):
        """
        Creates a semantic catalog from the specified archive.  Uses archive.get_names() to map data sources to
        semantic references.
        :param archive: a StaticArchive.  Foreground and background information
        :param privacy: [None] Numeric scale indicating the level of privacy protection.  This is TBD... for now the
        scale has the following meaning:
         0 - no restrictions, fully public
         1 - exchange lists are public, but exchange values are private
         2 - exchange lists and exchange values are private
        """
        super(BasicInterface, self).__init__(archive.ref, **kwargs)
        self._archive = archive
        self._privacy = privacy or 0

        self._quantities = set()  # quantities for which archive has been characterized

    @property
    def privacy(self):
        return self._privacy

    def __str__(self):
        return '%s for %s (%s)' % (self.__class__.__name__, self.origin, self._archive.source)

    def __getitem__(self, item):
        return self._archive[item]

    def is_characterized(self, quantity):
        return quantity in self._quantities

    def characterize(self, qdb, quantity, force=False, overwrite=False, locale='GLO'):
        """
        A hook that allows the LcCatalog to lookup characterization values using a supplied quantity db.  Quantities
        that are looked up are added to a list so they aren't repeated.
        :param qdb:
        :param quantity: an actual entity
        :param force: [False] re-characterize even if the quantity has already been characterized.
        :param overwrite: [False] remove and replace existing characterizations.  (may have no effect if force=False)
        :param locale: ['GLO'] which CF to retrieve
        :return: a list of flows that have been characterized
        """
        chars = []
        if quantity not in self._quantities or force:
            for f in self._archive.flows():
                if f.has_characterization(quantity):
                    if overwrite:
                        f.del_characterization(quantity)
                    else:
                        chars.append(f)
                        continue
                val = qdb.convert(flow=f, query=quantity, locale=locale)
                if val != 0.0:
                    chars.append(f)
                    f.add_characterization(quantity, value=val)
            self._quantities.add(quantity)
        return chars

    def make_ref(self, entity):
        if entity is None:
            return None
        if entity.entity_type == 'flow':
            return entity  # keep characterizations intact
        return CatalogRef(self.origin, entity.external_ref, catalog=self._catalog, entity_type=entity.entity_type)

    def get_item(self, external_ref, item):
        return self._archive.get_item(external_ref, item)

    def get_reference(self, external_ref):
        return self._archive.get_reference(external_ref)

    def get_uuid(self, external_ref):
        return self._archive.get_uuid(external_ref)

    def get(self, external_ref):
        return self.make_ref(self._archive.retrieve_or_fetch_entity(external_ref))
