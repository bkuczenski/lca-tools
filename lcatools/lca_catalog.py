from lcatools.catalog import CatalogInterface, ProcessFlowInterface, FlowQuantityInterface


class LcaCatalog(CatalogInterface):
    """
    Class for storing, searching, and referencing lca-tools archives. The catalog will be used to perform
    queries for foreground construction, and fragment traversal, scoring, and publishing.

    An LcaCatalog takes a CatalogInterface and adds ProcessFlow and FlowQuantity utilities
    """

    def __init__(self, catalog_dir=None):
        self._pf = ProcessFlowInterface(self)
        self._fq = FlowQuantityInterface(self)
        self.catalog_dir = catalog_dir
        super(LcaCatalog, self).__init__()

    def load_json_archive(self, *args, **kwargs):
        super(LcaCatalog, self).load_json_archive(*args, **kwargs)
        self._load_exchanges(len(self.archives) - 1)

    def _load_exchanges(self, index):
        self._pf.add_archive(index)

    def exchanges(self, cat_ref, direction=None):
        """
        :param cat_ref: a catalog reference to look up exchanges
        :param direction: [None] if present, filter exchanges to those having the specified direction
        :return: a list of exchanges featuring the entity
        """
        x = self._pf.exchanges(cat_ref)
        if direction is None:
            return x
        return filter(lambda z: z[1].direction == direction, x)

    def characterizations(self, cat_ref):
        if cat_ref.entity_type() == 'quantity':
            return self._fq.characterizations()
        elif cat_ref.entity_type() == 'flow':
            return sorted(self._pf.characterizations(cat_ref),
                          key=lambda x: x.characterization.quantity.reference_entity)
