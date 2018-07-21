from lcatools.interfaces import IndexInterface, InventoryInterface, QuantityInterface  # , EntityNotFound, IndexRequired


class AntelopeV1Query(IndexInterface, InventoryInterface, QuantityInterface):
    def __init__(self, archive, debug=False):
        self._archive = archive
        self._debug = debug

    def _iface(self, itype, strict=False):
        yield self._archive.make_interface(itype)

    @property
    def origin(self):
        return self._archive.ref

    '''
    I think that's all I need to do!
    '''
