from antelope_interface import (IndexInterface, InventoryInterface, QuantityInterface, BackgroundInterface,
                                ConfigureInterface)
# , EntityNotFound, IndexRequired


class BasicQuery(IndexInterface, InventoryInterface, QuantityInterface):
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


class LcQuery(BasicQuery, BackgroundInterface, ConfigureInterface):
    pass
