from .interfaces import (IndexInterface, InventoryInterface, QuantityInterface, BackgroundInterface,
                         ConfigureInterface)
# , EntityNotFound, IndexRequired


class BasicQuery(IndexInterface, InventoryInterface, QuantityInterface):
    def __init__(self, archive, debug=False):
        self._archive = archive
        self._debug = debug

    def _iface(self, itype, strict=False):
        if itype is None:
            itype = 'basic'
        yield self._archive.make_interface(itype)

    @property
    def origin(self):
        return self._archive.ref

    def validate(self):
        return self.origin

    '''
    I think that's all I need to do!
    '''


class LcQuery(BasicQuery, BackgroundInterface, ConfigureInterface):
    pass
