from .interfaces import (IndexInterface, InventoryInterface, QuantityInterface, BackgroundInterface,
                         ConfigureInterface, EntityNotFound)

from .lcia_results import LciaResult
# , EntityNotFound, IndexRequired


class BasicQuery(IndexInterface, InventoryInterface, QuantityInterface):
    def __init__(self, archive, debug=False):
        self._archive = archive
        self._debug = debug

    def _iface(self, itype, **kwargs):
        if itype is None:
            itype = 'basic'
        yield self._archive.make_interface(itype)

    @property
    def origin(self):
        return self._archive.ref

    @property
    def _tm(self):
        return self._archive.tm

    '''
    I think that's all I need to do!
    '''


class LcQuery(BasicQuery, BackgroundInterface, ConfigureInterface):
    pass
