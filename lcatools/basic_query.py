from .interfaces import (IndexInterface, InventoryInterface, QuantityInterface, BackgroundInterface,
                         ConfigureInterface, EntityNotFound)

from .lcia_results import LciaResult
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

    def get_item(self, external_ref, item):
        """
        access an entity's dictionary items
        :param external_ref:
        :param item:
        :return:
        """
        return self._perform_query(None, 'get_item', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref, item)

    '''
    I think that's all I need to do!
    '''


class LcQuery(BasicQuery, BackgroundInterface, ConfigureInterface):
    pass
