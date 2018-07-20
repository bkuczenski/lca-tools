from lcatools.interfaces import IndexInterface, InventoryInterface, QuantityInterface  # , EntityNotFound, IndexRequired


class AntelopeV1Query(IndexInterface, InventoryInterface, QuantityInterface):
    pass
