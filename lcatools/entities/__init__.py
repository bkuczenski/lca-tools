from .quantities import LcQuantity, LcUnit
from .flows import Flow, LcFlow
from .processes import LcProcess
from .entities import LcEntity, entity_types
from .fragments import LcFragment

class DummyFlow(Flow):
    """
    It's not even clear what the purpose of this is
    """

    class DummyQuantity(object):
        name = 'Dummy Quantity'
        origin = 'local.dummy'
        entity_type = 'quantity'
        external_ref = 'dummy'
        uuid = None
        is_entity = False

        def __getitem__(self, key):
            return 'Dummy Property'

        @property
        def link(self):
            return '%s/%s' % (self.origin, self.external_ref)

        @property
        def unit(self):
            return 'd'

        def quantity_terms(self):
            yield self.name
            yield self.external_ref
            yield self.link

    _reference_entity = DummyQuantity()

    @property
    def link(self):
        return 'local.dummy/flow'

    @property
    def reference_entity(self):
        return self._reference_entity