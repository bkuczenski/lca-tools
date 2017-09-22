from .base import EntityRef


class FlowRef(EntityRef):
    """
    Flows can lookup:
    """
    _etype = 'flow'

    @property
    def _addl(self):
        return self.reference_entity.unit()

    def terminate(self, direction=None, **kwargs):
        return self._query.terminate(self.external_ref, direction, **kwargs)

    def originate(self, direction=None, **kwargs):
        return self._query.originate(self.external_ref, direction, **kwargs)

    def mix(self, direction, **kwargs):
        return self._query.mix(self.external_ref, direction, **kwargs)

