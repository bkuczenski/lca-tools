from .base import EntityRef

'''
FlowRef needs to actually inherit from flow entity and not from EntityRef-- because the flowRef needs to be able
to store characterizations.
'''

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
