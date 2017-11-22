from .base import EntityRef
from lcatools.interfaces import IndexInterface, QuantityInterface
from lcatools.characterizations import Characterization

'''
FlowRef needs to actually inherit from flow entity and not from EntityRef-- because the flowRef needs to be able
to store characterizations.
'''


class FlowRef(EntityRef, IndexInterface, QuantityInterface):
    """
    Flows can lookup:
    """
    _etype = 'flow'

    def __init__(self, *args, **kwargs):
        super(FlowRef, self).__init__(*args, **kwargs)
        self._characterizations = dict()
        self.add_characterization(self.reference_entity, value=1.0)

    def unit(self):
        return self.reference_entity.reference_entity

    @property
    def _addl(self):
        return self.reference_entity.unit()

    def has_characterization(self, quantity, location='GLO'):
        """
        A flow ref keeps track of characterizations by link
        :param quantity:
        :param location:
        :return:
        """
        if quantity.link in self._characterizations.keys():
            if location == 'GLO' or location is None:
                return True
            if location in self._characterizations[quantity.link].locations():
                return True
        return False

    def add_characterization(self, quantity, value=None, **kwargs):
        q = quantity.link
        if q in self._characterizations.keys():
            if value is None:
                return
            c = self._characterizations[q]
        else:
            c = Characterization(self, quantity)
            self._characterizations[q] = c
        if value is not None:
            if isinstance(value, dict):
                c.update_values(**value)
            else:
                c.add_value(value=value, overwrite=False, **kwargs)
        return c

    def characterizations(self):
        for i in self._characterizations.values():
            yield i

    '''
    Interface methods
    '''

    def terminate(self, direction=None, **kwargs):
        return self._query.terminate(self.external_ref, direction, **kwargs)

    def originate(self, direction=None, **kwargs):
        return self._query.originate(self.external_ref, direction, **kwargs)

    def cf(self, query_quantity, locale='GLO', **kwargs):
        return self._query.cf(self, query_quantity, locale=locale, **kwargs)

    def profile(self, **kwargs):
        """
        Made to print out results, to operate identically to LcFlow.profile()
        :param kwargs:
        :return:
        """
        print('%s' % self)
        out = []
        for cf in self._query.profile(self.external_ref, **kwargs):
            print('%2d %s' % (len(out), cf.q_view()))
            out.append(cf)
        return out

    '''
    def mix(self, direction, **kwargs):
        return self._query.mix(self.external_ref, direction, **kwargs)
    '''
