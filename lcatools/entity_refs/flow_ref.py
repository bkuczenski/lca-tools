from .base import EntityRef

from ..interfaces import trim_cas, QuantityRequired

from lcatools.characterizations import Characterization

'''
FlowRef needs to actually inherit from flow entity and not from EntityRef-- because the flowRef needs to be able
to store characterizations.
'''


class FlowRef(EntityRef):
    """
    Flows can lookup:
    """
    _etype = 'flow'

    def __init__(self, *args, **kwargs):
        super(FlowRef, self).__init__(*args, **kwargs)
        self._characterizations = dict()
        self._cfs_fetched = False
        self.add_characterization(self.reference_entity, value=1.0)

    def unit(self):
        return self.reference_entity.reference_entity

    def match(self, other):
        """
        Re-implement flow match method
        :param other:
        :return:
        """
        return (self.uuid == other.uuid or
                self['Name'].lower() == other['Name'].lower() or
                (trim_cas(self['CasNumber']) == trim_cas(other['CasNumber']) and len(self['CasNumber']) > 4) or
                self.external_ref == other.external_ref)  # not sure about this last one! we should check origin too

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
        if quantity.uuid in self._characterizations.keys():
            if location == 'GLO' or location is None:
                return True
            if location in self._characterizations[quantity.uuid].locations():
                return True
        return False

    def add_characterization(self, quantity, value=None, **kwargs):
        q = quantity.uuid
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
                kwargs['overwrite'] = kwargs.pop('overwrite', False)
                c.add_value(value=value, **kwargs)
        return c

    def characterizations(self):
        for i in self._characterizations.values():
            yield i

    def serialize(self, characterizations=False, domesticate=False, drop_fields=None, **kwargs):
        j = super(FlowRef, self).serialize(domesticate=domesticate, drop_fields=drop_fields)
        if characterizations:
            j['characterizations'] = sorted([x.serialize(**kwargs) for x in self._characterizations.values()],
                                            key=lambda x: x['quantity'])
        else:
            j['characterizations'] = [x.serialize(**kwargs) for x in self._characterizations.values()
                                      if x.quantity is self.reference_entity]

        return j

    '''
    Interface methods
    '''

    def terminate(self, direction=None, **kwargs):
        return self._query.terminate(self.external_ref, direction, **kwargs)

    def originate(self, direction=None, **kwargs):
        return self._query.originate(self.external_ref, direction, **kwargs)

    def cf(self, query_quantity, locale='GLO', **kwargs):
        if isinstance(query_quantity, str):
            query_quantity = self._query.get(query_quantity)
        u = query_quantity.uuid
        if u in self._characterizations:
            return self._characterizations[u][locale]
        try:
            val = self._query.cf(self.external_ref, query_quantity, locale=locale, **kwargs)
        except QuantityRequired:
            print('!Unable to lookup flow CF\nflow: %s\nquantity: %s' % (self.link, query_quantity.link))
            val = 0.0
        self.add_characterization(query_quantity, value=val, location=locale)
        return val

    def factor(self, quantity):
        if quantity.uuid in self._characterizations:
            return self._characterizations[quantity.uuid]
        return Characterization(self, quantity)

    def profile(self, show=False, **kwargs):
        """
        Made to print out results, to operate identically to LcFlow.profile()
        :param show: whether to print to stdout
        :param kwargs:
        :return:
        """
        if show:
            print('%s' % self)
        out = []
        seen = set()
        for cf in self._characterizations.values():
            if show:
                print('%2d %s' % (len(out), cf.q_view()))
            out.append(cf)
            seen.add(cf.quantity.uuid)
        if not self._cfs_fetched:
            try:
                for cf in self._query.profile(self.external_ref, **kwargs):
                    if cf.quantity.uuid in seen:
                        continue
                    if show:
                        print('%2d %s' % (len(out), cf.q_view()))
                    out.append(cf)
                    seen.add(cf.quantity.uuid)
                    self.add_characterization(cf.quantity, value={l: cf[l] for l in cf.locations()})
                self._cfs_fetched = True
            except QuantityRequired:
                pass  # return empty; keep checking on subsequent queries
        return out

    '''
    def mix(self, direction, **kwargs):
        return self._query.mix(self.external_ref, direction, **kwargs)
    '''
