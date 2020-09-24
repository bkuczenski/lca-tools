from .base import EntityRef

from lcatools.flow import Flow


'''
FlowRef needs to actually inherit from flow entity and not from EntityRef-- because the flowRef needs to be able
to store characterizations.  Also contexts + flowables.  Frankly, it's hard to think of any reason a flow ref should
ever be used in place of a regular flow.  

Think about this for future.  For now, just reimplement everything.
'''


class FlowRef(EntityRef, Flow):
    """
    Flows can lookup:
    """
    _etype = 'flow'
    _ref_field = 'referenceQuantity'

    def __init__(self, *args, **kwargs):
        super(FlowRef, self).__init__(*args, **kwargs)
        for t in ('name', 'casnumber'):  # have to do this bc we use _d.update() and this seems least crunchy
            if self.has_property(t):
                self._flowable.add_term(self._localitem(t))
        self._flowable.add_term(self.link)

    @property
    def _addl(self):
        return self.unit

    '''
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
    '''
    def __setitem__(self, key, value):
        """
        trade one DRY for another... this is not too bad though.
        :param key:
        :param value:
        :return:
        """
        self._catch_context(key, value)
        self._catch_flowable(key.lower(), value)
        super(FlowRef, self).__setitem__(key, value)

    def serialize(self, characterizations=False, domesticate=False, **kwargs):
        j = super(FlowRef, self).serialize(domesticate=domesticate)
        j['referenceQuantity'] = self.reference_entity.external_ref

        return j

    '''
    Interface methods
    '''

    def terminate(self, direction=None, **kwargs):
        return self._query.terminate(self.external_ref, direction, **kwargs)

    def originate(self, direction=None, **kwargs):
        return self._query.originate(self.external_ref, direction, **kwargs)

    def profile(self, **kwargs):
        return self._query.profile(self.external_ref, **kwargs)

    def characterize(self, quantity, value, context=None, **kwargs):
        if context is None:
            context = self.context
        flowable = self.name
        return self._query.characterize(flowable, self.reference_entity, quantity, value, context=context,
                                        origin=self.origin, **kwargs)

    '''
    def cf(self, query_quantity, locale='GLO', **kwargs):
        if isinstance(query_quantity, str):
            query_quantity = self._query.get(query_quantity)
        u = query_quantity.uuid
        if u in self._characterizations:
            return self._characterizations[u][locale]
        try:
            val = self._query.cf(self.link, query_quantity, locale=locale, **kwargs)
            self._query.add_c14n(self.link, self.reference_entity.external_ref,
                                 query_quantity.external_ref, val, context=self.context, location=locale)
        except QuantityRequired:
            print('!Unable to lookup flow CF\nflow: %s\nquantity: %s' % (self.link, query_quantity.link))
            val = 0.0
        return val

    def profile(self, show=False, **kwargs):
        """
        Made to print out results, to operate identically to LcFlow.profile()
        :param show: whether to print to stdout
        :param kwargs:
        :return:
        """
        return self._query.profile()
        if show:
            print('%s' % self)
        out = []
        seen = set()
        if not self._cfs_fetched:
            try:
                for cf in self._query.profile(self.external_ref, **kwargs):
                    if cf.quantity.uuid in seen:
                        continue
                    if show:
                        print('%2d %s' % (len(out), cf.q_view()))
                    out.append(cf)
                    seen.add(cf.quantity.uuid)
                    self._query.add_c14n(self['Name'], self.reference_entity.link,
                                         cf.quantity.link, value={l: cf[l] for l in cf.locations()},
                                         context=self.context)
                self._cfs_fetched = True
            except QuantityRequired:
                pass  # return empty; keep checking on subsequent queries
        return out

    def mix(self, direction, **kwargs):
        return self._query.mix(self.external_ref, direction, **kwargs)
    '''
