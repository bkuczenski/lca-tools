from .base import EntityRef


class ProcessRef(EntityRef):
    """
    Processes can lookup:
    """
    _etype = 'process'

    @property
    def _addl(self):
        return self.__getitem__('SpatialScope')

    def __init__(self, external_ref, query, reference_entity, **kwargs):
        if reference_entity is None:
            reference_entity = []
        for rx in reference_entity:
            rx.process = self
        reference_entity = tuple(reference_entity)  # non mutable
        super(ProcessRef, self).__init__(external_ref, query, reference_entity, **kwargs)
        self._default_rx = None
        rxs = [rx for rx in self.references()]
        if len(rxs) == 1:
            self._default_rx = rxs[0].flow.external_ref

    def _show_ref(self):
        for i in self.references():
            print('reference: %s' % i)

    @property
    def name(self):
        return '%s [%s]' % (self['Name'], self['SpatialScope'])

    @property
    def default_rx(self):
        """
        The 'primary' reference exchange of a process CatalogRef.  This is an external_ref for a flow

        (- which is req. unique among references)
        :return:
        """
        return self._default_rx

    @default_rx.setter
    def default_rx(self, value):
        if not isinstance(value, str) and not isinstance(value, int):
            if hasattr(value, 'external_ref'):
                value = value.external_ref
            elif hasattr(value, 'entity_type'):
                if value.entity_type == 'exchange':
                    value = value.flow.external_ref
        if value in [rx.flow.external_ref for rx in self.references()]:
            self._default_rx = value
        else:
            print('Not a valid reference exchange specification')

    def reference(self, flow=None):
        """
        This used to fallback to regular exchanges; no longer.
        :param flow:
        :return:
        """
        '''
        try:
            return next(x for x in self.references(flow=flow))
        except StopIteration:
            return next(x for x in self.exchange_values(flow=flow))
        '''
        return next(x for x in self.references(flow=flow))

    def references(self, flow=None):
        for x in self.reference_entity:
            if flow is None:
                yield x
            elif isinstance(flow, str) and not isinstance(flow, int):
                if x.flow.external_ref == flow:
                    yield x
            else:
                if x.flow == flow:
                    yield x

    '''
    def is_allocated(self, rx):
        """
        For process refs, assume
        :param rx:
        :return:
        """
        for _rx in self.reference_entity:
            if _rx.key == rx.key:
                return _rx.is_alloc
        return False
    '''

    def _use_ref_exch(self, ref_flow):
        if ref_flow is None and self._default_rx is not None:
            ref_flow = self._default_rx
        return ref_flow

    '''
    Inventory queries
    '''
    def exchanges(self, **kwargs):
        return self._query.exchanges(self.external_ref, **kwargs)

    def exchange_values(self, flow, direction=None, termination=None, reference=None, **kwargs):
        if not isinstance(flow, str) and not isinstance(flow, int):
            flow = flow.external_ref
        return self._query.exchange_values(self.external_ref, flow, direction,
                                           termination=termination, reference=reference, **kwargs)

    def inventory(self, ref_flow=None, **kwargs):
        # ref_flow = self._use_ref_exch(ref_flow)  # ref_flow=None returns unallocated inventory
        return self._query.inventory(self.external_ref, ref_flow=ref_flow, **kwargs)

    def exchange_relation(self, ref_flow, exch_flow, direction, termination=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.exchange_relation(self.external_ref, ref_flow.external_ref,
                                             exch_flow.external_ref, direction,
                                             termination=termination, **kwargs)

    def fg_lcia(self, lcia_qty, ref_flow=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.lcia(self.external_ref, ref_flow, lcia_qty, **kwargs)

    '''
    support process
    '''
    def reference_value(self, flow=None):
        return sum(x.value for x in self.exchange_values(flow, reference=True))

    def get_exchange(self, key):
        try:
            return next(x for x in self.reference_entity if x.key == key)
        except StopIteration:
            raise KeyError

    @property
    def alloc_qty(self):
        """
        This is hugely kludgely. What should be the expected behavior of a process ref asked to perform allocation?
        :return:
        """
        return None

    '''
    Background queries
    '''
    def foreground(self, ref_flow=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.foreground(self.external_ref, ref_flow=ref_flow, **kwargs)

    def consumers(self, ref_flow=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.consumers(self.external_ref, ref_flow=ref_flow, **kwargs)

    def dependencies(self, ref_flow=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.dependencies(self.external_ref, ref_flow=ref_flow, **kwargs)

    def emissions(self, ref_flow=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.emissions(self.external_ref, ref_flow=ref_flow, **kwargs)

    def is_in_background(self, termination=None, ref_flow=None, **kwargs):
        if termination is None:
            termination = self.external_ref
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.is_in_background(termination, ref_flow=ref_flow, **kwargs)

    def ad(self, ref_flow=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.ad(self.external_ref, ref_flow, **kwargs)

    def bf(self, ref_flow=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.bf(self.external_ref, ref_flow, **kwargs)

    def lci(self, ref_flow=None, **kwargs):
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.lci(self.external_ref, ref_flow, **kwargs)

    def bg_lcia(self, lcia_qty, ref_flow=None, **kwargs):
        """
        :param lcia_qty: should be a quantity ref (or qty), not an external ID
        :param ref_flow:
        :param kwargs:
        :return:
        """
        ref_flow = self._use_ref_exch(ref_flow)
        return self._query.bg_lcia(self.external_ref, lcia_qty, ref_flow=ref_flow, **kwargs)
