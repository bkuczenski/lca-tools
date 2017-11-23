from .basic import BasicImplementation
from lcatools.fragment_flows import frag_flow_lcia
from lcatools.interfaces import InventoryInterface, PrivateArchive, EntityNotFound


class InventoryImplementation(BasicImplementation, InventoryInterface):
    """
    This provides access to detailed exchange values and computes the exchange relation
    """
    def exchanges(self, process, **kwargs):
        if hasattr(self._archive, 'exchanges'):
            for x in self._archive.exchanges(process, **kwargs):
                yield x
        else:
            if self.privacy > 1:
                raise PrivateArchive('Exchange lists are protected')
            p = self._archive.retrieve_or_fetch_entity(process)
            for x in p.exchanges():
                yield x

    def exchange_values(self, process, flow, direction, termination=None, **kwargs):
        if hasattr(self._archive, 'exchange_values'):
            for x in self._archive.exchange_values(process, flow, direction, termination=termination, **kwargs):
                yield x
        else:
            if self.privacy > 0:
                raise PrivateArchive('Exchange values are protected')
            p = self._archive.retrieve_or_fetch_entity(process)
            for x in p.exchange_values(self.get(flow), direction=direction):
                if termination is None:
                    yield x
                else:
                    if x.termination == termination:
                        yield x

    def inventory(self, process, ref_flow=None, scenario=None, **kwargs):
        if hasattr(self._archive, 'inventory'):
            for x in self._archive.inventory(process, ref_flow=ref_flow, scenario=scenario, **kwargs):
                yield x
        else:
            if self.privacy > 0:
                raise PrivateArchive('Exchange values are protected')
            p = self._archive.retrieve_or_fetch_entity(process)
            if p.entity_type == 'process':
                for x in sorted(p.inventory(reference=ref_flow),
                                key=lambda t: (not t.is_reference, t.direction, t.value or 0.0)):
                    yield x
            elif p.entity_type == 'fragment':
                for x in p.inventory(scenario=scenario, observed=True):
                    yield x

    def exchange_relation(self, process, ref_flow, exch_flow, direction, termination=None, **kwargs):
        """

        :param process:
        :param ref_flow:
        :param exch_flow:
        :param direction:
        :param termination:
        :return:
        """
        if hasattr(self._archive, 'exchange_relation'):
            return self._archive.exchange_relation(process, ref_flow, exch_flow, direction, termination=termination,
                                                   **kwargs)
        if self.privacy > 0:
            raise PrivateArchive('Exchange values are protected')
        p = self._archive.retrieve_or_fetch_entity(process)
        xs = [x for x in p.inventory(reference=ref_flow)
              if x.flow.external_ref == exch_flow and x.direction == direction]
        norm = p.reference(ref_flow)
        if termination is not None:
            xs = [x for x in xs if x.termination == termination]
        if len(xs) == 1:
            return xs[0].value / norm.value
        elif len(xs) == 0:
            return 0.0
        else:
            return sum([x.value for x in xs]) / norm.value

    def lcia(self, process, ref_flow, quantity_ref, refresh=False, **kwargs):
        """
        Implementation of foreground LCIA -- moved from LcCatalog
        :param process:
        :param ref_flow:
        :param quantity_ref:
        :param refresh:
        :param kwargs:
        :return:
        """
        if hasattr(self._archive, 'lcia'):
            # this needs a fallback-
            try:
                return self._archive.lcia(process, ref_flow, quantity_ref, refresh=refresh, **kwargs)
            except EntityNotFound:
                pass  # fall through to local LCIA
        self._catalog.load_lcia_factors(quantity_ref)
        return self._catalog.qdb.do_lcia(quantity_ref, process.inventory(ref_flow=ref_flow),
                                         locale=process['SpatialScope'],
                                         refresh=refresh)

    def traverse(self, fragment, scenario=None, **kwargs):
        if hasattr(self._archive, 'traverse'):
            return self._archive.traverse(fragment.top(), scenario=scenario, **kwargs)
        frag = self._archive.retrieve_or_fetch_entity(fragment)
        return frag.top().traverse(scenario, observed=True)

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        if hasattr(self._archive, 'fragment_lcia'):
            return self._archive.fragment_lcia(fragment, quantity_ref, scenario=scenario, refresh=refresh, **kwargs)
        self._catalog.load_lcia_factors(quantity_ref)
        fragmentflows = self.traverse(fragment, scenario=scenario, **kwargs)
        return frag_flow_lcia(self._catalog.qdb, fragmentflows, quantity_ref, scenario=scenario, refresh=refresh)
