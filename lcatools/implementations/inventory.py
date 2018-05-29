from .basic import BasicImplementation
from lcatools.interfaces import InventoryInterface
from lcatools.fragment_flows import frag_flow_lcia


class InventoryImplementation(BasicImplementation, InventoryInterface):
    """
    This provides access to detailed exchange values and computes the exchange relation.
    Creates no additional requirements on the archive.
    """
    def exchanges(self, process, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        for x in p.exchanges():
            yield x

    def exchange_values(self, process, flow, direction=None, termination=None, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        for x in p.exchange_values(self.get(flow), direction=direction):
            if termination is None:
                yield x
            else:
                if x.termination == termination:
                    yield x

    def inventory(self, process, ref_flow=None, scenario=None, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        if p.entity_type == 'process':
            for x in sorted(p.inventory(ref_flow=ref_flow),
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
        p = self._archive.retrieve_or_fetch_entity(process)
        xs = [x for x in p.inventory(ref_flow=ref_flow)
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
        p = self._archive.retrieve_or_fetch_entity(process)
        return quantity_ref.do_lcia(p.inventory(ref_flow=ref_flow),
                                    locale=p['SpatialScope'],
                                    refresh=refresh)

    def traverse(self, fragment, scenario=None, **kwargs):
        frag = self._archive.retrieve_or_fetch_entity(fragment)
        return frag.top().traverse(scenario, observed=True)

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        quantity_ref.ensure_lcia()
        fragmentflows = self.traverse(fragment, scenario=scenario, **kwargs)
        return frag_flow_lcia(fragmentflows, quantity_ref, scenario=scenario, refresh=refresh)
