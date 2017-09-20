from lcatools.catalog.basic import BasicImplementation, PrivateArchive
from lcatools.interfaces.iinventory import InventoryInterface
from lcatools.lcia_results import LciaResult
from lcatools.terminations import SubFragmentAggregation


class InventoryImplementation(BasicImplementation, InventoryInterface):
    """
    This provides access to detailed exchange values and computes the exchange relation
    """
    def get(self, eid, **kwargs):
        if eid is None:
            return None
        return self.make_ref(self._archive.retrieve_or_fetch_entity(eid, **kwargs))

    def exchanges(self, process, **kwargs):
        if self.privacy > 1:
            raise PrivateArchive('Exchange lists are protected')
        p = self._archive.retrieve_or_fetch_entity(process)
        for x in p.exchanges():
            yield x.trim()

    def exchange_values(self, process, flow, direction, termination=None, **kwargs):
        if self.privacy > 0:
            raise PrivateArchive('Exchange values are protected')
        p = self._archive.retrieve_or_fetch_entity(process)
        for x in p.exchange_values(self.get(flow), direction=direction):
            if termination is None:
                yield x
            else:
                if x.termination == termination:
                    yield x

    def inventory(self, process, ref_flow=None, **kwargs):
        if self.privacy > 0:
            raise PrivateArchive('Exchange values are protected')
        p = self._archive.retrieve_or_fetch_entity(process)
        for x in sorted(p.exchanges(reference=ref_flow),
                        key=lambda t: (not t.is_reference, t.direction, t.value or 0.0)):
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
        if self.privacy > 0:
            raise PrivateArchive('Exchange values are protected')
        p = self._archive.retrieve_or_fetch_entity(process)
        xs = [x for x in p.exchanges(reference=ref_flow)
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
        self._catalog.load_lcia_factors(quantity_ref)
        return self._catalog.qdb.do_lcia(quantity_ref, process.inventory(ref_flow=ref_flow),
                                         locale=process['SpatialScope'],
                                         refresh=refresh)

    def traverse(self, fragment, scenario=None, **kwargs):
        frag = self._archive.retrieve_or_fetch_entity(fragment)
        return frag.traversal_entry(scenario, observed=True)

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        self._catalog.load_lcia_factors(quantity_ref)
        fragmentflows = self.traverse(fragment, scenario=scenario, **kwargs)
        return self._frag_flow_lcia(fragmentflows, quantity_ref, refresh=refresh)

    def _frag_flow_lcia(self, fragmentflows, quantity_ref, refresh=False):
        result = LciaResult(quantity_ref)
        for ff in fragmentflows:
            if ff.term.is_null:
                continue

            node_weight = ff.node_weight
            if node_weight == 0:
                continue

            try:
                v = ff.term.score_cache(quantity=quantity_ref, qdb=self._catalog.qdb, refresh=refresh)
            except SubFragmentAggregation:
                v = self._frag_flow_lcia(ff.subfragments, quantity_ref, refresh=refresh)
            if v.total() == 0:
                continue

            if ff.term.direction == ff.fragment.direction:
                # if the directions collide (rather than complement), the term is getting run in reverse
                node_weight *= -1

            result.add_summary(ff.fragment.uuid, ff, node_weight, v)
        return result

