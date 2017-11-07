from lcatools.catalog.basic import BasicImplementation, PrivateArchive
from lcatools.interfaces.iinventory import InventoryInterface
from lcatools.lcia_results import LciaResult
from lcatools.terminations import SubFragmentAggregation


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
            return self._archive.lcia(process, ref_flow, quantity_ref, refresh=refresh, **kwargs)
        self._catalog.load_lcia_factors(quantity_ref)
        return self._catalog.qdb.do_lcia(quantity_ref, process.inventory(ref_flow=ref_flow),
                                         locale=process['SpatialScope'],
                                         refresh=refresh)

    def traverse(self, fragment, scenario=None, **kwargs):
        if hasattr(self._archive, 'traverse'):
            return self._archive.traverse(fragment, scenario=scenario, **kwargs)
        frag = self._archive.retrieve_or_fetch_entity(fragment)
        return frag.traverse(scenario, observed=True)

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        if hasattr(self._archive, 'fragment_lcia'):
            return self._archive.fragment_lcia(fragment, quantity_ref, scenario=scenario, refresh=refresh, **kwargs)
        self._catalog.load_lcia_factors(quantity_ref)
        fragmentflows = self.traverse(fragment, scenario=scenario, **kwargs)
        return self._frag_flow_lcia(fragmentflows, quantity_ref, scenario=scenario, refresh=refresh)

    def _frag_flow_lcia(self, fragmentflows, quantity_ref, scenario=None, refresh=False):
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
                # if we were given interior fragments, recurse on them. otherwise ask remote.
                if len(ff.subfragments) == 0:
                    v = ff.term.term_node.fragment_lcia(quantity_ref, scenario=scenario, refresh=refresh)
                else:
                    v = self._frag_flow_lcia(ff.subfragments, quantity_ref, refresh=refresh)
            if v.total() == 0:
                continue

            if ff.term.direction == ff.fragment.direction:
                # if the directions collide (rather than complement), the term is getting run in reverse
                node_weight *= -1

            result.add_summary(ff.fragment.uuid, ff, node_weight, v)
        return result
