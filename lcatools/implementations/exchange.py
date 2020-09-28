from antelope import ExchangeInterface
from .basic import BasicImplementation


class ExchangeImplementation(BasicImplementation, ExchangeInterface):
    """
    This provides access to detailed exchange values and computes the exchange relation.
    Creates no additional requirements on the archive.
    """
    def exchanges(self, process, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        for x in p.exchanges():
            yield x

    def exchange_values(self, process, flow, direction=None, termination=None, reference=None, **kwargs):
        if reference is True:
            for x in self.get_reference(process):
                if x.flow.external_ref == flow:
                    yield x
        else:
            p = self._archive.retrieve_or_fetch_entity(process)
            for x in p.exchange_values(self.get(flow), direction=direction):
                if reference is False and x.is_reference:
                    continue
                if termination is None:
                    yield x
                else:
                    if x.termination == termination:
                        yield x

    def inventory(self, process, ref_flow=None, scenario=None, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        if p.entity_type == 'process':
            '''
            for x in sorted(p.inventory(ref_flow=ref_flow),
                            key=lambda t: (not t.is_reference, t.direction, t.value or 0.0)):
            '''
            for x in p.inventory(ref_flow=ref_flow, **kwargs):
                yield x
        elif p.entity_type == 'fragment':
            for x in p.inventory(scenario=scenario, observed=True, **kwargs):
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
        norm = p.reference(ref_flow)
        if termination is None:
            xs = [x for x in p.exchange_values(flow=exch_flow, direction=direction)]
            if len(xs) == 1:
                return xs[0].value / norm.value
            elif len(xs) == 0:
                return 0.0
            else:
                return sum([x.value for x in xs]) / norm.value
        else:
            x = p.get_exchange(hash((p.external_ref, exch_flow, direction, termination)))
            return x[norm]
