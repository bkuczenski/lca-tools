from lcatools.catalog.entity import EntityInterface


class PrivateArchive(Exception):
    pass


class ForegroundInterface(EntityInterface):
    """
    This provides access to detailed exchange values and computes the exchange relation
    """
    def get(self, eid):
        if self.privacy > 0:
            return self._archive[eid].trim()
        return self._archive[eid]

    def exchanges(self, process):
        if self.privacy > 1:
            raise PrivateArchive('Exchange lists are protected')
        p = self._archive[process]
        for x in p.exchanges():
            yield x.trim()

    def exchange_value(self, process, flow, direction, termination=None):
        if self.privacy > 0:
            raise PrivateArchive('Exchange values are protected')
        p = self._archive[process]
        for x in p.exchange(flow, direction=direction):
            if termination is None:
                yield x
            else:
                if x.termination == termination:
                    yield x

    def exchange_relation(self, process, ref_flow, exch_flow, direction, termination=None):
        if self.privacy > 0:
            raise PrivateArchive('Exchange values are protected')
        p = self._archive[process]
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
