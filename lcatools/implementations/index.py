from collections import defaultdict

from .basic import BasicImplementation
from ..interfaces import IndexInterface, comp_dir, CONTEXT_STATUS_


class IndexImplementation(BasicImplementation, IndexInterface):
    """
    A CatalogInterface provides basic-level semantic data about entities

    Only requires the abstract ArchiveImplementation

    Attribute requirements for the archive:
     - everything required by BasicImplementation
     - entities_by_type()
     - count_by_type()
     - search(), including implicitly _narrow_search()
    """
    def __init__(self, *args, cutoffs=False, **kwargs):
        super(IndexImplementation, self).__init__(*args, **kwargs)
        if not hasattr(self._archive, 'ti'):
            self.re_index(cutoffs=cutoffs)

    @property
    def _terminations(self):
        return self._archive.ti

    def re_index(self, cutoffs=False):
        self._archive.ti = defaultdict(set)
        self._index_terminations(cutoffs=cutoffs)

    def _index_terminations(self, cutoffs=False):
        """
        This can't be done on add because new processes may get stored before their references are setup.
        This should only be run if the archive is local
        :param cutoffs: [False] if true, terminations include all cutoff [null-termination] exchanges, not just
          references
         :NOTE: until context refactor there is no consistent usage of terminations, so we will skip this
        :return:
        """
        for p in self._archive.entities_by_type('process'):
            if cutoffs and CONTEXT_STATUS_ == 'new':
                for x in p.exchanges():
                    if x.termination is None:
                        self._terminations[x.flow.external_ref].add((x.direction, p))
            else:
                for rx in p.reference_entity:
                    self._terminations[rx.flow.external_ref].add((rx.direction, p))
    """
    CatalogInterface core methods
    These are the main tools for describing information about the contents of the archive
    """
    def count(self, entity_type, **kwargs):
        return self._archive.count_by_type(entity_type)

    def processes(self, literal=False, **kwargs):
        for p in self._archive.search('process', **kwargs):
            yield p

    def flows(self, **kwargs):
        for f in self._archive.search('flow', **kwargs):
            yield f

    def quantities(self, **kwargs):
        for q in self._archive.search('quantity', **kwargs):
            yield q

    def lcia_methods(self, **kwargs):
        for q in self._archive.search('quantity', **kwargs):
            if q.is_lcia_method():
                yield q

    def terminate(self, flow_ref, direction=None, **kwargs):
        """
        Generate processes in the archive that terminate a given exchange i.e. - have the same flow and a complementary
        direction.  If refs_only is specified, only report processes that terminate the exchange with a reference
        exchange.
        :param flow_ref: flow or flow's external key
        :param direction: [None] filter
        :return:
        """
        if hasattr(self._archive, 'terminate'):
            for x in self._archive.terminate(flow_ref, direction=direction, **kwargs):
                yield x
        else:
            cdir = comp_dir(direction)
            if not isinstance(flow_ref, str):
                flow_ref = flow_ref.external_ref
            for x in self._terminations[flow_ref]:  # defaultdict, so no KeyError
                if direction is None:
                    yield x[1]
                else:
                    if cdir == x[0]:
                        yield x[1]

    '''
    def mix(self, flow_ref, direction):
        if not isinstance(flow_ref, str):
            flow_ref = flow_ref.external_ref
        terms = [t for t in self.terminate(flow_ref, direction=direction)]
        flow = self[flow_ref]
        p = LcProcess.new('Market for %s' % flow['Name'], Comment='Auto-generated')
        p.add_exchange(flow, comp_dir(direction), value=float(len(terms)))
        p.add_reference(flow, comp_dir(direction))
        for t in terms:
            p.add_exchange(flow, direction, value=1.0, termination=t.external_ref)
        return p
    '''

    '''
    def terminate(self, flow, direction=None, **kwargs):
        for p in self._archive.terminate(flow, direction=direction, **kwargs):
            yield self.make_ref(p)

    def originate(self, flow, direction=None, **kwargs):
        for p in self._archive.originate(flow, direction=direction, **kwargs):
            yield self.make_ref(p)

    def mix(self, flow, direction, **kwargs):
        return self._archive.mix(flow, direction, **kwargs)
    '''
