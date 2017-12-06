from collections import defaultdict

from .basic import BasicImplementation
from lcatools.exchanges import comp_dir
from lcatools.interfaces.iindex import IndexInterface


class NotForeground(Exception):
    pass


class IndexImplementation(BasicImplementation, IndexInterface):
    """
    A CatalogInterface provides basic-level semantic data about entities
    """
    def __init__(self, *args, **kwargs):
        super(IndexImplementation, self).__init__(*args, **kwargs)
        self._terminations = defaultdict(set)
        self._index_terminations()

    def _index_terminations(self):
        """
        This can't be done on add because new processes may get stored before their references are setup.
        This should only be run if the archive is local
        :return:
        """
        for p in self._archive.entities_by_type('process'):
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

    def fragments(self, **kwargs):
        if hasattr(self._archive, 'fragments'):
            # we only want reference fragments
            for f in self._archive.fragments(show_all=False, **kwargs):
                yield f
        else:
            raise NotForeground('The resource does not contain fragments: %s' % self._archive.ref)

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

    def originate(self, flow_ref, direction=None, **kwargs):
        return self.terminate(flow_ref, comp_dir(direction))  # just gets flipped back again in terminate()

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
