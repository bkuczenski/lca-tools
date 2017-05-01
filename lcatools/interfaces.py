"""
An interface is a standard way for accessing resources.  These interfaces provide three levels of information
access to LcArchive information.  The interfaces provide a basis for exposing LcArchive information over the web, and
also function as a control point for introducing access control and privacy protection.

The interface adds a semantic reference to the physical data source referred to in the archive.  When the catalog
interface returns an entity, it translates the entity's physical origin to be semantic origin.  This also has the
effect of abstracting the upstream mechanism in the archives.  In other words: upstreaming is for combining physical
sources within the same semantic context, not for mixing semantic sources (you need a catalog for that).

At the moment the interfaces only deal with elementary LcEntities-- but once a scenario management framework is in
place it is feasible to imagine them used for fragment access as well.
"""
from collections import defaultdict
from lcatools.exchanges import comp_dir


class ForegroundRequired(Exception):
    pass


class BackgroundRequired(Exception):
    pass


def local_ref(source):
    """
    Create a semantic ref for a local filename.  Just uses basename.  what kind of monster would access multiple
    different files with the same basename without specifying ref?

    alternative is splitext(source)[0].translate(maketrans('/\\','..'), ':~') but ugghh...

    Okay, FINE.  I'll use the full path.  WITH leading '.' removed.

    Anyway, to be clear, local semantic references are not supposed to be distributed.
    :param source:
    :return:
    """
    xf = source.translate(str.maketrans('/\\', '..'), ':~')
    while xf[0] == '.':
        xf = xf[1:]
    return '.'.join(['local', xf])


class CatalogInterface(object):
    """
    A CatalogInterface provides basic-level semantic data about entities
    """
    def __init__(self, archive, ref=None):
        """
        Creates a semantic catalog from the specified archive.  Uses archive.get_names() to map data sources to
        semantic references.
        :param archive:
        :param ref: The semantic reference to be used for data sources not found in
        """
        self._archive = archive

        self._terminations = defaultdict(list)
        self._index_archive()

        if ref is None:
            ref = local_ref(archive.source)

        self._ref = ref  # this is used as the semantic origin for any entity whose data source is not found in names

        for v in self._archive.entities():
            v.map_origin(self.names, fallback=self._ref)

    def _index_archive(self):
        for p in self._archive.processes():
            for rx in p.reference_entity:
                self._terminations[rx.flow.external_ref].append((rx.direction, p))

    @property
    def names(self):
        return self._archive.get_names()

    """
    CatalogInterface core methods
    These are the main tools for describing information about the contents of the archive
    """
    def processes(self, **kwargs):
        for p in self._archive.processes(**kwargs):
            yield p.trim()

    def flows(self, **kwargs):
        for f in self._archive.flows(**kwargs):
            yield f.trim()

    def quantities(self, **kwargs):
        for q in self._archive.quantities(**kwargs):
            yield q

    def get(self, eid):
        return self._archive[eid].trim()

    def reference(self, eid):
        return self.get(eid).reference_entity

    def terminate(self, flow, direction=None):
        for x in self._terminations[flow.external_ref]:
            if direction is None:
                yield x[1].trim()
            else:
                if comp_dir(direction) == x[0]:
                    yield x[1].trim()

    def originate(self, flow, direction=None):
        for x in self._terminations[flow.external_ref]:
            if direction is None:
                yield x[1].trim()
            else:
                if direction == x[0]:
                    yield x[1].trim()

    """
    ForegroundInterface core methods: disabled at this level
    """
    def exchanges(self, process):
        raise ForegroundRequired('No access to exchange data')

    def exchange_value(self, process, flow, direction):
        raise ForegroundRequired('No access to exchange data')

    def exchange_relation(self, process, ref_flow, exch_flow, direction):
        raise ForegroundRequired('No access to exchange data')

    """
    BackgroundInterface core methods: disabled at this level; provided by use of a BackgroundManager
    """
    def foreground(self):
        raise BackgroundRequired('No knowledge of background')

    def background(self):
        raise BackgroundRequired('No knowledge of background')

    def exterior(self):
        raise BackgroundRequired('No knowledge of exterior flows')

    def cutoff(self):
        raise BackgroundRequired('No knowledge of cutoff flows')

    def emissions(self):
        raise BackgroundRequired('No knowledge of elementary compartments')

    def lci(self, process):
        raise BackgroundRequired('No knowledge of background system')

    def ref_lci(self, process, ref_flow):
        raise BackgroundRequired('No knowledge of background system')

    def ad(self, process):
        raise BackgroundRequired('No knowledge of background dependencies')

    def ref_ad(self, process, ref_flow):
        raise BackgroundRequired('No knowledge of background dependencies')

    def bf(self, process):
        raise BackgroundRequired('No knowledge of foreground emissions')

    def ref_bf(self, process, ref_flow):
        raise BackgroundRequired('No knowledge of foreground emissions')

    def lcia(self, process, query_qty):
        raise BackgroundRequired('No knowledge of background system')

    def ref_lcia(self, process, ref_flow, query_qty):
        raise BackgroundRequired('No knowledge of background system')


class ForegroundInterface(CatalogInterface):
    """
    This provides access to detailed exchange values and computes the exchange relation
    """
    def get(self, eid):
        """
        don't munge anymore
        :param eid:
        :return:
        """
        return self._archive[eid]

    def exchanges(self, process):
        p = self._archive[process]
        for x in p.exchanges():
            yield x.trim()

    def exchange_value(self, process, flow, direction, termination=None):
        p = self._archive[process]
        for x in p.exchange(flow, direction=direction):
            if termination is None:
                yield x
            else:
                if x.termination == termination:
                    yield x

    def exchange_relation(self, process, ref_flow, exch_flow, direction, termination=None):
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


class BackgroundInterface(ForegroundInterface):
    """
    The BackgroundInterface exposes LCI computation with matrix ordering and inversion, and LCIA computation with
    enclosed private access to a quantity db (optional).
    """
    def __init__(self, archive, ref=None, suppress_foreground=False, qdb=None):
        """

        :param archive:
        :param ref:
        :param suppress_foreground:
        :param qdb:
        """
        super(BackgroundInterface, self).__init__(archive, ref)
