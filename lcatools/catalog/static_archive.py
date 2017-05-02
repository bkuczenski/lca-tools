from collections import defaultdict
from background.background_manager import BackgroundManager

from lcatools.entities.processes import LcProcess
from lcatools.exchanges import comp_dir


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


class StaticArchive(object):
    """
    A read-only, indexed version of an LcArchive
    """

    def __init__(self, archive, ref=None):
        """

        :param archive: an LcArchive
        :param ref: The semantic reference to be used for data sources not found in the archives catalog_names
        :param ref:
        """

        self._archive = archive
        self._terminations = defaultdict(list)
        self._index_archive()

        self._bm = BackgroundManager(self)  # resources only accessible from a BackgroundInterface

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

    @property
    def bg(self):
        return self._bm

    def terminate(self, flow, direction=None):
        for x in self._terminations[flow.external_ref]:  # defaultdict, so no KeyError
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

    def mix(self, flow, direction):
        terms = [t for t in self.terminate(flow, direction=direction)]
        p = LcProcess.new('Market for %s' % flow['Name'], Comment='Auto-generated')
        p.add_exchange(flow, comp_dir(direction), value=float(len(terms)))
        p.add_reference(flow, comp_dir(direction))
        for t in terms:
            p.add_exchange(flow, direction, value=1.0, termination=t.external_ref)
        return p
