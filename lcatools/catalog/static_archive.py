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
        """

        self._archive = archive
        self._bg_flag = False

        self._bm = None

        self._terminations = defaultdict(list)
        self._index_archive()

        self._quantities = set()

        if ref is None:
            ref = local_ref(archive.source)

        self._ref = ref  # this is used as the semantic origin for any entity whose data source is not found in names

        for v in self._archive.entities():
            v.map_origin(self.names, fallback=self._ref)

    def _index_archive(self):
        self._terminations = defaultdict(list)  # reset the index
        for p in self._archive.processes():
            for rx in p.reference_entity:
                self._terminations[rx.flow.external_ref].append((rx.direction, p))

    @property
    def names(self):
        mapping = self._archive.get_names()
        mapping[self._ref] = self._ref  # idempotent on eponym (:P)
        return mapping

    @property
    def bg(self):
        if self._bg_flag is False:
            # perform costly operations only when/if required
            self._bg_flag = True
            self._archive.load_all()
            self._index_archive()
            self._bm = BackgroundManager(self)  # resources only accessible from a BackgroundInterface
        return self._bm

    def get(self, item):
        return self._archive[item]

    def is_characterized(self, quantity):
        return quantity in self._quantities

    def processes(self):
        for k in self._archive.processes():
            yield k

    def characterize(self, qdb, quantity, force=False, overwrite=False, locale='GLO'):
        """
        A hook that allows the LcCatalog to lookup characterization values using a supplied quantity db.  Quantities
        that are looked up are added to a list so they aren't repeated.
        :param qdb:
        :param quantity:
        :param force: [False] re-characterize even if the quantity has already been characterized.
        :param overwrite: [False] remove and replace existing characterizations.  (may have no effect if force=False)
        :param locale: ['GLO'] which CF to retrieve
        :return: a list of flows that have been characterized
        """
        chars = []
        if quantity not in self._quantities or force:
            for f in self._archive.flows():
                if f.has_characterization(quantity):
                    if overwrite:
                        f.del_characterization(quantity)
                    else:
                        chars.append(f)
                        continue
                val = qdb.convert(flow=f, query=quantity, locale=locale)
                if val != 0.0:
                    chars.append(f)
                    f.add_characterization(quantity, value=val)
            self._quantities.add(quantity)
        return chars

    def terminate(self, flow_ref, direction=None):
        for x in self._terminations[flow_ref]:  # defaultdict, so no KeyError
            if direction is None:
                yield x[1].trim()
            else:
                if comp_dir(direction) == x[0]:
                    yield x[1].trim()

    def originate(self, flow_ref, direction=None):
        for x in self._terminations[flow_ref]:
            if direction is None:
                yield x[1].trim()
            else:
                if direction == x[0]:
                    yield x[1].trim()

    def mix(self, flow_ref, direction):
        terms = [t for t in self.terminate(flow_ref, direction=direction)]
        flow = self._archive[flow_ref]
        p = LcProcess.new('Market for %s' % flow['Name'], Comment='Auto-generated')
        p.add_exchange(flow, comp_dir(direction), value=float(len(terms)))
        p.add_reference(flow, comp_dir(direction))
        for t in terms:
            p.add_exchange(flow, direction, value=1.0, termination=t.external_ref)
        return p
