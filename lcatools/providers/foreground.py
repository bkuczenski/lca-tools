
from lcatools.providers.base import LcArchive
from lcatools.entities import LcFlow
import json
import os


class ForegroundError(Exception):
    pass


FG_TEMPLATE = os.path.join(os.path.dirname(__file__), 'data', 'foreground_template.json')


class ForegroundArchive(LcArchive):
    """
    A foreground archive stores entities used within the foreground of a fragment model.  Its main useful
    trick is to allow the creation and de/serialization of fragments, and to represent fragments as processes.

    Foreground archives are not supposed to have upstreams-- instead, the user finds entities in the catalog and
     adds them to the foreground archive.  If those entities are found "correctly", they should arrive with their
     characterizations / exchanges intact.

    When a foreground is used in an antelope instance, only the contents of the foreground archive are exposed.
     This means that the foreground archive should include all flows that are used in fragment flows, and all
     processes that terminate fragment flows.
    """
    @classmethod
    def new(cls, directory, ref=None):
        """
        Create a new foreground and store it in the specified directory. The foreground is pre-loaded with about
        20 quantities (drawn from the ILCD collection) for easy use.

        This method does not return anything- the deliverable is creating the files.
        :param directory:
        :param ref:
        :return:
        """
        c = cls(directory, ref=ref, quiet=True)
        c._load_json_file(FG_TEMPLATE)
        c.save()

    @classmethod
    def load(cls, directory, ref=None):
        c = cls(directory, ref=ref)
        c._load_json_file(c._archive_file)
        c._load_fragments()
        return c

    def _load_json_file(self, filename):
        with open(filename, 'r') as fp:
            j = json.load(fp)

        for q in j['quantities']:
            self.entity_from_json(q)
        for q in j['flows']:
            self.entity_from_json(q)
        for q in j['processes']:
            self.entity_from_json(q)

    def _load_fragments(self):
        with open(self._fragment_file, 'r') as fp:
            j = json.load(fp)

        for f in j['fragments']:
            self.fragment_from_json(f)

    def __init__(self, folder, ref, upstream=None, quiet=False, **kwargs):
        if ref is None:
            ref = folder
        self._folder = folder
        if upstream is not None:
            raise ForegroundError('Foreground archive not supposed to have an upstream')
        super(ForegroundArchive, self).__init__(ref, quiet=quiet, **kwargs)

    def _fetch(self, entity, **kwargs):
        raise AttributeError('Foreground archives cannot fetch.')

    @property
    def _archive_file(self):
        return os.path.join(self._folder, 'entities.json')

    @property
    def _fragment_file(self):
        return os.path.join(self._folder, 'fragments.json')

    def save(self):
        self.write_to_file(self._archive_file, gzip=False, exchanges=True, characterizations=True, values=True)
        self.save_fragments()

    def save_fragments(self):
        with open(self._fragment_file, 'w') as fp:
            json.dump({'fragments': self.serialize_fragments()}, fp, indent=2)

    def create_fragment(self, flow, direction):
        """
        flow must present in self._entities.  This method is for creating new fragments- for appending
        fragment Flows (i.e. fragments with parent entries), use add_child_fragment_flow
        :param flow:
        :param direction:
        :return:
        """
        pass

    def add_child_fragment_flow(self, ff, flow, direction):
        pass

    def check_counter(self, entity_type=None):
        super(ForegroundArchive, self).check_counter(entity_type=entity_type)
        if entity_type is None:
            super(ForegroundArchive, self).check_counter(entity_type='fragment')

    def serialize_fragments(self, **kwargs):
        """
        writes serialized fragments
        :return:
        """
        return [f.serialize(**kwargs) for f in self._entities_by_type('fragment')]

    def fragment_from_json(self, j):
        pass
