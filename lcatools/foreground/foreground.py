
from lcatools.providers.base import LcArchive
from lcatools.entities import LcFlow
import json
import os
from collections import defaultdict


class ForegroundError(Exception):
    pass


FG_TEMPLATE = os.path.join(os.path.dirname(__file__), 'data', 'foreground_template.json')


class BgLciaCache(object):
    def __init__(self):
        self._ref_flow = None
        self._exchange_ref = None

    @property
    def exchange(self):
        return self._exchange_ref

    @exchange.setter
    def exchange(self, value):
        if self._exchange_ref is not None:
            raise ForegroundError('Exchange already set!')
        else:
            self._exchange_ref = value
            self._ref_flow = value.exchange.flow

    def bg_lookup(self, q, location=None):
        if self._ref_flow.factor(q) is not None:
            return self._ref_flow.factor(q)[location]
        else:
            if location is None:
                location = self._exchange_ref.exchange.process['SpatialScope']
            archive = self._exchange_ref.catalog[self._exchange_ref.index]
            result = archive.bg_lookup(self._exchange_ref.exchange.process,
                                       ref_flow = self._ref_flow,
                                       quantities=[q],
                                       location=location)
            factor = result.factor(q)
            if factor is not None:
                self._ref_flow.add_characterization(q, value=factor[location], location=location)
                return factor[location]
            return None

    def serialize(self):
        return {
            "source": self._exchange_ref.catalog.source(self._exchange_ref.index),
            "process": self._exchange_ref.exchange.process.get_uuid(),
            "flow": self._exchange_ref.exchange.flow.get_uuid(),
            "direction": self._exchange_ref.exchange.direction
        }


class BgReference(object):
    """
    A BG Reference is a dict that translates geography to exchange.
    The way it works is:
     - a flow is specified as a background flow- so it's added to the foreground's _background dict.
     - once identified as a background, the BgReference can be given different terminations for different
       geographies.  The termination is created based on the incoming flow's geography.
     - A termination is a CatalogRef and an exchange
     - The BgReference computes LCIA scores by catalog lookup using the bg_lookup method. It can cache these if they
       turn out to be slow.
     - if the background flow instance's direction is opposite the stored ExchangeRef's direction, then the sign of
       the LCIA result is inverted.
    """
    def __init__(self):
        self._geog = defaultdict(BgLciaCache)

    def add_bg_termination(self, location, exchange):
        if location in self._geog.keys():
            raise ForegroundError('Location already terminated for this background flow')
        self._geog[location].exchange = exchange

    def lookup_bg_lcia(self, location, q):
        return self._geog[location].bg_lookup(q, location=location)

    def serialize(self):
        return {k: v.serialize() for k, v in self._geog.items()}


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
    def load(cls, directory, ref=None, **kwargs):
        c = cls(directory, ref=ref, **kwargs)
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
        """
        A Foreground is a regular archive that can store fragments. Plus it has a background, which is a mapping
         from flow to exchange reference.
        :param folder:
        :param ref:
        :param upstream:
        :param quiet:
        :param kwargs:
        """
        if ref is None:
            ref = folder
        self._folder = folder
        self._background = defaultdict(BgReference)
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

    @property
    def _background_file(self):
        return os.path.join(self._folder, 'background.json')

    def add(self, entity):
        try:
            super(ForegroundArchive, self).add(entity)
        except KeyError:
            # merge incoming entity's properties with existing entity
            current = self._entities[self._key_to_id(entity.get_external_ref())]
            print('Merging incoming entity with existing')
            current.merge(entity)

    def save(self):
        self.write_to_file(self._archive_file, gzip=False, exchanges=True, characterizations=True, values=True)
        self.save_fragments()
        self.save_background()

    def save_fragments(self):
        with open(self._fragment_file, 'w') as fp:
            json.dump({'fragments': self.serialize_fragments()}, fp, indent=2)

    def save_background(self):
        with open(self._background_file, 'w') as fp:
            json.dump({'background': self.serialize_background()}, fp, indent=2)

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

    def serialize_background(self):
        return {"background": {k: v.serialize() for k, v in self._background.items()}}

    def fragment_from_json(self, j):
        pass
