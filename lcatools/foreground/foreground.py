
from lcatools.providers.base import LcArchive
from lcatools.foreground.fragment_flows import LcFragment
from lcatools.exchanges import comp_dir
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

    Note that the foreground constructor can't load fragments- catalog reference must be passed in
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
    def catalog_file(self):
        return os.path.join(self._folder, 'catalog.json')

    def add(self, entity):
        try:
            super(ForegroundArchive, self).add(entity)
        except KeyError:
            # merge incoming entity's properties with existing entity
            current = self[entity.get_uuid()]
            current.merge(entity)

    def add_entity_and_children(self, entity):
        self.add(entity)
        if entity.entity_type == 'quantity':
            # reset unit strings- units are such a hack
            entity.reference_entity._external_ref = entity.reference_entity._unitstring
        elif entity.entity_type == 'flow':
            # need to import all the flow's quantities
            for cf in entity.characterizations():
                self.add_entity_and_children(cf.quantity)
        elif entity.entity_type == 'process':
            # need to import all the process's flows
            for x in entity.exchanges():
                self.add_entity_and_children(x.flow)
        elif entity.entity_type == 'fragment':
            self.add_entity_and_children(entity.flow)

    def save(self):
        self.write_to_file(self._archive_file, gzip=False, exchanges=True, characterizations=True, values=True)
        self.save_fragments()

    def save_fragments(self):
        with open(self._fragment_file, 'w') as fp:
            json.dump({'fragments': self.serialize_fragments()}, fp, indent=2, sort_keys=True)

    def create_fragment(self, flow, direction, Name=None, **kwargs):
        """
        flow must present in self._entities.  This method is for creating new fragments- for appending
        fragment Flows (i.e. fragments with parent entries), use add_child_fragment_flow
        :param flow:
        :param direction:
        :param Name: the fragment name (defaults to flow name)
        :return:
        """
        if Name is None:
            Name = flow['Name']
        f = LcFragment.new(Name, flow, direction, exchange_value=1.0, **kwargs)
        self.add_entity_and_children(f)
        return f

    def _fragments(self, show_all=False):
        for f in self._entities_by_type('fragment'):
            if (f.reference_entity is None) or show_all:
                yield f

    def fragments(self, background=None, show_all=False):
        if background is not None:
            return [f for f in self._fragments(show_all=show_all) if f.is_background == background]
        return sorted([f for f in self._fragments(show_all=show_all)], key=lambda x: x.is_background)

    def add_child_fragment_flow(self, ff, flow, direction, **kwargs):
        f = LcFragment.new(flow['Name'], flow, direction, parent=ff, **kwargs)
        self.add_entity_and_children(f)

        return f

    def add_child_ff_from_exchange(self, ff, exchange):
        """
        Uses a process intermediate exchange to define a child flow to the process.  If the exchange's termination
        is non-null, then the child exchange will also be terminated.
        Want to use a background flow if one exists already
        :param ff:
        :param exchange:
        :return:
        """
        try:
            bg = next(f for f in self.fragments(background=True) if f.term.terminates(exchange))
            f = LcFragment.new(exchange.flow['Name'], exchange.flow, exchange.direction,
                               parent=ff, exchange_value=exchange.value)
            f.terminate(bg)
        except StopIteration:
            f = LcFragment.from_exchange(ff, exchange)
        self.add_entity_and_children(f)
        return f

    def add_background_ff_from_fragment(self, fragment):
        bg = self.create_fragment(fragment.flow, fragment.direction, background=True)
        self.add_entity_and_children(bg)
        fragment.shift_terms_to_background(bg)

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

    def load_fragments(self, catalog):
        """
        This must be done in two steps, since fragments refer to other fragments in their definition.
        First step: create all fragments.
        Second step: set reference entities and terminations
        :param catalog:
        :return:
        """
        with open(self._fragment_file, 'r') as fp:
            j = json.load(fp)

        for f in j['fragments']:
            frag = LcFragment.from_json(catalog, f)
            self.add(frag)
        for f in j['fragments']:
            frag = self[f['entityId']]
            frag.finish_json_load(catalog, f)

