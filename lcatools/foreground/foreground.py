
from lcatools.providers.base import LcArchive
from lcatools.foreground.fragment_flows import LcFragment
from lcatools.exchanges import comp_dir
from lcatools.interact import ifinput
import json
import os
import re


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

    def __init__(self, folder, ref, upstream=None, quiet=True, **kwargs):
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

    def child_flows(self, fragment):
        """
        This is a lambda method used during traversal in order to generate the child fragment flows from
        a given fragment.
        :param fragment:
        :return: fragments listing fragment as parent
        """
        for x in self.fragments(show_all=True):
            if fragment is x.reference_entity:
                yield x

    def _fetch(self, entity, **kwargs):
        raise AttributeError('Foreground archives cannot fetch.')

    @property
    def _archive_file(self):
        return os.path.join(self._folder, 'entities.json')

    @property
    def _fragment_dir(self):
        return os.path.join(self._folder, 'fragments')

    @property
    def catalog_file(self):
        return os.path.join(self._folder, 'catalog.json')

    @property
    def compartment_file(self):
        return os.path.join(self._folder, 'compartments.json')

    @property
    def synonyms_file(self):
        return os.path.join(self._folder, 'synonyms.json')

    def add(self, entity):
        """
        Reimplement base add to merge instead of raising a key error.
        :param entity:
        :return:
        """
        try:
            super(ForegroundArchive, self).add(entity)
        except KeyError:
            # merge incoming entity's properties with existing entity
            current = self[entity.get_uuid()]
            current.merge(entity)

    def save(self):
        self.write_to_file(self._archive_file, gzip=False, exchanges=True, characterizations=True, values=True)
        if not os.path.isdir(self._fragment_dir):
            os.makedirs(self._fragment_dir)
        self.save_fragments()

    def _recurse_frags(self, frag):
        frags = [frag]
        for x in sorted(self.child_flows(frag), key=lambda z: z.get_uuid()):
            frags.extend(self._recurse_frags(x))
        return frags

    def save_fragments(self):
        current_files = os.listdir(self._fragment_dir)
        for r in self._fragments(show_all=False):
            frags = [t.serialize() for t in self._recurse_frags(r)]
            fname = r.get_uuid() + '.json'
            if fname in current_files:
                current_files.remove(fname)
            tgt_file = os.path.join(self._fragment_dir, fname)
            with open(tgt_file, 'w') as fp:
                json.dump({'fragments': frags}, fp, indent=2, sort_keys=True)
        for leftover in current_files:
            if not os.path.isdir(os.path.join(self._fragment_dir, leftover)):
                print('deleting %s' % leftover)
                os.remove(os.path.join(self._fragment_dir, leftover))

    def create_fragment(self, flow, direction, Name=None, exchange_value=1.0, **kwargs):
        """
        flow must present in self._entities.  This method is for creating new fragments- for appending
        fragment Flows (i.e. fragments with parent entries), use add_child_fragment_flow
        :param flow:
        :param direction:
        :param Name: the fragment name (defaults to flow name)
        :param exchange_value:
        :return:
        """
        if Name is None:
            Name = flow['Name']
        f = LcFragment.new(lambda x: self.child_flows(x), Name, flow, direction, exchange_value=exchange_value, **kwargs)
        self.add_entity_and_children(f)
        return f

    def _fragments(self, show_all=False, match=None):
        for f in self._entities_by_type('fragment'):
            if (f.reference_entity is None) or show_all:
                if match is not None:
                    if not bool(re.search(match, f['Name'], flags=re.IGNORECASE)):
                        continue
                yield f

    def fragments(self, background=None, **kwargs):
        if background is not None:
            return sorted([f for f in self._fragments(**kwargs) if f.is_background == background],
                          key=lambda x: x.term.is_null)
        return sorted([f for f in self._fragments(**kwargs)], key=lambda x: (x.is_background, x['Name']))

    def add_child_fragment_flow(self, ff, flow, direction, Name=None, **kwargs):
        if Name is None:
            Name = flow['Name']
        f = LcFragment.new(lambda x: self.child_flows(x), Name, flow, direction, parent=ff, **kwargs)
        self.add_entity_and_children(f)

        return f

    def add_child_ff_from_exchange(self, ff, exchange, **kwargs):
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
            f = LcFragment.new(lambda x: self.child_flows(x), exchange.flow['Name'], exchange.flow, exchange.direction,
                               parent=ff, exchange_value=exchange.value, **kwargs)
            f.terminate(bg)
        except StopIteration:
            f = LcFragment.from_exchange(lambda x: self.child_flows(x), ff, exchange)
        self.add_entity_and_children(f)
        return f

    def add_background_ff_from_fragment(self, fragment):
        bg = self.create_fragment(fragment.flow, fragment.direction, background=True)
        self.add_entity_and_children(bg)
        fragment.shift_terms_to_background(bg)
        return bg

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

    def _do_load(self, catalog, fragments):
        for f in fragments:
            frag = LcFragment.from_json(lambda x: self.child_flows(x), catalog, f)
            self.add(frag)

        for f in fragments:
            frag = self[f['entityId']]
            frag.finish_json_load(catalog, f)

    def load_fragments(self, catalog):
        """
        This must be done in two steps, since fragments refer to other fragments in their definition.
        First step: create all fragments.
        Second step: set reference entities and terminations
        :param catalog:
        :return:
        """
        fragments = []
        if not os.path.exists(self._fragment_dir):
            os.makedirs(self._fragment_dir)
        for file in os.listdir(self._fragment_dir):
            if os.path.isdir(os.path.join(self._fragment_dir, file)):
                continue
            with open(os.path.join(self._fragment_dir, file), 'r') as fp:
                j = json.load(fp)

            fragments.extend(j['fragments'])
        self._do_load(catalog, fragments)

    def _find_cfs(self, quantity):
        for f in self.flows():
            if f.has_characterization(quantity):
                yield f

    def del_quantity(self, quantity):
        """
        This will remove all characterizations for a quantity, and then delete the quantity
        :param quantity:
        :return:
        """
        print('Flows characterized by the quantity:')
        count = False
        for f in self._find_cfs(quantity):
            count = True
            print('%s' % f)
            if quantity is f.reference_entity:
                print('   *** reference quantity *** %s' % f.get_uuid())
        if count:
            if ifinput('Really delete this quantity? y/n', 'y') != 'y':
                print('Aborted.')
                return
        for f in self._find_cfs(quantity):
            f.del_characterization(quantity)
        self._entities.pop(quantity._uuid)
        print('Deleted from foreground.')

    def _del_f(self, f):
        print('Deleting %s' % f)
        del self._entities[f._uuid]

    def del_orphans(self, for_real=False):
        """
        self is a foreground archive
        """
        for f in self.fragments(background=True):
            if f.reference_entity is not None:
                continue
            try:
                next(self._find_links(f))
                print('Found a link for %s' % f)
            except StopIteration:
                print('### Found orphan %s' % f)
                if for_real:
                    self._del_f(f)

    def _find_links(self, frag):
        for i in self.fragments(show_all=True):
            for t in i.terminations():
                if i.termination(t).term_node is frag:
                    yield i.termination(t)

    def linked_terms(self, frag):
        """
        returns a list of terminations that match the input.
        :param frag:
        :return:
        """
        return [f for f in self._find_links(frag)]

    '''
    Load fragments from other folders

    '''
    def import_fragments(self, catalog, fg_dir):
        if not os.path.exists(fg_dir):
            print('No foreground found in specified directory %s' % fg_dir)
        # first need to load flows
        self._load_json_file(os.path.join(fg_dir, 'entities.json'))

        fragments = []
        for file in os.listdir(os.path.join(fg_dir, 'fragments')):
            with open(os.path.join(fg_dir, 'fragments', file), 'r') as fp:
                j = json.load(fp)

            fragments.extend(j['fragments'])
        self._do_load(catalog, fragments)

    def import_fragment(self, catalog, filename):
        """
        load just one fragment from the specified file.
        :param catalog:
        :param filename:
        :return:
        """
        with open(filename) as fp:
            self._do_load(catalog, json.load(fp)['fragments'])
