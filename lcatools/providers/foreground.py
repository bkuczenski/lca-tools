"""
A foreground archive is an LcArchive that also knows how to access, serialize, and deserialize fragments.
"""
import json
import os
import re

from lcatools.entity_store import to_uuid
from lcatools.entities import LcFragment, entity_types
from lcatools.entity_refs import CatalogRef
from lcatools.implementations import ForegroundImplementation
from lcatools.providers.lc_archive import LcArchive


class AmbiguousReference(Exception):
    pass


class FragmentNotFound(Exception):
    pass


class LcForeground(LcArchive):
    """
    An LcForeground is defined by being anchored to a physical directory, which is used to serialize the non-fragment
    entities.  Also within this directory is a subdirectory called fragments, which is used to store fragments
    individually as files.

    Fragments are "observations" of foreground systems. A flow is observed to pass from / into some reference entity
    and is terminated (other end of the flow) to x, which can either be None (a cutoff), or an entity ID which is
    dereferenced to a context (=> elementary) or a process (=>intermediate).

    A foreground is then a collection of observations of flows passing through processes.

    Foreground models can be constructed flow by flow (observed from unit process inventories
    """
    _entity_types = entity_types
    _ns_uuid_required = None

    def _load_json_file(self, filename):
        with open(filename, 'r') as fp:
            self.load_json(json.load(fp))

    def _key_to_id(self, key):
        """
        Fragments should be permitted to have any desired external_ref -- which means key_to_id needs to store a
        mapping of non-standard external refs to uuids.  This is only because (at the moment) every entity is stored
        in the archive using a uuid as its internal key.

        Open question: should this be a general feature?  currently only NsUuidArchives allow arbitrary external_refs.
        :param key:
        :return:
        """
        i = to_uuid(key)
        if i is None:
            try:
                i = self._ext_ref_mapping[key]
            except KeyError:
                i = None
        return i

    @property
    def _archive_file(self):
        return os.path.join(self.source, 'entities.json')

    @property
    def _fragment_dir(self):
        return os.path.join(self.source, 'fragments')

    def __init__(self, fg_path, catalog=None, **kwargs):
        """

        :param fg_path:
        :param catalog: A foreground archive requires a catalog to deserialize saved fragments. If None, archive will
        still initialize (and will even be able to save fragments) but loading fragments will fail.
        :param ns_uuid: Foreground archives may not use ns_uuids, so any namespace uuid provided will be ignored.
        :param kwargs:
        """
        super(LcForeground, self).__init__(fg_path, **kwargs)
        self._catalog = catalog
        self._ext_ref_mapping = dict()
        if not os.path.isdir(self.source):
            os.makedirs(self.source)
        self.load_all()

    def _fetch(self, entity, **kwargs):
        return self.__getitem__(entity)

    def _load_all(self):
        if os.path.exists(self._archive_file):
            self._load_json_file(self._archive_file)
            self._load_fragments()

    def make_interface(self, iface, privacy=None):
        if iface == 'foreground':
            return ForegroundImplementation(self, privacy=privacy)
        else:
            return super(LcForeground, self).make_interface(iface, privacy=privacy)

    def catalog_ref(self, origin, external_ref, entity_type=None):
        ref = self._catalog.fetch(origin, external_ref)
        if ref is None:
            ref = CatalogRef(origin, external_ref, entity_type=entity_type)
        return ref

    def add(self, entity):
        """
        Reimplement base add to (1) allow fragments, (2) merge instead of raising a key error.
        :param entity:
        :return:
        """
        if entity.entity_type not in entity_types:
            raise ValueError('%s is not a valid entity type' % entity.entity_type)
        try:
            self._add(entity)
        except KeyError:
            # merge incoming entity's properties with existing entity
            current = self[entity.get_uuid()]
            current.merge(entity)

    def _add_children(self, entity):
        if entity.entity_type == 'fragment':
            self.add_entity_and_children(entity.flow)
            for c in entity.child_flows:
                self.add_entity_and_children(c)
        else:
            super(LcForeground, self)._add_children(entity)

    def check_counter(self, entity_type=None):
        super(LcForeground, self).check_counter(entity_type=entity_type)
        if entity_type is None:
            super(LcForeground, self).check_counter(entity_type='fragment')

    def name_fragment(self, frag, name):
        if self[frag.external_ref] is None:
            raise FragmentNotFound(frag)
        k = self._key_to_id(name)
        if k is not None:
            raise ValueError('Name is already taken')
        frag.external_ref = name  # will raise PropertyExists if already set
        self._ext_ref_mapping[name] = frag.uuid

    '''
    Save and load the archive
    '''
    def _do_load(self, fragments):
        for f in fragments:
            frag = LcFragment.from_json(self, f)
            if frag.external_ref != frag.uuid:
                self._ext_ref_mapping[frag.external_ref] = frag.uuid
            self.add(frag)

        for f in fragments:
            frag = self[f['entityId']]
            try:
                frag.finish_json_load(self, f)
            except AttributeError:
                print(f)
                raise

    def _load_fragments(self):
        """
        This must be done in two steps, since fragments refer to other fragments in their definition.
        First step: create all fragments.
        Second step: set reference entities and terminations
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
        self._do_load(fragments)

    def _recurse_frags(self, frag):
        frags = [frag]
        for x in sorted(frag.child_flows, key=lambda z: z.get_uuid()):
            frags.extend(self._recurse_frags(x))
        return frags

    def save_fragments(self, save_unit_scores=True):
        current_files = os.listdir(self._fragment_dir)
        for r in self._fragments():
            frags = [t.serialize(save_unit_scores=save_unit_scores) for t in self._recurse_frags(r)]
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

    def save(self, save_unit_scores=True):
        self.write_to_file(self._archive_file, gzip=False, exchanges=True, characterizations=True, values=True)
        if not os.path.isdir(self._fragment_dir):
            os.makedirs(self._fragment_dir)
        self.save_fragments(save_unit_scores=save_unit_scores)

    def clear_score_caches(self):
        for f in self.entities_by_type('fragment'):
            for s, t in f.terminations():
                t.clear_score_cache()

    '''
    Retrieve + display fragments
    '''
    def _fragments(self, background=None):
        for f in self.entities_by_type('fragment'):
            if f.reference_entity is None:
                if background is None or f.is_background == background:
                    yield f

    def _show_frag_children(self, frag, level=0, show=False):
        level += 1
        for k in frag.child_flows:
            if show:
                print('%s%s' % ('  ' * level, k))
            else:
                yield k
            for j in self._show_frag_children(k, level, show=show):
                yield j

    def fragments(self, *args, show_all=False, background=None, show=False):
        """
        :param : optional first param is filter string-- note: filters only on reference fragments!
        :param show_all: show child fragments as well as reference fragments
        :param background: [None] if True or False, show fragments whose background status is as specified
        :param show: [False] if true, print the fragments instead of returning them
        :return:
        """
        for f in sorted([x for x in self._fragments(background=background)], key=lambda x: x.is_background):
            if len(args) != 0:
                if not bool(re.search(args[0], str(f), flags=re.IGNORECASE)):
                    continue
            if show:
                print('%s' % f)
                if show_all:
                    self._show_frag_children(f, show=show)
            else:
                yield f
                if show_all:
                    for k in self._show_frag_children(f):
                        yield k

    def frag(self, string, strict=True):
        """
        strict=True is slow
        Works as an iterator. If nothing is found, raises StopIteration. If multiple hits are found and strict is set,
        raises Ambiguous Reference.
        :param string:
        :param strict: [True] whether to check for ambiguous reference. if False, return first matching fragment.
        :return:
        """
        if strict:
            k = [f for f in self.fragments(show_all=True) if f.get_uuid().startswith(string.lower())]
            if len(k) > 1:
                for i in k:
                    print('%s' % i)
                raise AmbiguousReference()
            try:
                return k[0]
            except IndexError:
                raise StopIteration
        else:
            return next(f for f in self.fragments(show_all=True) if f.get_uuid().startswith(string.lower()))

    def draw(self, string, **kwargs):
        if not isinstance(string, LcFragment):
            string = self.frag(string)
        string.show_tree(**kwargs)

    '''
    Utilities for finding terminated fragments and deleting fragments
    '''
    def _del_f(self, f):
        print('Deleting %s' % f)
        del self._entities[f.uuid]

    def del_orphans(self, for_real=False):
        """
        self is a foreground archive -- delete
        """
        for f in self._fragments(background=True):
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
            for s, t in i.terminations():
                if t.term_node is frag:
                    yield t

    def linked_terms(self, frag):
        """
        returns a list of terminations that match the input.
        :param frag:
        :return:
        """
        return [f for f in self._find_links(frag)]
