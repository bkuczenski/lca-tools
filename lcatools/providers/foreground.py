"""
A foreground archive is an LcArchive that also knows how to access, serialize, and deserialize fragments.
"""
import json
import os
import re

from lcatools.providers.base import LcArchive, to_uuid
from lcatools.entities import LcFragment


FG_TEMPLATE = os.path.join(os.path.dirname(__file__), 'data', 'foreground_template.json')


class LcForeground(LcArchive):
    """
    An LcStudy is defined by its being anchored to a physical directory, which is used to serialize the non-fragment
    entities.  Also within this directory is a subdirectory called fragments, which is used to store fragments.
    """
    def _load_json_file(self, filename):
        with open(filename, 'r') as fp:
            self.load_json(json.load(fp))

    def _key_to_id(self, key):
        """
        Fragments should be permitted to have any desired external_ref -- which means key_to_id needs to store a
        mapping of non-standard external refs to uuids.  This is only because (at the moment) every entity is stored
        in the archive using a uuid as its internal key
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

    def __init__(self, fg_path, catalog=None, **kwargs):
        """

        :param fg_path:
        :param catalog: A foreground archive requires a catalog to deserialize saved fragments. If None, archive will
        still initialize (and will even be able to save fragments) but loading fragments will fail.
        :param kwargs:
        """
        super(LcForeground, self).__init__(fg_path, **kwargs)
        self._catalog = catalog
        self._ext_ref_mapping = dict()
        if not os.path.isdir(self.source):
            os.makedirs(self.source)
        if os.path.exists(self._archive_file):
            self._load_json_file(self._archive_file)
            self._load_fragments()
        else:
            self._load_json_file(FG_TEMPLATE)
            self.save()

    def add(self, entity):
        """
        Reimplement base add to merge instead of raising a key error.
        :param entity:
        :return:
        """
        try:
            super(LcForeground, self).add(entity)
        except KeyError:
            # merge incoming entity's properties with existing entity
            current = self[entity.get_uuid()]
            current.merge(entity)

    def save(self):
        self.write_to_file(self._archive_file, gzip=False, exchanges=True, characterizations=True, values=True)
        if not os.path.isdir(self._fragment_dir):
            os.makedirs(self._fragment_dir)
        self.save_fragments()

    @property
    def _archive_file(self):
        return os.path.join(self.source, 'entities.json')

    @property
    def _fragment_dir(self):
        return os.path.join(self.source, 'fragments')

    def _fragments(self, background=None):
        for f in self._entities_by_type('fragment'):
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
            self._show_frag_children(k, level)

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

    def check_counter(self, entity_type=None):
        super(LcForeground, self).check_counter(entity_type=entity_type)
        if entity_type is None:
            super(LcForeground, self).check_counter(entity_type='fragment')

    def name_fragment(self, frag, name):
        k = self._key_to_id(name)
        if k is not None:
            raise ValueError('Name is already taken')
        frag.external_ref = name  # will raise PropertyExists if already set
        self._ext_ref_mapping[name] = frag.uuid

    def _do_load(self, fragments):
        for f in fragments:
            frag = LcFragment.from_json(lambda x: self.child_flows(x), self._catalog, f)
            if frag.external_ref != frag.uuid:
                self._ext_ref_mapping[frag.external_ref] = frag.uuid
            self.add(frag)

        for f in fragments:
            frag = self[f['entityId']]
            frag.finish_json_load(self._catalog, f)

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

    def child_flows(self, fragment):
        """
        This is a lambda method used during traversal in order to generate the child fragment flows from
        a given fragment.
        :param fragment:
        :return: fragments listing fragment as parent
        """
        for x in self._entities_by_type('fragment'):
            if fragment is x.reference_entity:
                yield x

    def _recurse_frags(self, frag):
        frags = [frag]
        for x in sorted(self.child_flows(frag), key=lambda z: z.get_uuid()):
            frags.extend(self._recurse_frags(x))
        return frags

    def save_fragments(self):
        current_files = os.listdir(self._fragment_dir)
        for r in self._fragments():
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

    '''
    Utilities for finding terminated fragments and deleting fragments
    '''
    def _del_f(self, f):
        print('Deleting %s' % f)
        del self._entities[f._uuid]

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

