"""
A foreground archive is an LcArchive that also knows how to access, serialize, and deserialize fragments.
"""
import json
import os
import re

from collections import defaultdict

from ..implementations import ForegroundImplementation

from lcatools.archives import BasicArchive, EntityExists, BASIC_ENTITY_TYPES
from lcatools.entities.fragments import LcFragment
from lcatools.entity_refs import CatalogRef


FOREGROUND_ENTITY_TYPES = BASIC_ENTITY_TYPES + ('fragment', )


class AmbiguousReference(Exception):
    pass


class FragmentNotFound(Exception):
    pass


class FragmentMismatch(Exception):
    pass


class NonLocalEntity(Exception):
    """
    Foregrounds should store only local entities and references to remote entities
    """
    pass


class LcForeground(BasicArchive):
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
    _entity_types = set(FOREGROUND_ENTITY_TYPES)
    _ns_uuid_required = None

    def _load_entities_json(self, filename):
        with open(filename, 'r') as fp:
            self.load_from_dict(json.load(fp), jsonfile=filename)

    def _ref_to_key(self, key):
        """
        In a foreground, there are three different ways to retrieve an entity.

        (0) by link.  The master key for each entity is its link, which is its key in _entities.

        (1) by uuid.  fg archives have no nsuuid capabilities, but if an entity has a uuid, it can be retrieved by it.

        (2) by name.  Fragments and any other entities native to the foreground archive can also be retrieved by their
        external ref.  Fragments are the only entities that can be *assigned* a name after creation.  [for this reason,
        Fragments MUST have UUIDs to be used for hashing]

         * _ref_to_key transmits the user's heterogeneous input into a valid key to self._entities, or None
           - if the key is already a known link, it's easy
           - if the key is a custom name, it's almost as easy
           - if the key CONTAINS a uuid known to the db, then a link for an entity with that UUID is
              NONDETERMINISTICALLY returned
         * __getitem__ uses _ref_to_key to translate the users input into a key

        For the thorns around renaming fragments, see self.name_fragment() below

        :param key:
        :return:
        """
        if key in self._entities:
            return key
        elif key in self._ext_ref_mapping:
            return self._ext_ref_mapping[key]
        else:
            uid = self._ref_to_uuid(key)
            if uid in self._uuid_map:
                return next(k for k in self._uuid_map[uid])
        return None

    def _add_ext_ref_mapping(self, entity):
        if entity.external_ref in self._ext_ref_mapping:
            raise EntityExists('External Ref %s already refers to %s' % (entity.external_ref, self[entity.external_ref]))
        self._ext_ref_mapping[entity.external_ref] = entity.link

    def __getitem__(self, item):
        """
        Note: this user-friendliness check adds 20% to the execution time of getitem-- so avoid it if possible
        (use _get_entity directly -- especially now that upstream is now deprecated)
        (note that _get_entity does not get contexts)

        :param item:
        :return:
        """
        if hasattr(item, 'link'):
            item = item.link
        return super(BasicArchive, self).__getitem__(item)

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
        still initialize (and will even be able to save fragments) but non-locally terminated fragments will fail.
        :param ns_uuid: Foreground archives may not use ns_uuids, so any namespace uuid provided will be ignored.
        :param kwargs:
        """
        if catalog is not None:
            kwargs['term_manager'] = kwargs.pop('term_manager', catalog.lcia_engine)
        super(LcForeground, self).__init__(fg_path, **kwargs)
        self._uuid_map = defaultdict(set)
        self._catalog = catalog
        self._ext_ref_mapping = dict()
        self._frags_with_flow = defaultdict(set)

        if not os.path.isdir(self.source):
            os.makedirs(self.source)
        self.load_all()
        self.check_counter('fragment')

    def _fetch(self, entity, **kwargs):
        return self.__getitem__(entity)

    def _load_all(self):
        if os.path.exists(self._archive_file):
            self._load_entities_json(self._archive_file)
            self._load_fragments()

    def make_interface(self, iface):
        if iface == 'foreground':
            return ForegroundImplementation(self)
        else:
            return super(LcForeground, self).make_interface(iface)

    def set_origin(self, origin):
        super(LcForeground, self).set_origin(origin)
        for k in self.entities_by_type('fragment'):
            k._origin = origin

    def catalog_ref(self, origin, external_ref, entity_type=None):
        """
        TODO: make foreground-generated CatalogRefs lazy-loading. This mainly requires removing the expectation of a
        locally-defined reference entity, and properly implementing and using a reference-retrieval process in the
        basic interface.
        :param origin:
        :param external_ref:
        :param entity_type:
        :return:
        """
        # TODO: catalog.fetch is costly because it loads the entire target object
        ref = self._catalog.fetch(origin, external_ref)
        if ref is None:
            ref = CatalogRef(origin, external_ref, entity_type=entity_type)
        return ref

    def _flow_ref_from_json(self, e, external_ref):
        origin = e.pop('origin')
        try:
            ref_qty_uu = e.pop('referenceQuantity')
        except KeyError:
            c = e.pop('characterizations')
            ref_qty_uu = next(cf['quantity'] for cf in c if 'isReference' in cf and cf['isReference'] is True)
        return CatalogRef.from_query(external_ref, self._catalog.query(origin), 'flow', self[ref_qty_uu], **e)

    def _qty_ref_from_json(self, e, external_ref):
        origin = e.pop('origin')
        unitstring = e.pop('referenceUnit')
        q = self._catalog.query(origin)
        ref = CatalogRef.from_query(external_ref, q, 'quantity', unitstring, **e)
        return q.get_canonical(ref)

    def _make_entity(self, e, etype, ext_ref):
        if e['origin'] != self.ref:
            if etype == 'flow':
                return self._flow_ref_from_json(e, ext_ref)
            elif etype == 'quantity':
                return self._qty_ref_from_json(e, ext_ref)
        return super(LcForeground, self)._make_entity(e, etype, ext_ref)

    def _ensure_valid_refs(self, entity):
        if entity.is_entity and entity.origin != self.ref:
            entity = entity.make_ref(self._catalog.query(entity.origin))  # only store foreign entities as refs
            '''
            entity.show()
            print('my ref: %s' % self.ref)
            raise NonLocalEntity(entity)
            '''
        super(LcForeground, self)._ensure_valid_refs(entity)

    def add(self, entity):
        """
        Reimplement base add to (1) use link instead of external_ref, (2) merge instead of raising a key error.
        not sure we really want/need to do the merge thing- we will find out
        :param entity:
        :return:
        """
        if entity.origin is None:
            entity.origin = self.ref  # have to do this now in order to have the link properly defined
        try:
            self._add(entity, entity.link)
        except EntityExists:
            # merge incoming entity's properties with existing entity
            current = self[entity.link]
            current.merge(entity)

        if entity.uuid is not None:
            # self._entities[entity.uuid] = entity
            self._uuid_map[entity.uuid].add(entity.link)

        if entity.origin == self.ref and entity.external_ref != entity.uuid:
            self._add_ext_ref_mapping(entity)

        self._add_to_tm(entity, merge_strategy='distinct')

        if entity.entity_type == 'fragment':
            self._frags_with_flow[entity.flow].add(entity)

    def _add_children(self, entity):
        if entity.entity_type == 'fragment':
            self.add_entity_and_children(entity.flow)
            for c in entity.child_flows:
                self.add_entity_and_children(c)
        else:
            super(LcForeground, self)._add_children(entity)

    def name_fragment(self, frag, name):
        """
        This function is complicated because we have so many dicts:
         _entities maps link to entity
         _ext_ref_mapping maps custom name to link
         _uuid_map maps UUID to a set of links that share the uuid
         _ents_by_type keeps a set of links by entity type

        So, when we name a fragment, we want to do the following:
         - ensure the fragment is properly in the _entities dict
         - ensure the name is not already taken
         - set the fragment entity's name
         * pop the old link and replace its object with the new link in _entities
         * remove the old link and replace it with the new link in the uuid map
         * remove the old link and replace it with the new link in ents_by_type['fragment']
         * add the name with the new link to the ext_ref_mapping
        :param frag:
        :param name:
        :return:
        """
        if frag.external_ref == name:
            return  # nothing to do-- fragment is already assigned that name
        current = self[frag.link]
        if current is not frag:
            if current is None:
                raise FragmentNotFound(frag)
            raise FragmentMismatch('%s\n%s' % (current, frag))
        if self._ref_to_key(name) is not None:
            raise ValueError('Name is already taken: "%s"' % name)
        oldname = frag.link
        frag.external_ref = name  # will raise PropertyExists if already set
        self._add_ext_ref_mapping(frag)
        self._entities[frag.link] = self._entities.pop(oldname)

        self._uuid_map[frag.uuid].remove(oldname)
        self._uuid_map[frag.uuid].add(frag.link)
        self._ents_by_type['fragment'].remove(oldname)
        self._ents_by_type['fragment'].add(frag.link)

    '''
    Save and load the archive
    '''
    def _do_load(self, fragments):
        for f in fragments:
            frag = LcFragment.from_json(self, f)
            # if frag.external_ref != frag.uuid:  # don't need this anymore- auto-added for entities with self.ref
            #     self._add_ext_ref_mapping(frag)
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
        for x in sorted(frag.child_flows, key=lambda z: z.uuid):
            frags.extend(self._recurse_frags(x))
        return frags

    def save_fragments(self, save_unit_scores=False):
        current_files = os.listdir(self._fragment_dir)
        for r in self._fragments():
            frags = [t.serialize(save_unit_scores=save_unit_scores) for t in self._recurse_frags(r)]
            fname = r.uuid + '.json'
            if fname in current_files:
                current_files.remove(fname)
            tgt_file = os.path.join(self._fragment_dir, fname)
            with open(tgt_file, 'w') as fp:
                json.dump({'fragments': frags}, fp, indent=2, sort_keys=True)
        for leftover in current_files:
            if not os.path.isdir(os.path.join(self._fragment_dir, leftover)):
                print('deleting %s' % leftover)
                os.remove(os.path.join(self._fragment_dir, leftover))

    def save(self, save_unit_scores=False):
        self.write_to_file(self._archive_file, gzip=False, characterizations=True, values=True, domesticate=True)
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

    def fragments_with_flow(self, flow):
        for k in self._frags_with_flow[flow]:
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
            k = [f for f in self.fragments(show_all=True) if f.uuid.startswith(string.lower())]
            if len(k) > 1:
                for i in k:
                    print('%s' % i)
                raise AmbiguousReference()
            try:
                return k[0]
            except IndexError:
                raise StopIteration
        else:
            return next(f for f in self.fragments(show_all=True) if f.uuid.startswith(string.lower()))

    def draw(self, string, **kwargs):
        if not isinstance(string, LcFragment):
            string = self.frag(string)
        string.show_tree(**kwargs)

    '''
    Utilities for finding terminated fragments and deleting fragments
    '''
    def delete_fragment(self, frag):
        """
        Need to purge the fragment from:
         _entities
         _ents_by_type
         _ext_ref_mapping
         _uuid_map
         _frags_with_flow
        and correct _counter.
        The fragment is not destroyed- and can be re-added. Deleting child fragments is left to interface code (why?)
        :param frag:
        :return:
        """
        self._entities.pop(frag.link)
        self._ents_by_type['fragment'].remove(frag.link)
        self._counter['fragment'] -= 1
        self._uuid_map.pop(frag.uuid)
        self._ext_ref_mapping.pop(frag.external_ref, None)
        self._frags_with_flow[frag.flow].remove(frag)

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
