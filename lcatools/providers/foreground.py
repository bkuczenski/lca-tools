"""
A foreground archive is an LcArchive that also knows how to access, serialize, and deserialize fragments.
"""
import json
import os
import re

from lcatools.providers.base import LcArchive
from lcatools.entities import LcFragment


FG_TEMPLATE = os.path.join(os.path.dirname(__file__), 'data', 'foreground_template.json')


class LcStudy(LcArchive):
    """
    An LcStudy is defined by its being anchored to a physical directory, which is used to serialize the non-fragment
    entities.  Also within this directory is a subdirectory called fragments, which is used to store fragments.
    """
    def _load_json_file(self, filename):
        with open(filename, 'r') as fp:
            self.load_json(json.load(fp))

    def __init__(self, fg_path, **kwargs):
        super(LcStudy, self).__init__(fg_path, **kwargs)
        if not os.path.isdir(self.source):
            os.makedirs(self.source)
        if os.path.exists(self._archive_file):
            self._load_json_file(self._archive_file)
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
            super(LcStudy, self).add(entity)
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

    def check_counter(self, entity_type=None):
        super(LcStudy, self).check_counter(entity_type=entity_type)
        if entity_type is None:
            super(LcStudy, self).check_counter(entity_type='fragment')

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
