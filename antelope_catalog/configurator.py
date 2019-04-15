import os
import re

from .lc_resource import LcResource
from lcatools.lcia_engine import LciaDb
from lcatools.contexts import NullContext
from lcatools.archives.entity_store import local_ref


class Configurator(object):

    _config = dict()
    _cat_root = None

    '''Catalog Emulation'''
    def abs_path(self, rel_path):
        if os.path.isabs(rel_path):
            return rel_path
        elif rel_path.startswith('$CAT_ROOT'):
            return re.sub('^\$CAT_ROOT', self.catalog_root, rel_path)
        return os.path.join(self.catalog_root, rel_path)

    @staticmethod
    def cache_file(source):
        return os.path.join('/this/path/does/not/exist', os.path.basename(source))

    @property
    def catalog_root(self):
        return self._cat_root

    @catalog_root.setter
    def catalog_root(self, root):
        if os.path.isdir(root):
            self._cat_root = root
        else:
            raise ValueError('Catalog root does not exist or is not directory: %s' % root)

    '''
    Class Methods for creating new configurations
    '''

    @classmethod
    def new(cls, source, ds_type, **kwargs):
        ref = local_ref(source, prefix='config')
        res = LcResource(ref, source, ds_type, **kwargs)
        return cls(res)

    @classmethod
    def from_download(cls, url, md5sum=None):
        # punt on this because of stupid mkstemp stuff
        pass

    @classmethod
    def from_existing_resource(cls, cat_root, ref, interface='basic'):
        resource_file = os.path.join(cat_root, 'resources', ref)
        ress = [k for k in LcResource.from_json(resource_file) if interface in k.interfaces]
        if len(ress) > 1:
            print('Warning: using first of several matching resources')
        cfg = cls(ress[0])
        cfg.catalog_root = cat_root

    def __init__(self, resource):
        self._ldb = LciaDb.new()
        self._resource = resource
        self._resource.check(self)

    @property
    def archive(self):
        return self._resource.archive

    @property
    def lcia_engine(self):
        return self._ldb.tm

    def load_all(self):
        self._resource.archive.load_all()

    def add_context_hint(self, local_name, canonical_name):
        self._resource.add_context_hint(local_name, canonical_name, catalog=self)

    def write_to_catalog(self, assign_ref=None, cat_root=None):
        if cat_root is None:
            if self.catalog_root is None:
                raise ValueError('Catalog root must be specified')
            cat_root = self.catalog_root
        resource_root = os.path.join(cat_root, 'resources')
        if assign_ref is None:
            assign_ref = self._resource.reference
        self._resource.write_to_file(resource_root, assign_ref)

    def check_contexts(self):
        valid = []
        null = []
        for cx in self._resource.make_interface('index').contexts():
            if self._ldb.tm[cx] is None:
                null.append(cx)
            else:
                valid.append(cx)
        print('Valid Contexts:')
        for k in valid:
            print(' %s ==> %s' % (k.name, repr(self._ldb.tm[k])))

        print('Null Contexts:')
        for k in null:
            print(' %s' % k.fullname)

    def check_flowables(self):
        return self._ldb.tm.unmatched_flowables(self._resource.make_interface('index').flowables())
