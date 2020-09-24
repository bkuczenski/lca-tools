import os
import re

from .lc_resource import LcResource
from lcatools.lcia_engine import LciaDb

from antelope import EntityNotFound
from lcatools.archives.entity_store import local_ref
from lcatools.archives import InterfaceError


class Configurator(object):

    _cat_root = None

    def _add_config(self, config, *args):
        """
        does no validation
        :param config:
        :param args:
        :return:
        """
        self._config[config].add(args)

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
    def from_existing_resource(cls, catalog_root, ref, interface='basic'):
        resource_file = os.path.join(catalog_root, 'resources', ref)
        ress = [k for k in LcResource.from_json(resource_file) if interface in k.interfaces]
        if len(ress) > 1:
            print('Warning: using first of several matching resources')
        cfg = cls(ress[0], catalog_root=catalog_root)
        return cfg

    def __init__(self, resource, catalog_root=None):
        if catalog_root is not None:
            self.catalog_root = catalog_root
        self._ldb = LciaDb.new()
        self._resource = resource
        self._resource.check(self)
        self._config = resource.config

    @property
    def archive(self):
        return self._resource.archive

    @property
    def lcia_engine(self):
        return self._ldb.tm

    def load_all(self):
        self._resource.archive.load_all()

    def add_hint(self, hint_type, local_name, canonical_name):
        return self.add_config('hints', hint_type, local_name, canonical_name)

    def add_config(self, option, *args):
        """
        Add a configuration setting to the resource, and apply it to the archive.
        :param option: the option being configured
        :param args: the arguments, in the proper sequence
        :return: None if archive doesn't support configuration; False if unsuccessful, True if successful
        """
        try:
            cf = self.archive.make_interface('configure')
        except InterfaceError:
            print('No Configure interface')
            return None

        if cf.check_config(option, args):
            if option == 'hints':
                self.lcia_engine.apply_hints(self.archive.catalog_names, [args])
            cf.apply_config({option: {args}})
            self._add_config(option, *args)
            return True
        print('Configuration failed validation.')
        return False

    def _serialize_config(self):
        j = dict()
        for k, v in self._config.items():
            j[k] = sorted([list(g) for g in v], key=lambda x: x[0])
        return j

    def write_to_catalog(self, assign_ref=None, catalog_root=None):
        if catalog_root is None:
            if self.catalog_root is None:
                raise ValueError('Catalog root must be specified')
            catalog_root = self.catalog_root
        resource_root = os.path.join(catalog_root, 'resources')
        if assign_ref is None:
            assign_ref = self._resource.reference
        self._resource.write_to_file(resource_root, assign_ref, apply_config=self._config)

    def check_contexts(self):
        """
        Compares contexts known to the resource with contexts known to the captive LCIA engine.  Prints matching
        contexts. Returns a list of non-matching contexts.
        :return:
        """
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

        '''
        print('Null Contexts:')
        for k in null:
            print(' %s' % k.fullname)
        '''
        return null

    def check_quantities(self):
        """
        Compares quantities known to the resource with quantities known to the captive LCIA engine.  Prints recognized
        quantities. Returns a list of unknown quantities.
        :return:
        """
        valid = []
        null = []
        for q in self.archive.entities_by_type('quantity'):
            try:
                can = self.lcia_engine.get_canonical(q)
                valid.append((q, can))
            except EntityNotFound:
                null.append(q)
        print ('Known Quantities:')
        for k in valid:
            print(' %s ==> %s' % (k[0].name, k[1].name))

        '''
        print('Unrecognized Quantities:')
        for k in null:
            print(' %s' % k)
        '''
        return null

    def unmatched_flowables(self):
        """
        Compares flowables known to the resource with flowables known to the captive LCIA engine.  Returns a list of
        non-matching flowables (does not print known flowables).
        :return:
        """
        null = self._ldb.tm.unmatched_flowables(self._resource.make_interface('index').flowables())
        print('%d Unmatched flowables' % len(null))
        return null
