import os
import json


MANIFEST = ('catalog.json', 'entities.json', 'fragments.json', 'flows.json')

from lcatools.catalog import CatalogInterface, CatalogRef
from lcatools.foreground.flow_database import LcFlows
from lcatools.providers.foreground import ForegroundArchive



class ForegroundManager(object):
    """
    The foreground manager is the one-liner that you load to start building and editing LCI foreground models.

    It consists of:
     * a catalog of LcArchives, of which the 0th one is a ForegroundArchive to store fragments;

     * a logical database of flows, which tracks observed exchange values, quantities, and characterization factors.

    A foreground is constructed from scratch by giving a directory specification. The directory is used for
    serializing the foreground; the same serialization can be used to invoke an Antelope instance.

    The directory contains:
      - catalog.json: a serialization of the catalog
      - entities.json: the foreground archive
      - fragments.json: serialized FragmentFlows
      - flows.json: a list of logical flows defined as a list of sets of synonymous catalog references

    The foreground manager directs the serialization process and writes the files, but the components serialize
    and de-serialize themselves.
    """

    def save(self):
        self._catalog['FG'].save()
        self._save(self._catalog.serialize(), 'catalog.json')
        self._save(self._flowdb.serialize(), 'flows.json')

    def _create_new_catalog(self):
        pass

    def _exists(self, item):
        return os.path.exists(os.path.join(self._folder, item))

    def _load(self, item):
        with open(os.path.join(self._folder, item)) as fp:
            return json.load(fp)

    def _save(self, j, item):
        with open(os.path.join(self._folder, item), 'w') as fp:
            json.dump(j, fp, indent=2)

    def _check_entity_files(self):
        """
        This function ensures that entities.json and fragments.json exist-- if entities does not exist,
        creates and serializes a new foreground archive.
        If fragments does not exist, asks the foreground archive to create it.
        :return:
        """
        if self._exists('entities.json'):
            if self._exists('fragments.json'):
                return
            a = ForegroundArchive(self._folder, None)
            a.save_fragments()
            return
        ForegroundArchive.new(self._folder)

    def _create_or_load_catalog(self):
        if self._exists('catalog.json'):
            catalog = CatalogInterface.from_json(self._load('catalog.json'))
        else:
            catalog = CatalogInterface()
            catalog.load_archive(self._folder, 'ForegroundArchive', nick='FG')
        return catalog

    def _create_or_load_flowdb(self):
        if self._exists('flows.json'):
            flowdb = LcFlows.from_json(self._catalog, self._load('flows.json'))
        else:
            flowdb = LcFlows()
            for q in self.fg.quantities():
                flowdb.add_quantity(CatalogRef(self._catalog, 0, q.get_uuid()))
            for f in self.fg.flows():
                flowdb.add_flow(CatalogRef(self._catalog, 0, f.get_uuid()))
        return flowdb

    @property
    def fg(self):
        return self._catalog['FG']

    @property
    def db(self):
        return self._flowdb

    def __init__(self, folder):
        """

        :param folder: directory to store the foreground.
        """
        if not os.path.isdir(folder):
            try:
                os.makedirs(folder)
            except PermissionError:
                print('Permission denied.')

        if not os.path.isdir(folder):
            raise EnvironmentError('Must provide a working directory.')

        self._folder = folder
        self._check_entity_files()
        self._catalog = self._create_or_load_catalog()
        self._catalog.show()
        self._flowdb = self._create_or_load_flowdb()
        self.save()

    def _add_entity(self, index, entity):
        self._catalog[index].add(entity)
        return self._catalog.ref(index, entity)

    def cat(self, i):
        """
        return self._catalog[i]
        :param i:
        :return:
        """
        return self._catalog[i]

    def search(self, *args, **kwargs):
        return self._catalog.search(*args, **kwargs)

    def terminate(self, *args, **kwargs):
        return self._catalog.terminate(*args, **kwargs)

    def add_catalog(self, ref, ds_type, nick=None, **kwargs):
        self._catalog.load_archive(ref, ds_type, nick=nick, **kwargs)

    def foreground_flow(self, cat_ref):
        if cat_ref.entity_type == 'flow':
            new_ref = self._add_entity(0, cat_ref.entity())
            self._flowdb.add_flow(cat_ref)
            self._flowdb.add_ref(cat_ref, new_ref)

    def foreground_quantity(self, cat_ref):
        if cat_ref.entity_type == 'quantity':
            new_ref = self._add_entity(0, cat_ref.entity())
            self._flowdb.add_quantity(cat_ref)
            self._flowdb.add_ref(cat_ref, new_ref)
