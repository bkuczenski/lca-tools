import os
import json


from lcatools.catalog import CatalogInterface, CatalogRef, CFRef
from lcatools.flowdb.flowdb import FlowDB
from lcatools.providers.foreground import ForegroundArchive

MANIFEST = ('catalog.json', 'entities.json', 'fragments.json', 'flows.json')


class ForegroundManager(object):
    """
    This class is used for building LCA models based on catalog refs.

    It consists of:

     * a catalog containing inventory and LCIA data
     * a Flow-Quantity database

    It manages:
     - adding and loading archives to the catalog
     - searching the catalog

    It maintains:
     - a result set generated from search
     - a select set for comparisons

    The interface subclass provides UI for these activities
    """
    def __init__(self, catalog=None, cfs=('LCIA', 'EI-LCIA'), ):
        import time
        t0 = time.time()
        if catalog is None:
            catalog = CatalogInterface.new()

        self._catalog = catalog
        self._cfs = cfs
        print('Generating flow-quantity database...')
        self._flowdb = FlowDB(catalog)
        self.unmatched_flows = dict()
        if cfs is not None:
            print('Loading LCIA data... (%.2f s)' % (time.time() - t0))
            for c in cfs:
                self._catalog.load(c)
                print('Importing CFs... (%.2f s)' % (time.time() - t0))
                self.unmatched_flows[c] = self._flowdb.import_cfs(c)
                print('%d unmatched flows found from source %s... \n' %
                      (len(self.unmatched_flows[c]), self._catalog.name(c)))

        print('finished... (%.2f s)' % (time.time() - t0))

    def show(self):
        self._catalog.show()

    def load(self, item):
        self._catalog.load(item)

    def save(self):
        self._catalog[0].save()  # nothing else to save

    def __getitem__(self, item):
        return self._catalog.__getitem__(item)

    def search(self, *args, **kwargs):
        return self._catalog.search(*args, **kwargs)

    def terminate(self, *args, **kwargs):
        return self._catalog.terminate(*args, **kwargs)

    def workon(self, folder):
        """
        Select the current foreground.  Create folder if needed.
        If folder/entities.json does not exist, creates and saves a new foreground in folder.
        loads and installs archive from folder/entities.json
        :param folder:
        :return:
        """
        if not os.path.exists(folder):
            os.makedirs(folder)
        if not os.path.exists(os.path.join(folder, 'entities.json')):
            ForegroundArchive.new(folder)
        self._catalog.set_foreground_dir(folder)
        self._catalog.load(0)

    def add_to_foreground(self, ref):
        print('Add to foreground: %s' % ref)
        self._catalog[0].add(ref.entity())

    # inspection methods
    def _filter_exch(self, process_ref, elem=True):
        return [x for x in process_ref.archive.fg_lookup(process_ref.entity())
                if self._flowdb.is_elementary(x.flow) is elem]

    def intermediate(self, process_ref):
        exch = self._filter_exch(process_ref, elem=False)
        if len(exch) == 0:
            print('No intermediate exchanges')
            return
        print('Intermediate exchanges:')
        for i in exch:
            print('%s' % i)

    def elementary(self, process_ref):
        exch = self._filter_exch(process_ref, elem=True)
        if len(exch) == 0:
            print('No elementary exchanges')
            return
        print('Elementary exchanges:')
        for i in exch:
            print('%s' % i)

    def fg_lcia(self, process_ref):
        """
        :param process_ref:
        :return:
        """
        if self._catalog.fg is None:
            print('Missing a foreground!')
            return None
        if not self._catalog.is_loaded(0):
            self._catalog.load(0)
        exch = self._filter_exch(process_ref, elem=True)
        qs = self._catalog[0].lcia_methods()
        if len(qs) == 0:
            print('No foreground LCIA methods')
            return None
        results = dict()
        for q in qs:
            q_result = []
            for x in exch:
                if not x.flow.has_characterization(q):
                    cf_ref = self._flowdb.lookup_single_cf(x.flow, q)
                    if cf_ref is None:
                        x.flow.add_characterization(q)
                    else:
                        x.flow.add_characterization(cf_ref.characterization)
                fac = x.flow.cf(q)
                if fac is not None and fac != 0.0:
                    # TODO: make LCIA results a class (looking toward antelope)
                    q_result.append((x, fac, x.value * fac))
            results[q.get_uuid()] = q_result
        return results


class OldForegroundManager(object):
    """
    The foreground manager is the one-liner that you load to start building and editing LCI foreground models.

    It consists of:
     * a catalog of LcArchives, of which the 0th one is a ForegroundArchive to store fragments;

     * a database of flows, which functions as a FlowQuantity interface - tracks quantities, flowables, compartments

    A foreground is constructed from scratch by giving a directory specification. The directory is used for
    serializing the foreground; the same serialization can be used to invoke an Antelope instance.

    The directory contains:
      - entities.json: the foreground archive
      - fragments.json: serialized FragmentFlows
      - catalog.json: a serialization of the catalog (optional - not necessary if it uses only reference data)

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

    def __getitem__(self, item):
        return self._catalog.__getitem__(item)

    def show(self):
        self._catalog.show()

    def _add_entity(self, index, entity):
        if self._catalog[index][entity.get_uuid()] is None:
            self._catalog[index].add(entity)
        c_r = self._catalog.ref(index, entity)
        if c_r.entity() is not None:
            return c_r
        return None

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
        if len(args) == 1:
            ref = args[0]
            return self._catalog.terminate(ref.index, ref, **kwargs)
        else:
            return self._catalog.terminate(*args, **kwargs)

    def add_catalog(self, ref, ds_type, nick=None, **kwargs):
        self._catalog.load_archive(ref, ds_type, nick=nick, **kwargs)

    def get_flow(self, flow):
        return self._flowdb.flow(flow)

    def get_quantity(self, q):
        return self._flowdb.quantity(q)

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
