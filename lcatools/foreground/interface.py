import re

from lcatools.foreground.manager import ForegroundManager
from lcatools.interact import menu_list, pick_list, ifinput, cyoa, get_kv_pair, get_kv_pairs, \
    pick_one, flows_by_compartment, group


dataSourceTypes = ('JSON', 'IlcdArchive', 'IlcdLcia',
                   'EcospoldV1Archive', 'EcospoldV2Archive', 'EcoinventSpreadsheet')

def show_res(res):
    for i, k in enumerate(res):
        print('[%2d] %s ' % (i, k))


class ForegroundInterface(ForegroundManager):
    """
    This class stores all the user functions for handling the foreground, all wrapped up in a tidy menu.

    The ForegroundManager takes care of serializing changes as soon as they are made.

    This interface is where we spell out "all the things Kyle would want to do"

    So here it is:
     = background catalog reference:
       - list flows / processes / quantities
       - group by properties, browse + search
       - select a set of entities
       - find terminations / originations
       = flows: list cfs
        - synonyms
        + manage synonyms?
       = processes: list exchanges
        - perform LCIA on selected quantity
     = new foreground (dir)
       = default quantities (ILCD)
       + add selected entities to foreground
       - quantities list
       - background list, of catalog refs
       + create flows-- browse compartments to select
        - name
        - CAS
        + characterizations
         - browse quantities (need ref)

     = create fragments
       - select a flow
       = terminate as foreground or background
       - fg: match terminating process and recurse on exchanges (elementary flows omitted)
       - foreground lcia: all flows net, vs each flow
       * serialize + deserialize fragments

    The UX:
      + Search for entities [type of entities]
      + browse entities by type - process-Classification
      - select single entity
      - fg query
        = ecoinvent- to data
        = ilcd / uslci- straight from catalog
      - fg lcia of quantity
      - tabular output


    so for TOMORROW... list group search + select

    and THEN it's the reporting and the charting- barfigs for that?? gonna have to be that or more python-
    (- feh, day's work to do the TeX)

    Antelope products:
     -processes
     -processflows
     -lciaresults
     -fragments
     -fragmentflows
     -lciaresults

     -flowproperties [lack Method tag]
     -flows

     -stages
     -scenarios
     -params
    """

    inspections = {
        'flow': {
            'terminate': [],
            'generate': [],
            'lookup characterizations': []
        },
        'process': {
            'intermediate exchanges': lambda x: x.intermediate,
            'elementary exchanges': lambda x: x.elementary,
            'foreground lcia': lambda x: x.fg_lcia,
            'background lcia': lambda x: x.bg_lcia
        },
        'quantity': {
            'flowables': [],
            'factors': []
        }
    }

    choices = {
        # 33 handlers at first count for v0.1 - what, 20 minutes each?
        'Catalog': {
            'show catalog': lambda x: x.show,
            'add archive': lambda x: x.add_archive,
            'load archive': lambda x: x.load_archive,
            'choose archive': lambda x: x.choose_archive,
            'search entities': {
                'processes': lambda x: x.isearch_p,
                'flows': lambda x: x.isearch_f,
                'quantities': lambda x: x.isearch_q
            },
            'browse entities': {
                'processes': lambda x: x.browse_p,
                'flows': lambda x: x.browse_f,
                'quantities': lambda x: x.browse_q
            },
            'selection': {
                'add to foreground': [],
                'inspect': lambda x: x.inspect,
                'compare': [],
                'unselect': [],
            },
        },
        'FlowDB': {
            'flowables': {
                'search': [],
                'add synonym': []
            },
            'compartments': {
                'browse': [],
                'add synonym': [],
                'add subcompartment': []
            }
        },
        'Foreground': {
            'create flow': [],
            'edit flow': [],
            'add background': [],
        },
        'Fragments': {
            'create fragment': [],
            'edit fragment': [],
            'fragment flows': [],
            'fragment LCIA': []
        }
    }

    def __init__(self, *args):
        super(ForegroundInterface, self).__init__(*args)
        self._menu_position = [self.choices]
        self._current_archive = None
        self._result_set = []
        self.selected = set()

    @property
    def _archive(self):
        if self._current_archive is None:
            self.choose_archive()
        return self._catalog[self._current_archive]

    def menu(self):
        """
        present the menu pointed to by the current _menu_position- follow subdicts until a handler is encountered

        handlers are callable
        :return:
        """

        self._catalog.show_loaded()
        print('\nCurrent archive: %s' % self._current_archive)
        pointer = self._menu_position.pop(-1)

        while isinstance(pointer, dict):
            self._menu_position.append(pointer)
            uchoice = menu_list(*[k for k, v in pointer.items() if v != []])
            pointer = pointer[uchoice]

        # if pointer is not a dict, it must be callable
        pointer(self)()

        # now the fun part- writing the callables

    def add_archive(self):
        print('Add a new archive by reference.')
        source = input('Source: ')
        nick = input('Nickname for this source: ')
        ds_type = pick_list(dataSourceTypes)
        print('Input parameters or blank')
        params = get_kv_pairs('Parameter')
        self._catalog.add_archive(source, [nick], ds_type, **params)

    def load_archive(self):
        self.show()
        index = input('Enter choice: ')
        if not self._catalog.is_loaded(index):
            self._catalog[index].load()

    def choose_archive(self):
        self.show()
        index = input('Enter choice, 0-%d or "a" to select all:' % len(self._catalog))
        if index.lower() == 'a':
            self._current_archive = None
        else:
            self._current_archive = self._catalog.name(int(index))

    @staticmethod
    def _narrow_search(result_set):
        key = ifinput('Enter search key', 'Name')
        val = input('Enter search expression (regexp): ')
        n = [r for r in result_set if key in r.keys()
             and bool(re.search(val, r[key], flags=re.IGNORECASE))]

        if len(n) == 0:
            print('No results')
            return result_set
        else:
            return n

    def _continue_search(self, result_set):
        while 1:
            if len(result_set) == 0:
                print('No results.')
                return None
            print('\n')
            if len(result_set) > 20:
                group(result_set)
                i = cyoa('(B) Browse results, (A) select all, (N)arrow search, or (X) abandon search?', 'BANX', 'N')
            else:
                show_res(result_set)
                i = cyoa('(S) Select one, (A) select all, (N)arrow search, or (X) abandon search?', 'SANX', 'S')

            if i.lower() == 'x':
                return None
            elif i.lower() == 'a':
                self.selected = self.selected.union(result_set)
                return result_set
            elif i.lower() == 'n':
                result_set = self._narrow_search(result_set)
            else:  # 'b' ad 's' are the same- browse and pick
                pick = pick_one(result_set)
                self.selected.add(pick)
                return pick

    def isearch(self, etype):
        self._menu_position = self.choices['Catalog']

        string = input('Search term (regex):')
        self._continue_search(self.search(self._current_archive, etype, Name=string, show=False))

    def isearch_p(self):
        self.isearch('process')

    def isearch_f(self):
        self.isearch('flow')

    def isearch_q(self):
        self.isearch('quantity')

    def ibrowse(self, entities):
        g = pick_one(entities)
        p = ifinput('Add to selection? y/n', 'y')
        if p == 'y':
            self.selected.add(g)
        return g

    def browse_p(self):
        self.ibrowse(self._archive.processes())

    def browse_f(self):
        self.ibrowse(self._archive.flows())

    def browse_q(self):
        self.ibrowse(self._archive.quantities())

    def inspect(self):
        sel = pick_list(self.selected)

        uchoice = menu_list(*[k for k, v in self.inspections[sel.entity_type].items() if v != []])
        uchoice(self)(sel)

    def _filter_exch(self, process_ref, elem=True):
        return [x for x in process_ref.entity().exchanges() if self._flowdb.is_elementary(x.flow) is elem]

    def intermediate(self, process_ref):
        exch = self._filter_exch(process_ref, elem=False)
        for i in exch:
            print('%s' % i)

    def elementary(self, process_ref):
        exch = self._filter_exch(process_ref, elem=True)
        for i in exch:
            print('%s' % i)

    def fg_lcia(self, process_ref):
        """
        this really belongs at a lower level
        :param process_ref:
        :return:
        """
        exch = self._filter_exch(process_ref, elem=True)
        try:
            qs = self._catalog[0].lcia_methods()
        except TypeError:
            print('No foreground.')
            return None
        if len(qs) == 0:
            print('No foreground LCIA methods')
            return None
        results = dict()
        for q in qs:
            q_result = []
            for x in exch:
                if not x.flow.has_characterization(q):
                    cf = self._flowdb.lookup_single_cf(x.flow, q)
                    if cf is None:
                        x.flow.add_characterization(q)
                    else:
                        x.flow.add_characterization(q, cf.value)
                fac = x.flow.cf(q)
                if fac != 0.0:
                    q_result.append((x, fac, x.value * fac))
            results[q.get_uuid()] = q_result
        return results


