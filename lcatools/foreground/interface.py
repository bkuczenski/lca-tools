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
        'characterization': {
            'show locations': []
        },
        'exchange': {
            'terminate': [],
            'originate': []
        },
        'flow': {
            'source': [],
            'sink': [],
            'show characterizations': []
        },
        'process': {
            'intermediate exchanges': lambda x: x.intermediate,
            'elementary exchanges': lambda x: x.elementary,
            'foreground lcia': lambda x: x.fg_lcia,
            'background lcia': []  # lambda x: x.bg_lcia
        },
        'quantity': {
            'flowables': [],
            'factors': []
        }
    }

    choices = {
        # 33 handlers at first count for v0.1 - what, 20 minutes each? 2016-08-04 22:30
        # 13 written (plus a lot of background work), 25 to go... 2016-08-05 13:42
        'Catalog': {
            'show catalog': lambda x: x.show,
            'add archive': lambda x: x.add_archive,
            'load archive': lambda x: x.load_archive,
            'choose archive': lambda x: x.choose_archive,
            'search entities': {
                'processes': lambda x: x.isearch('process'),
                'flows': lambda x: x.isearch('flow'),
                'quantities': lambda x: x.isearch('quantity')
            },
            'browse entities': {
                'processes': lambda x: x.browse_p,
                'flows': lambda x: x.browse_f,
                'quantities': lambda x: x.browse_q
            },
            'selection': {
                'add to foreground': lambda x: x.add_selection,
                'inspect': lambda x: x.inspect,
                'compare': [],
                'unselect': [],
            },
        },
        'FlowDB': {
            'flowables': {
                'search': [],
                'add synonym': [],
                'lookup characterizations': []
            },
            'compartments': {
                'browse': [],
                'add synonym': [],
                'add subcompartment': []
            }
        },
        'Foreground': {
            'work on foreground': lambda x: x.specify_foreground,
            'create flow': [],
            'edit flow': [],
            'add background': [],
        },
        'Fragments': {
            'list fragments': [],
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

    '''
    @staticmethod
    def pass_input(self, func, *args):
        i = input(*args)
        return lambda: func(i)
    '''

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
            self._catalog[int(index)].load()

    def choose_archive(self):
        self.show()
        index = input('Enter choice, 0-%d or "a" to select all:' % len(self._catalog))
        if index.lower() == 'a':
            self._current_archive = None
        else:
            self._current_archive = self._catalog.name(int(index))

    def _prompt_add(self, entity):
        p = ifinput('Add to selection? y/n', 'y')
        if p == 'y':
            self.selected.add(self._catalog.ref(self._current_archive, entity))
        return entity

    def add_selection(self):
        for i in self.selected:
            self.add_to_foreground(i)
        self.selected.clear()

    @staticmethod
    def _narrow_search(result_set):
        key = ifinput('Enter search key', 'Name')
        val = input('Enter search expression (regexp): ')
        n = [r for r in result_set if key in r.keys() and bool(re.search(val, r[key], flags=re.IGNORECASE))]

        if len(n) == 0:
            print('No results')
            return result_set
        else:
            return n

    def _continue_search(self, result_set):
        while 1:
            if len(result_set) == 0:
                print('No results.')
                return
            print('\n')
            if len(result_set) > 20:
                group(result_set)
                i = cyoa('(B) Browse results, (A) select all, (N)arrow search, or (X) abandon search / finished?',
                         'BANX', 'N')
            else:
                show_res(result_set)
                i = cyoa('(S) Select one, (A) select all, (N)arrow search, or (X) abandon search / finished?',
                         'SANX', 'S')

            if i.lower() == 'x':
                return
            elif i.lower() == 'a':
                self.selected = self.selected.union(result_set)
                return
            elif i.lower() == 'n':
                result_set = self._narrow_search(result_set)
            elif i.lower() == 'b':
                pick = pick_one(result_set)
                self._prompt_add(pick)
            else:  # 'b' ad 's' are the same- browse and pick
                pick = pick_one(result_set)
                self.selected.add(pick)

    def isearch(self, etype):
        self._menu_position = self.choices['Catalog']

        string = input('Search term (regex):')
        return lambda: self._continue_search(self.search(self._current_archive, etype, Name=string, show=False))

    '''
    def isearch_p(self):
        self.isearch('process')

    def isearch_f(self):
        self.isearch('flow')

    def isearch_q(self):
        self.isearch('quantity')
    '''

    def ibrowse(self, entities):
        self._menu_position = self.choices['Catalog']

        g = pick_one(entities)
        return self._prompt_add(g)

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

    def specify_foreground(self):
        folder = ifinput('Choose foreground: ', self._catalog.fg)
        self.workon(folder)
