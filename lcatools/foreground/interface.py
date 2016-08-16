import re
from time import sleep

from lcatools.foreground.manager import ForegroundManager, NoLoadedArchives
from lcatools.foreground.fragment_flows import LcFragment
from lcatools.foreground.ui_layout import choices, inspections, comparisons
from lcatools.entities import LcFlow
from lcatools.catalog import ExchangeRef, CFRef
from lcatools.interact import menu_list, pick_list, ifinput, cyoa, get_kv_pair, get_kv_pairs, \
    pick_one, flows_by_compartment, group, pick_compartment, pick_by_etype


dataSourceTypes = ('JSON', 'IlcdArchive', 'IlcdLcia',
                   'EcospoldV1Archive', 'EcospoldV2Archive', 'EcoinventSpreadsheet')


def show_res(res):
    for i, k in enumerate(res):
        print('[%2d] %s ' % (i, k))


class ForegroundInterface(object):
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

    def __init__(self, *args, **kwargs):

        # encapsulate fg manager
        self._m = ForegroundManager(*args, **kwargs)
        # some common / useful shortcuts
        self._catalog = self._m._catalog
        self._flowdb = self._m._flowdb

        self._menu_position = [choices]
        self._current_archive = None
        self._result_set = []
        self.selected = set()
        self._selected_flowable = None
        self._selected_compartment = None
        self._selected_fragment = None

    def _show(self):
        self._m.show(loaded=False)
        return True

    def _choose_archive(self, loaded=True, allow_all=True):
        try:
            self._m.show(loaded=loaded)
        except NoLoadedArchives:
            return -1

        while True:
            item = input('Enter choice, 0-%d%s ("x" to cancel):' % (len(self._catalog)-1,
                                                                    ' or "a" to select all' * allow_all))
            if item.lower() == 'a' and allow_all:
                return None
            elif item.lower() == 'x':
                return -1
            else:
                try:
                    index = int(item)
                except ValueError:
                    try:
                        index = self._catalog.get_index(item)
                    except KeyError:
                        break
                if loaded:
                    if not self._catalog.is_loaded(index):
                        index = None
                if index is not None:
                    return index
            print('Invalid choice.')

    @property
    def _archive(self):
        if self._current_archive is None:
            ar = self._choose_archive(allow_all=False)
            if ar == -1:
                raise NoLoadedArchives
            return self._catalog[ar]
        return self._catalog[self._current_archive]

    def menu(self):
        """
        present the menu pointed to by the current _menu_position- follow subdicts until a handler is encountered

        handlers are callable
        :return:
        """

        pointer = self._menu_position.pop(-1)

        while True:
            print('\n\n%s' % ('#' * 120))

            self._catalog.show_loaded()
            if self._current_archive is None:
                print('\nSearching all loaded archives')
            else:
                print('\nCurrent archive: %s' % self._current_archive)

            if self._selected_flowable is not None:
                print('\n Current Flowable: %.100s' % self._flowdb.flowables[self._selected_flowable])

            if self._selected_compartment is not None:
                print('\n Current Compartment: %s' % self._selected_compartment.to_list())

            if len(self.selected) == 0:
                print(' [[ No entities currently selected ]]')
            else:
                print('\n Current entity selection: ')
                for i in self.selected:
                    print('[[ %s ]]' % i)

            while isinstance(pointer, dict):
                self._menu_position.append(pointer)
                go_up = len(self._menu_position) > 1
                uchoice = menu_list(*[k for k, v in pointer.items() if v != []], go_up=go_up)
                if uchoice == -1 or uchoice is None:
                    if len(self._menu_position) == 1:
                        pointer = 1
                        break
                    self._menu_position.pop(-1)
                    pointer = self._menu_position.pop(-1)
                else:
                    pointer = pointer[uchoice]

            # pointer = 1 is the signal to quit
            if pointer == 1:
                break

            # if pointer is not a dict, it must be callable
            # and it must return a valid pointer string
            message = pointer(self)()

            if message is not True and message is not None:
                print('** %s ** ' % message)
                sleep(0.8)

            pointer = self._menu_position.pop(-1)

        self._m.save()
        # now the fun part- writing the callables
        # callables should specify new menu position to return to

    def add_archive(self):
        print('Add a new archive by reference.')
        source = input('Source: ')
        nick = input('Nickname for this source: ')
        ds_type = pick_list(dataSourceTypes)
        print('Input parameters or blank')
        params = get_kv_pairs('Parameter')
        self._catalog.add_archive(source, [nick], ds_type, **params)
        return True

    def load_archive(self):
        index = self._choose_archive(loaded=False, allow_all=False)
        if index == -1:
            return 'No option selected'
        if self._catalog.is_loaded(index):
            print('Loading all entities')
            self._catalog.load_all(index)
        else:
            self._catalog.load(index)
        return True

    def set_current_archive(self):
        self._current_archive = self._choose_archive(loaded=False, allow_all=True)
        if not self._catalog.is_loaded(self._current_archive):
            self._m.load(self._current_archive)
        return True

    def _prompt_add(self, entity):
        if entity is None:
            return True
        p = ifinput('Add to selection? y/n', 'y')
        if p == 'y':
            self.selected.add(entity)
            return entity
        return True

    def add_selection(self):
        self._menu_position.pop()

        if not self._catalog.is_loaded(0):
            return 'Foreground is not loaded.'
        for i in self.selected:
            self._m.add_to_foreground(i)
        self.selected.clear()
        return True

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
        """
        :param result_set: set of catalog refs
        :return:
        """
        while 1:
            if len(result_set) == 0:
                return 'No results.'
            if len(result_set) > 20:
                group(result_set)
                i = cyoa('\n(B) Browse results, (A) select all, (N)arrow search, or (X) abandon search / finished?',
                         'BANX', 'N')
            else:
                show_res(result_set)
                i = cyoa('\n(S) Select one, (A) select all, (N)arrow search, or (X) abandon search / finished?',
                         'SANX', 'S')

            if i.lower() == 'x':
                return True
            elif i.lower() == 'a':
                self.selected = self.selected.union(result_set)
                return True
            elif i.lower() == 'n':
                result_set = self._narrow_search(result_set)
            elif i.lower() == 'b':
                pick = pick_one(result_set)
                e = self._prompt_add(pick)
                if e in result_set:
                    result_set.remove(e)
            elif i.lower() == 'b' or i.lower() == 's':  # 'b' ad 's' are the same- browse and pick
                pick = pick_one(result_set)
                if pick is not None:
                    self.selected.add(pick)
                    result_set.remove(pick)
            else:
                try:
                    if int(i) < len(result_set):
                        pick = result_set[int(i)]
                        self.selected.add(pick)
                        result_set.remove(pick)
                except ValueError:
                    pass

    def isearch(self, etype):
        self._menu_position = [choices, choices['Catalog']]

        string = input('Search term (regex):')
        return lambda: self._continue_search(self._m.search(self._current_archive, etype, Name=string, show=False))

    '''
    def isearch_p(self):
        self.isearch('process')

    def isearch_f(self):
        self.isearch('flow')

    def isearch_q(self):
        self.isearch('quantity')
    '''

    def ibrowse(self):
        self._menu_position = [choices, choices['Catalog']]
        ar = self._current_archive or self._choose_archive(allow_all=False)
        if ar == -1:
            print('No archives loaded!')
            return []
        entities = [e for e in self._catalog[ar].entities()]
        if len(entities) == 0:
            return lambda: 'No entities found'
        g = pick_one(entities)
        return self._prompt_add(self._catalog.ref(ar, g))

    '''
    def _processes(self):
        ar = self._current_archive or self._choose_archive(allow_all=False)
        if ar == -1:
            print('No archives loaded!')
            return []
        return self._catalog.processes_for(ar)

    def _flows(self):
        return self._catalog.flows_for(ar)

    def _quantities(self):
        ar = self._current_archive or self._choose_archive(allow_all=False)
        if ar == -1:
            print('No archives loaded!')
            return []
        return self._catalog.quantities_for(ar)
    '''

    def inspect(self):
        sel = pick_list(self.selected)
        sel.show()
        while True:
            print('** Select Inspection **')
            uchoice = menu_list(*[k for k, v in inspections[sel.entity_type].items() if v != []], go_up=True)
            if uchoice == -1 or uchoice is None:
                return True
            inspections[sel.entity_type][uchoice](self)(sel)

    def compare(self):
        sel = pick_by_etype(self.selected) or self.selected

    def lcia(self, p_ref):
        self._m.lcia(p_ref)

    def q_lcia(self, p_ref):
        q = pick_one(self._catalog[0].lcia_methods())
        self._m.show_detailed_lcia(p_ref, quantity=q)

    def select_exchange(self, p_ref):
        exch = self._m._filter_exch(p_ref, elem=False)
        g = pick_one(exch)
        self.terminate(ExchangeRef(self._catalog, p_ref.index, g))

    def originate(self, exch_ref):
        z = self._catalog.originate(exch_ref, show=False)
        g = pick_one(z)
        return self._prompt_add(g)

    def terminate(self, exch_ref):
        z = self._catalog.terminate(exch_ref, show=False)
        g = pick_one(z)
        return self._prompt_add(g)

    def source(self, flow_ref):
        z = self._catalog.source(flow_ref)
        g = pick_one(z)
        return self._prompt_add(g)

    def sink(self, flow_ref):
        z = self._catalog.sink(flow_ref)
        g = pick_one(z)
        return self._prompt_add(g)

    def factors(self, q_ref):
        self._flowdb.factors_for_quantity(q_ref.id)

    def specify_foreground(self):
        folder = ifinput('Choose foreground: ', self._catalog.fg)
        self._m.workon(folder)
        return True

    def search_flowables(self):
        regex = input('Enter search term (regex): ')
        hits = sorted(self._flowdb.flowables.search(regex))
        for f in hits[:min([len(hits), 50])]:
            self._flowdb.friendly_flowable(f)
        if len(hits) > 50:
            print('.. %d more results .. ' % (len(hits) - 50))
        i = None
        while i is None:
            i = input('Pick one (or n to narrow search, x to abandon search):')
            if i == 'n':
                r = input('regex: ')
                narrow = self._flowdb.flowables.search(r)
                inter = narrow.intersection(hits)
                if len(inter) == 0:
                    print('No results in intersection.')
                else:
                    hits = inter
                i = None
            elif int(i) in hits:
                print('Selecting flowable %d: %s' % int(i), self._flowdb.flowables[i])
                self._selected_flowable = int(i)
            elif i != 'x':
                print('Invalid choice.')
                i = None
        return True

    def browse_compartments(self):
        comp = pick_compartment(self._flowdb.compartments)
        if comp is None:
            return 'Nothing selected'
        self._selected_compartment = comp
        return '%s' % comp.to_list()

    def view_foreground(self):
        print('Foreground Entities \n\nProcesses:')
        for i, p in enumerate(self._catalog[0].processes()):
            print('%4d (%s) %s' % (i, self._catalog.name(0), p))
        print('\nFlows:')
        for i, f in enumerate(self._catalog[0].flows()):
            print('%4d (%s) %s' % (i, self._catalog.name(0), f))
        print('\nQuantities:')
        for i, q in enumerate(self._catalog[0].quantities()):
            print('%4d (%s) %s' % (i, self._catalog.name(0), q))

    def view_background(self):
        self._catalog[0].fragments(background=True, all=False)

    def create_flow(self):
        name = input('Enter flow name: ')
        cas = ifinput('Enter CAS number (or none): ', '')
        print('Choose reference quantity: ')
        q = pick_one(self._catalog[0].quantities())
        print('Choose compartment:')
        comment = input('Enter comment: ')
        c = pick_compartment(self._flowdb.compartments)
        flow = LcFlow.new(name, q, CasNumber=cas, Compartment=c.to_list(), Comment=comment)
        # flow.add_characterization(q, reference=True)
        self._catalog[0].add(flow)
        return flow

    def edit_flow(self):
        flow = pick_one(self._catalog[0].flows())
        print('Select field to edit:')
        field = menu_list(*flow.keys())
        if field == -1 or field is None:
            return True
        new = ifinput('Enter new value for %s: ' % field, flow[field])
        flow[field] = new

    def list_fragments(self):
        print('Fragments:')
        for i, f in enumerate(self._catalog[0].fragments()):
            print('%4d (%s) %s' % (i, self._catalog.name(0), f))

    def create_fragment(self):
        print('Create fragment.')
        name = input('Name: ')
        k = cyoa('Reference flow: Use (F)oreground flow or (S)earch for flow?', 'FS', 'F')
        if k.lower() == 'f':
            print('Select Reference flow:')
            flow = pick_one(self._catalog[0].flows())
        else:
            self.isearch('flow')()
            flow = pick_one([f for f in self.selected if f.entity_type == 'flow'])
            self._m.add_to_foreground(flow)
            flow = flow.entity()
        print('Direction w.r.t. upstream:')
        direction = menu_list('Input', 'Output')
        print('interface\nname: %s\nflow: %s\ndirn: %s' % (name, flow, direction))
        self._catalog[0].create_fragment(flow, direction, name=name)

    def add_child_fragment(self):
        parent = self._selected_fragment
        k = cyoa('use (N)ew or (E)xisting flow?', 'NE', 'N')
        if k.lower() == 'e':
            print('Select Reference flow:')
            flow = pick_one(self._catalog[0].flows())
            if flow is None:
                print('Canceling child fragment flow')
                return None
        else:
            flow = self.create_flow()
        direction = menu_list('Input', 'Output')
        self._catalog[0].add_child_fragment_flow(parent, flow, direction)
