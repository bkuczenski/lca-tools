import re
from time import sleep

from lcatools.foreground.manager import ForegroundManager
from lcatools.interact import menu_list, pick_list, ifinput, cyoa, get_kv_pairs, \
    pick_one, group  # , flows_by_compartment, pick_compartment, pick_by_etype, get_kv_pair


dataSourceTypes = ('JSON', 'IlcdArchive', 'IlcdLcia',
                   'EcospoldV1Archive', 'EcospoldV2Archive', 'EcoinventSpreadsheet')


def show_res(res):
    for i, k in enumerate(res):
        print('[%2d] %s ' % (i, k))


class ArchiveUi(object):
    """
    This class stores all the user functions for handling the foreground, all wrapped up in a tidy menu.
    """
    def __init__(self, *args, **kwargs):

        # encapsulate fg manager
        self._m = ForegroundManager(*args, **kwargs)
        # some common / useful shortcuts
        self._catalog = self._m._catalog
        self._flowdb = self._m.db
        self._menu_position = []

    def _show(self):
        self._m.show(loaded=False)
        return True

    def _menu_leader(self):
        print('## MENU ##')

    def menu(self):
        """
        present the menu pointed to by the current _menu_position- follow subdicts until a handler is encountered

        handlers are callable
        :return:
        """

        pointer = self._menu_position.pop(-1)

        while True:
            self._menu_leader()

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
                self.selected.add(pick)
                if pick in result_set:
                    result_set.remove(pick)
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
