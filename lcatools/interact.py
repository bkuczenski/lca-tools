"""
This file includes a collection of functions to facilitate shell-style interaction with archives and
catalogs.  Things like menus, input processing, and etc.
"""
from string import ascii_uppercase
from math import log10, ceil
from eight import input

from itertools import groupby


def ifinput(prompt, default):
    g = input('%s [%s]: ' % (prompt, default))
    if len(g) == 0:
        g = default
    return g


def _pick_list(items, *args):
    """
    enumerates items and asks the user to pick one. Additional options can be provided as positional
     arguments.  The first unique letter of positional arguments is chosen to represent them. If no unique
     letter can be found, something else can be done.

    Returns a, b: if the user selected an item from the list, a = the enumeration index and b = None.
    If the user selected one of the alternate options, a = None and b = the option selected.

    A null entry is not allowed, but if the user types and enters 'None', then None, None is returned.

    options are not case sensitive.

    Example: _pick_list(['one cat', 'two dogs', 'three turtles'], 'abort', 'retry', 'fail')
    will present a menu to the user containing options 0, 1, 2, A, R, F and will return one of the following:
    (0, None)
    (1, None)
    (2, None)
    (None, 'abort')
    (None, 'retry')
    (None, 'fail')
    (None, None)
    :param items:
    :return:
    """
    menu = []
    for arg in args:
        letter = (k.upper() for k in arg + ascii_uppercase)
        mypick = None
        while mypick is None:
            j = next(letter)
            if j not in menu:
                mypick = j
        menu += mypick

    print('Choice Item')
    print('%s %s' % ('=' * 6, '=' * 70))

    field_width = ceil(log10(len(items)))

    for i, k in enumerate(items):
        print(' [%*d]%s %s' % (field_width, i, ' ' * (3 - field_width), k))

    for i, k in enumerate(args):
        print('  (%s)  %s' % (menu[i], k))

    print('%s %s' % ('-' * 6, '-' * 70))

    choice = None
    while choice is None:
        c = input('Enter choice (or "None"): ')
        if c == 'None':
            choice = (None, None)
        else:
            try:
                if int(c) < len(items):
                    choice = (int(c), None)
                    break
            except ValueError:
                if c.upper() in menu:
                    choice = (None, args[next(i for i, k in enumerate(menu) if k == c.upper())])
                    break
        print('Invalid choice')
    return choice


def pick_list(object_list):
    l = sorted(list(object_list), key=lambda x: str(x))
    if len(l) == 1:
        print('(selecting only choice)')
        return l[0]
    print('\nSelect item: ')
    c = _pick_list(l)
    if c == (None, None):
        return None
    return l[c[0]]


def pick_from_groups(groups):
    if len(groups) == 1:
        print('(selecting only choice %s)' % groups[0][0])
        return None
    c = _pick_list(['(%d) %s' % (len(i[1]), i[0]) for i in groups], 'done (keep all)')
    print(c)
    if c == (None, 'done (keep all)') or c == (None, None):
        return None
    else:
        return groups[c[0]][1]


def _group_by(object_list, group_key):
    """
    itertools.groupby wrapper-
    :param object_list: iterable
    :param group_key: lambda expression to sort / group by
    :return: keys, groups where keys is a list of unique group_keys and groups is a list of lists of objects per key
    """
    groups = []
    try:
        object_list = sorted(object_list, key=group_key)
    except TypeError:
        return [(None, object_list)]
    for i, j in groupby(object_list, group_key):
        groups.append((i, list(j)))

    return groups


def _metagroup(entities, func):
    return pick_from_groups(sorted(_group_by(entities, func), key=lambda x: len(x[1]), reverse=True))


def pick_by_tag(entities, tag):
    """
    sort by prevalence
    :param entities:
    :param tag:
    :return:
    """
    def get_tag(ent):
        if tag in ent.keys():
            return ent[tag]
        return '(none)'

    return _metagroup(entities, get_tag) or entities


def pick_by_hier(entities, tag, level):
    def get_cmp(ent):
        if level >= len(ent[tag]):
            return '(none)'
        return ent[tag][level]
    return _metagroup(entities, get_cmp) or entities


def descend_hier(entities, tag):
    level = 0
    while True:
        subset = pick_by_hier(entities, tag, level)
        if subset == entities:
            return entities
        entities = subset
        level += 1


def pick_by_etype(entities):
    return _metagroup(entities, lambda x: x.entity_type)


def select_subset(entities, tags):
    tag = tags.pop(0)
    print('Grouped by %s' % tag)
    subset = pick_by_tag(entities, tag)
    if subset == entities:
        return entities
    if len(tags) > 0:
        # as long as we have tags, narrow the selection
        return select_subset(subset, tags)
    return subset


def flows_by_compartment(flows):
    return descend_hier(flows, 'Compartment')


def filter_processes(processes):
    if len(processes) < 10:
        return pick_list(processes)
    # first, Classifications
    if len([p for p in processes if 'Classifications' in p.keys()]) != 0:
        processes = descend_hier(processes, 'Classifications')
    if len(processes) < 10:
        return pick_list(processes)
    # next IsicClass - NOP if the field is not present
    if len([p for p in processes if 'IsicClass' in p.keys()]) != 0:
        processes = pick_by_tag(processes, 'IsicClass')
    if len(processes) < 10:
        return pick_list(processes)
    processes = pick_by_tag(processes, 'SpatialScope')
    return pick_list(processes)


def filter_flows(flows):
    if len(flows) < 10:
        return pick_list(flows)
    flows = flows_by_compartment(flows)
    return pick_list(flows)


def filter_quantities(quantities):
    quantities = select_subset(quantities, ['Method', 'Category', 'Indicator'])
    return pick_list(quantities)


def pick_one(entities):
    """
    given a list of entities, allow the user to pick one by successive filtering
    :param entities: a list of entities
    :return:
    """
    if len(set([k.entity_type for k in entities])) > 1:
        entities = pick_by_etype(entities)
        if entities is None:
            print('No item selected.')
            return None
    picker = {
        "process": filter_processes,
        "flow": filter_flows,
        "quantity": filter_quantities
    }[entities[0].entity_type]
    return picker(entities)
