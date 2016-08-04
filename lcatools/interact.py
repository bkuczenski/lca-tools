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
    l = list(object_list)
    c = _pick_list(l)
    if c == (None, None):
        return None
    return l[c[0]]


def pick_from_group(groups):
    if len(groups) == 1:
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
    for i, j in groupby(sorted(object_list, key=group_key), group_key):
        groups.append((i, list(j)))

    return groups


def group_by_tag(entities, tag):
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

    return sorted(_group_by(entities, get_tag), key=lambda x: len(x[1]), reverse=True)


def group_by_hier(entities, tag, level):
    def get_cmp(ent):
        if level >= len(ent[tag]):
            return '(none)'
        return ent[tag][level]
    return sorted(_group_by(entities, get_cmp), key=lambda x: len(x[1]), reverse=True)


def select_subset(entities, tags):
    tag = tags.pop(0)
    print('Grouped by %s' % tag)
    groups = group_by_tag(entities, tag)
    subset = pick_from_group(groups)
    if subset is None:
        return entities
    if len(tags) > 0:
        # as long as we have tags, narrow the selection
        return select_subset(subset, tags)
    return subset


def flows_by_compartment(flows):
    level = 0
    while True:
        print('Select compartment:')
        groups = group_by_hier(flows, 'Compartment', level)
        subset = pick_from_group(groups)
        if subset is None:
            break
        flows = subset
        level += 1
    return flows
