"""
This file includes a collection of functions to facilitate shell-style interaction with archives and
catalogs.  Things like menus, input processing, and etc.
"""
from string import ascii_uppercase
from math import log10, ceil
from eight import input


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
            except ValueError:
                if c.upper() in menu:
                    choice = (None, next(i for i, k in enumerate(menu) if k == c.upper()))
        print('Invalid choice')
    return choice
