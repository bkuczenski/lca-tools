from collections import defaultdict

from lcatools.catalog import CFRef, ExchangeRef  # , CatalogRef
from lcatools.characterizations import Characterization
from lcatools.exchanges import Exchange
from lcatools.lcia_results import LciaResult


def _result_to_str(result, width=8):
    if result is None:
        return '%-*.*s ' % (width, width, '  --')
    elif isinstance(result, CFRef):
        res = result.characterization.value
    elif isinstance(result, Characterization):
        res = result.value
    elif isinstance(result, Exchange):
        res = result.value
    elif isinstance(result, ExchangeRef):
        res = result.exchange.value
    elif isinstance(result, LciaResult):
        res = result.total()
    else:
        return '%s ' % ('!' * width)
    return '%*.3g ' % (width, float(res))


def dynamic_grid(comparands, comparators, func, near_label, far_label, returns_sets=False, width=8,
                 suppress_col_list=False):
    """
    Construct a sparse table grid containing the output of a function with respect to two sets of inputs.

    Uses the companion function _result_to_str to format the output (this could be a lambda too, I suppose)

    :param comparands: list of columns (must be indexable)
    :param comparators: row iterator (should be ordered externally)
    :param func: lambda(row, column)
    :param near_label: 2-tuple of (header, lambda(row)) for left-side heading (width of header
      determines width of column)
    :param far_label: 2-tuple of (header, lambda(row)) for right-side heading
    :param returns_sets:
        if the function returns single objects, returns_sets should be False.
        if the function returns sets or lists of objects, returns_sets should be True
        if the function returns a mixture of sets and singles, WHAT, were you RAISED in a BARN?!!
    :param width: column width (default 8)
    :return: NOTHING!
    """
    h_str = '%s  ' % near_label[0]
    near_width = len(near_label[0])

    width = max([6, width])

    n = len(comparands)
    for i in range(n):
        h_str += '|%-*.*s' % (width, width, '   C%d' % i)
    h_str = '%s %s' % (h_str, far_label[0])

    #######
    print('%s' % h_str)
    print('-' * len(h_str))
    #######

    for row in comparators:
        f_str = '%*.*s  ' % (near_width, near_width, near_label[1](row))
        data_sets = defaultdict(set)
        for col in range(n):
            if returns_sets:
                for k in func(row, comparands[col]):
                    data_sets[col].add(k)
            else:
                data_sets[col].add(func(row, comparands[col]))
        max_len = max([len(k) for k in data_sets.values()])
        for col in range(n):
            if len(data_sets[col]) > 0:
                f_str += _result_to_str(data_sets[col].pop(), width=width)
            else:
                f_str += _result_to_str(None, width=width)
        max_len -= 1

        print('%s %s' % (f_str, far_label[1](row)))

        while max_len > 0:
            n_str = '%*.*s  ' % (near_width, near_width, '.')
            for col in range(n):
                if len(data_sets[col]) > 0:
                    n_str += _result_to_str(data_sets[col].pop(), width=width)
                else:
                    n_str += ' ' * (width + 1)
            print('%s ' % n_str)
            max_len -= 1
    if suppress_col_list is False:
        print('%s' % h_str)
        print('\nColumns:')
        for i in range(n):
            print('C%d: %s' % (i, comparands[i]))
