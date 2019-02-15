"""
Functions for creating tables for useful / important comparisons.  These are analogous to charts in that they
are forms of output and it's not clear where they belong.

Lists of tabular outputs:
 * process or fragment Inventory

 * compare process inventories
 * compare allocations of a multioutput process

 * compare LCIA factors for different methods
 * compare an LCIA method with the components of one or more Lcia Results using it

Here's another thing: right now I'm using dynamic grid to show these in the window... but wouldn't it perhaps be
preferable to use pandas? doesn't pandas afford all sorts of useful features, like ...
um...
what is pandas good for again? for working with data frames. Not necessarily for creating data frames.

Most likely, I could modify dynamic_grid to *return* a dataframe instead of drawing a table.
"""

from collections import defaultdict
from pandas import DataFrame


def printable(tup, width=8):
    out = []
    for k in tup:
        if isinstance(k, str):
            out.append(k)
        elif k is None:
            out.append('')
        else:
            try:
                g = '%*.3g' % (width, k)
            except TypeError:
                g = '%*.*s' % (width, width, '----')
            out.append(g)
    return tuple(out)


class BaseTableOutput(object):
    """
    A prototype class for storing and returning tabular information.  This should ultimately be adopted in places
    where dynamic_grids are used, or where TeX or excel tables are produced (like in lca_matrix foreground output
    generators) but for now it is just being used to provide some separation of concerns for the flowables super-grid.

    At the heart is a dict whose key is a 2-tuple of (row signifier, column index).  The row signifier can be any
    hashable object, but the column indices are always sequential. re-ordering columns is something we do not feel
    particularly like enabling at the present time.

    The user creates the table with initialization parameters as desired, and then builds out the table by adding
    columns in sequence.

    The table has an inclusion criterion for the iterables (which could be None)-- if the criterion is met, the object
    is added; if not, it is skipped.  The criterion can change, but (since the table contents are static) this will not
    result in columns being re-iterated.

    Subclasses MAY redefine:
     _returns_sets: determines whether each grid item is singly or multiply valued

    Subclasses MUST implement:
     _near_headings -- column names for left-side headings
     _generate_items(col) -- argument is a column iterable - generates items
     _pull_row_from_item(item) -- argument is one of the objects returned by the column iteration, returns row key
     _extract_data_from_item -- argument is an dict from the grid dict, returns either a dict or an immutable object

    """

    _near_headings = '',  # should be overridden
    _far_headings = '', # should be overridden
    _returns_sets = False

    def _pull_row_from_item(self, item):
        """
        Returns the row tuple from an item, for insertion into the rows set. meant to be overridden
        :param item:
        :return: always a tuple.  default item,
        """
        row = item
        # if not self._returns_sets:
        return row,

    def _pull_note_from_item(self, item):
        """
        Returns the "long" / descriptive text appended to the right-hand side of the table. should return a str.
        Only used if _returns_sets is false (otherwise, the sets indicate the row + subrow labels)
        This is may turn out to be totally silly / pointless.
        :param item:
        :return:
        """
        return ''

    def _generate_items(self, iterable):
        """
        yields the items from a column entry. Meant to be overridden.
        :param iterable:
        :return:
        """
        for item in iterable:
            if self._criterion(item):
                yield item

    def _extract_data_from_item(self, item):
        """
        note: dict item is a list of components
        Determines how to get the data point from the item/list. Meant to be overridden.
        If self._returns_sets is true, should return a dict. Else should return an immutable.
        :param item:
        :return: a string
        """
        return item

    def _header_row(self):
        """
        Returns a tuple of columns for the header row
        :return:
        """
        header = self._near_headings
        for i, _ in enumerate(self._columns):
            header += ('C%d' % i),
        header += self._far_headings  # placeholder for row notes / subitem keys
        return header

    def _build_near_header(self, row, prev):
        the_row = []
        for i, _ in enumerate(self._near_headings):
            if prev is not None:
                if prev[i] == row[i]:
                    the_row.append('""')
                    continue
            the_row.append('%s' % row[i])
        return the_row

    def _build_row(self, row, prev=None):
        """
        Returns a single row as a tuple.
        :param row:
        :param prev: [None] previous row printed (input, not output). Used to suppress header output for repeat entries.
        :return:
        """
        # first build the near header
        the_row = self._build_near_header(row, prev)

        data_keys = set()
        data_vals = []

        # first pass: get all the data / keys
        for i, _ in enumerate(self._columns):
            data = self._extract_data_from_item(self._d[row, i])
            if isinstance(data, dict):
                if not self._returns_sets:
                    raise TypeError('multiple values returned but subclass does not allow them!')
                for k in data.keys():
                    data_keys.add(k)
            data_vals.append(data)

        # second pass: build the sub-table by rows
        if self._returns_sets:
            the_rows = []
            _ftt = True  # first time through
            keys = tuple(sorted(data_keys, key=lambda x: x[-2]))
            for k in keys:
                if not _ftt:
                    the_row = ['' for i in range(len(self._near_headings))]
                for i, _ in enumerate(self._columns):
                    if k in data_vals[i]:
                        the_row.append(data_vals[i][k])
                    else:
                        the_row.append(None)
                the_row.append(k)
                if _ftt:
                    the_row.append(self._notes[row])
                else:
                    the_row.append('')
                the_rows.append(the_row)
                _ftt = False
            return the_rows
        else:
            the_row.extend(data_vals)
            # add notes
            the_row.append(self._notes[row])
            return the_row

    def __init__(self, *args, criterion=None):
        """
        Provide 0 or more positional arguments as data columns; add data columns later with add_column(arg)

        :param args: sequential data columns
        :param criterion: A callable expression that returns true if a given
        """
        self._d = defaultdict(list)

        if callable(criterion):
            self._criterion = criterion
        else:
            if criterion is not None:
                print('Ignoring non-callable criterion')

            self._criterion = lambda x: True

        self._rows = set()  # set of valid keys to dict
        self._notes = dict()
        self._columns = []  # list of columns in the order added
        # a valid reference consists of (x, y) where x in self._rows and y < len(self._columns)

        for arg in args:
            self.add_column(arg)

    def _add_rowitem(self, col_idx, item, row=None):
        if row is None:
            row = self._pull_row_from_item(item)
        self._rows.add(row)
        if row not in self._notes:
            self._notes[row] = self._pull_note_from_item(item)
        self._d[row, col_idx].append(item)

    def add_column(self, arg):
        col_idx = len(self._columns)
        for k in self._generate_items(arg):
            self._add_rowitem(col_idx, k)
        self._columns.append(arg)

    def _sorted_rows(self):
        for row in sorted(self._rows, key=lambda x: tuple([str(k) for k in x])):
            yield row

    def text(self, width=10, hdr_width=24, max_width=112, expanded=True):
        """
        Outputs the table in text format
        :return: nothing.
        """
        header = self._header_row()
        prev = None
        body = []
        width = max(6, width)

        wds = [len(header[i]) for i in range(len(self._near_headings))]

        # determine column widths
        for row in self._sorted_rows():
            prt_row = self._build_row(row, prev=prev)

            if self._returns_sets:
                wds = [min(max(wds[i], len('%s' % prt_row[0][i])), hdr_width) for i in range(len(self._near_headings))]
            else:
                wds = [min(max(wds[i], len('%s' % prt_row[i])), hdr_width) for i in range(len(self._near_headings))]

            body.append(prt_row)
            prev = row

        # build display string
        rem_width = max_width
        fmt = ''
        for i in wds:
            rem_width -= i
            fmt += '%%-%d.%ds ' % (i, i)
            rem_width -= 1

        for i in range(len(self._columns)):
            rem_width -= width
            fmt += '%%-%d.%ds ' % (width, width)
            rem_width -= 1

        if rem_width < 0:
            # uh oh negative rem width: widen freely; set remainder to 10 chars
            max_width -= (rem_width - 10)
            rem_width = 10

        fmt += '%%-%d.%ds' % (rem_width, rem_width)

        if self._returns_sets:
            fmt += ' %s'

        print(fmt % header)
        print('-' * max_width)

        for row in body:
            if self._returns_sets:
                for subrow in row:  # sorted(row, key=lambda x: x[-2])
                    print(fmt % printable(subrow, width=width))
            else:
                print(fmt % printable(row, width=width))

        print(fmt % header)
        print('\nColumns:')
        for i, c in enumerate(self._columns):
            print('C%d: %s' % (i, c))

    def dataframe(self):
        df = DataFrame(columns=self._header_row())
        prev = None
        for row in self._sorted_rows():
            if self._returns_sets:
                for r in self._build_row(row):
                    d = dict(zip(self._header_row(), printable(r)))
                    df = df.append(d, ignore_index=True)
            else:
                d = dict(zip(self._header_row(), printable(self._build_row(row, prev=prev))))
                df = df.append(d, ignore_index=True)
            prev = row

        return df

    def to_excel(self, xl_writer, sheetname, width_scaling=0.75):
        """
        Must supply a pandas XlsxWriter. This routine does not save the document.
        :param xl_writer:
        :param sheetname:
        :param width_scaling:
        :return:
        """
        df = self.dataframe()
        df.to_excel(xl_writer, sheet_name=sheetname)
        sht = xl_writer.sheets[sheetname]

        for k in self._near_headings + self._far_headings:
            ix = df.columns.tolist().index(k) + 1
            mx = max([7, width_scaling * df[k].astype(str).str.len().max()])
            sht.set_column(ix, ix, width=mx)
