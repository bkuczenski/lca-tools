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
    hashable object,

    but the column indices are always sequential. re-ordering columns is something we do not feel
    particularly like enabling at the present time.

    The user creates the table with initialization parameters as desired, and then builds out the table by adding
    columns in sequence.  Each column should have the following attributes:
      __iter__: returns grid entries
      __str__: returns column descriptor (for table legend)

    Each grid entry (object returned by the iterator)


    is expected to be an iterable object (i.e. has an __iter__ attribute).  The
    object should not be a generator because it

    The table has an inclusion criterion for the iterables (which could be None)-- if the criterion is met, the object
    is added; if not, it is skipped.  The criterion can change, but (since the table contents are static) this will not
    result in columns being re-iterated.
    """

    _near_headings = '',  # should be overridden
    _returns_sets = False

    def _pull_row_from_item(self, item):
        """
        Returns the row tuple from an item, for insertion into the rows set. meant to be overridden
        :param item:
        :return: always a tuple.  default item,
        """
        row = item
        if not self._returns_sets:
            if row not in self._notes:
                self._notes[row] = self._pull_note_from_item(item)
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
        Determines how to get the data point from the item. Meant to be overridden.
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
        header += '',  # placeholder for row notes / subitem keys
        return header

    def _build_row(self, row, prev=None):
        """
        Returns a single row as a tuple.
        :param row:
        :param prev: [None] previous row printed (input, not output). Used to suppress header output for repeat entries.
        :return:
        """
        # first build the near header
        the_row = []
        for i, _ in enumerate(self._near_headings):
            if prev is not None:
                if prev[i] == row[i]:
                    the_row.append('""')
                    continue
            the_row.append('%s' % row[i])

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
            keys = tuple(data_keys)
            for k in keys:
                if not _ftt:
                    the_row = ['' for i in range(len(self._near_headings))]
                for i, _ in enumerate(self._columns):
                    if k in data_vals[i]:
                        the_row.append(data_vals[i][k])
                    else:
                        the_row.append(None)
                the_row.append(k)
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
        self._d = defaultdict(lambda: None)

        if callable(criterion):
            self._criterion = criterion
        else:
            if criterion is not None:
                print('Ignoring non-callable criterion')

            def criterion(x):
                return True
            self._criterion = criterion

        self._rows = set()  # set of valid keys to dict
        self._notes = dict()
        self._columns = []  # list of columns in the order added
        # a valid reference consists of (x, y) where x in self._rows and y < len(self._columns)

        for arg in args:
            self.add_column(arg)

    def add_column(self, arg):
        col_idx = len(self._columns)
        for k in self._generate_items(arg):
            row = self._pull_row_from_item(k)
            self._rows.add(row)
            self._d[row, col_idx] = k
        self._columns.append(arg)

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
        for row in sorted(self._rows, key=lambda x: tuple([str(k) for k in x])):
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

        fmt += '%%-%d.%ds' % (rem_width, rem_width)

        print(fmt % header)
        print('-' * max_width)

        for row in body:
            if self._returns_sets:
                for subrow in row:
                    print(fmt % printable(subrow, width=width))
            else:
                print(fmt % printable(row, width=width))

        print(fmt % header)
        print('\nColumns:')
        for i, c in enumerate(self._columns):
            print('C%d: %s' % (i, c))


class FlowablesGrid(BaseTableOutput):
    """
    This table can be produced dynamically

    """
    _near_headings = 'Flowable', 'Compartment'  # should be overridden
    _returns_sets = True

    def _pull_row_from_item(self, item):
        """
        Items are either exchanges, factors, or flattened LciaResult components.
        The first two have a 'flow' attribute
        the last has an 'entity' which should be a flow.
        :param item:
        :return: 2-tuple: flowable name (string), compartment (Compartment)
        """
        if hasattr(item, 'flow'):
            flow = item.flow
        elif hasattr(item, 'entity'):
            flow = item.entity
        else:
            raise TypeError('Cannot find a flow in item of type %s' % type(item))
        return self._qdb.parse_flow(flow)

    def _generate_items(self, flow_collection):
        """
        Columns are: process ref, qty (lcia method) ref, lciaresult, or explicit iterator that provides exchanges,
        factors, or lciaresult components.

        :param flow_collection:
        :return:
        """
        if hasattr(flow_collection, 'flatten'):
            lcia = flow_collection.flatten()
            for component in lcia.components():
                if self._criterion(component.entity):
                    yield component
        elif hasattr(flow_collection, 'entity_type'):
            if flow_collection.entity_type == 'process':
                for exchange in flow_collection.inventory():
                    if self._criterion(exchange.flow):
                        yield exchange
            elif flow_collection.entity_type == 'quantity':
                if flow_collection.is_lcia_method():
                    if self._qdb.is_known(flow_collection):
                        for cf in self._qdb.factors(flow_collection):
                            if self._criterion(cf.flow):
                                yield cf
                    else:
                        for cf in flow_collection.factors():
                            if self._criterion(cf.flow):
                                yield cf
        else:
            # generic iterable-- assume exchange for now
            for x in flow_collection:
                if self._criterion(x.flow):
                    yield x

    def _canonical(self, flow):
        """
        TODO: this is not quite right because parse_flow is too 'fuzzy'-- matches "correction flows" to the actual
        flowables because of CAS number.  still thinking.
        :param flow:
        :return:
        """
        name, _ = self._qdb.parse_flow(flow)
        return '[%s] %s' % (flow.unit(), self._qdb.f_name(name))

    def _extract_data_from_item(self, item):
        """
        We need to return dicts because we have _returns_sets = True.
        Items are either exchanges, factors, or flattened LciaResult components.
        First one is _value_dict (no existing public method to access)
        Second one is _locations (also no existing public method to access)
        Third one has no dict, it's just cumulative_result

        :param item:
        :return:
        """
        d = dict()
        if item is None:
            return d
        if hasattr(item, 'entity_type'):
            if item.entity_type == 'exchange':
                if hasattr(item, '_value_dict'):
                    for rx, v in item._value_dict.items():
                        key = '%s: %s' % (rx.direction, rx.flow['Name'])
                        d[key] = v
                d[self._canonical(item.flow)] = item.value
            elif item.entity_type == 'characterization':
                for loc, v in item._locations.items():
                    if loc == 'GLO':
                        d[self._canonical(item.flow)] = v
                    else:
                        d[loc] = v
            else:
                raise TypeError('Unsure what to do with item %s' % item)
        elif hasattr(item, 'cumulative_result'):
            d[self._canonical(item.entity)] = item.cumulative_result
        else:
            raise TypeError('Unsure what to do with item %s' % item)
        return d

    def __init__(self, qdb, *args, include_flows=None, quell_locations=False):
        """

        :param qdb: Mandatory Qdb for resolving flowables and compartments
        :param args: supply process ref (exchanges), quantity ref (cfs), or lcia result (flows)
        :param include_flows: either None (include all flows) or an iterable of flowables to include
        :param quell_locations:  for characterizations, suppress reporting of locations other than GLO. [not yet impl.]
        """
        self._qdb = qdb
        if include_flows is not None:
            include_fbs = set([qdb.f_index(qdb.parse_flow(x)[0]) for x in include_flows])

            def criterion(x):
                return qdb.f_index(x) in include_fbs
        else:
            criterion = None

        super(FlowablesGrid, self).__init__(*args, criterion=criterion)
