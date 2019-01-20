
from .base import BaseTableOutput
from collections import defaultdict


class AllocationGrid(BaseTableOutput):
    """
    This table can be produced dynamically

    """
    _near_headings = 'Direction', 'Ref'  # should be overridden
    _returns_sets = True
    _far_headings = 'Flow', 'Comment'

    def _generate_items(self, alloc_inv):
        """
        columns should be seeded with allocated exchanges

        :param alloc_inv: the inventory iterable
        :return: allocated exchanges
        """
        for x in alloc_inv:
            if not x.is_reference:
                yield x

    def _pull_row_from_item(self, item):
        """
        :param item: an allocated exchange
        :return: 3-tuple: direction, is_ref (bool), flow name, flow compartment
        """
        row = item.direction, False, '; '.join(item.flow['Compartment']), item.flow['Name']
        if row not in self._notes:
            self._notes[row] = self._pull_note_from_item(item)
        return row

    def _pull_note_from_item(self, item):
        return item.comment

    def _canonical(self, item):
        """
        canonical row name for entry
        :param item:
        :return:
        """
        term = item.termination
        if term is not None:
            term = self._ar.get(item.termination)
        if term is None:
            return '%s %s' % (item.unit, item.flow)
        else:
            return '%s %s' % (item.unit, term._name)

    def _extract_data_from_item(self, objects):
        """
        Each item here is an exchange. The sets are multiple terminations of the same flow.
        :param objects:
        :return:
        """
        d = defaultdict(float)
        if objects is None:
            return d
        for item in objects:
            value = item.value
            if value is None:
                continue
            d[self._canonical(item)] += value
        return d

    def _add_alloc_column(self, col_idx, arg, ref):
        """
        extracts items; only adds a column if there is a nonzero count of items
        :param arg:
        :param ref:
        :return:
        """
        count = False
        for k in self._generate_items(arg.inventory(ref_flow=ref)):
            if k.value is not None:
                count = True
                self._add_rowitem(col_idx, k)
        return count

    def _add_alloc_refs(self, arg, flow=None):
        col_idx = len(self._columns)
        for k in arg.references(flow=flow):
            row = k.direction, True, '; '.join(k.flow['Compartment']), k.flow['Name']
            self._add_rowitem(col_idx, k, row=row)
        self._columns.append(arg)

    def _sorted_rows(self):
        for row in sorted(self._rows, key=lambda x: (not x[1], x[0], x[2], x[3])):
            yield row

    def _build_near_header(self, row, prev):
        the_row = []
        if prev is not None:
            if row[0] == prev[0]:
                the_row.append('""')
            else:
                the_row.append(row[0])
        else:
            the_row.append(row[0])
        if row[1]:
            the_row.append('{*}')
        else:
            the_row.append('')
        return the_row

    def add_column(self, arg):
        col_idx = len(self._columns)
        # add process's native values
        if self._report_unallocated:
            if self._add_alloc_column(col_idx, arg, None):
                self._add_alloc_refs(arg)

        for ref in arg.references():
            col_idx = len(self._columns)
            if self._add_alloc_column(col_idx, arg, ref.flow):
                self._add_alloc_refs(arg, flow=ref.flow)

    def __init__(self, archive, *prefs, report_unallocated=True):
        """

        :param archive:
        :param prefs: one or more process refs
        :param report_unallocated: [True] by default, report process's unallocated values alongside the allocated ones
        """
        self._ar = archive
        self._report_unallocated = bool(report_unallocated)
        super(AllocationGrid, self).__init__(*prefs)

    def to_excel(self, xl_writer, sheetname, width_scaling=0.75):
        super(AllocationGrid, self).to_excel(xl_writer, sheetname, width_scaling=width_scaling)
        ix = self._near_headings.index('Ref') + 1
        center_ref = xl_writer.book.add_format({'align': 'center', 'bold': True})
        xl_writer.sheets[sheetname].set_column(ix, ix, cell_format=center_ref)
