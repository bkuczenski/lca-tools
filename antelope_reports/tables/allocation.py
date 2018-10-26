
from .base import BaseTableOutput
from collections import defaultdict


class AllocationGrid(BaseTableOutput):
    """
    This table can be produced dynamically

    """
    _near_headings = 'Direction', 'Ref'  # should be overridden
    _returns_sets = True

    def _generate_items(self, alloc_inv):
        """
        columns should be seeded with allocated exchanges

        :param flow_collection:
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
        return item.direction, False, '; '.join(item.flow['Compartment']), item.flow['Name']

    def _canonical(self, item):
        """
        canonical row name for entry
        :param item:
        :return:
        """
        _p = self._ar.retrieve_or_fetch_entity(item.termination)
        if _p is None:
            print('%s => None' % item.termination)
            return item.termination
        return '[%s] %s' % (_p['SpatialScope'], _p['Name'])

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
            if item.termination is None:
                d[item.flow['Name']] += value
            else:
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
                row = self._pull_row_from_item(k)
                self._rows.add(row)
                self._d[row, col_idx].append(k)
        return count

    def _add_alloc_refs(self, arg, flow=None):
        col_idx = len(self._columns)
        for k in arg.references(flow=flow):
            row = k.direction, True, '; '.join(k.flow['Compartment']), k.flow['Name']
            self._rows.add(row)
            self._d[row, col_idx].append(k)
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
