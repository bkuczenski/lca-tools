class XlDict(object):
    """
    wrapper class for xlrd that exposes a simple pandas-like interface to access tabular spreadsheet data with iterrows.
    """
    @classmethod
    def from_sheetname(cls, workbook, sheetname):
        return cls(workbook.sheet_by_name(sheetname))

    def __init__(self, sheet, nulls=None):
        """

        :param sheet: an xlrd.sheet.Sheet
        :param nulls: What value to assign to empty entries.  Default is to set them to None.  Pass in the desired
        literal
        """
        self._sheet = sheet
        self._nulls = nulls

    def _map_value(self, value):
        if not isinstance(value, str):
            return value
        value = value.strip()
        if value == '':
            return self.nulls
        return value

    @property
    def nulls(self):
        return self._nulls

    @nulls.setter
    def nulls(self, value):
        self._nulls = value

    def iterrows(self):
        """
        Using the first row as a list of headers, yields a dict for each subsequent row using the header names as keys.
        returning index, row for pandas compatibility
        :return:
        """
        _gen = self._sheet.get_rows()
        # grab first row
        d = dict((v.value, k) for k, v in enumerate(next(_gen)))
        index = 0
        for r in _gen:
            index += 1
            yield index, dict((k, self._map_value(r[v].value)) for k, v in d.items())

    def unique_units(self, internal=False):
        """
                unitname = 'unit' if self.internal else 'unitName'
        units = set(_elementary[unitname].unique().tolist()).union(
            set(_intermediate[unitname].unique().tolist()))
        for u in units:
            self._create_quantity(u)

        :param internal:
        :return:
        """
        units = set()
        unitname = 'unit' if internal else 'unitName'
        for index, row in self.iterrows():
            units.add(row[unitname])
        return units
