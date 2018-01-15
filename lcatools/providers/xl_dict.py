from lcatools.providers.base import NsUuidArchive


class XlDict(object):
    """
    wrapper class for xlrd that exposes a simple pandas-like interface to access tabular spreadsheet data with iterrows.
    """
    @classmethod
    def from_sheetname(cls, workbook, sheetname):
        return cls(workbook.sheet_by_name(sheetname))

    def __init__(self, sheet):
        """

        :param sheet: an xlrd.sheet.Sheet
        """
        self._sheet = sheet

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
            yield index, dict((k, r[v].value) for k, v in d.items())

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


class XlsArchive(NsUuidArchive):
    """
    A specialization of NsUUID archive that has some nifty spreadsheet tools.
    """
    pass
