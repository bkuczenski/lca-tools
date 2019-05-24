"""
Class for interchanging entities with an excel spreadsheet.

There are two basic parts to this operation:
 * we want to be able to write a subset of entities in an archive to an excel worksheet whose name is the same as the
   entity_type. We do not want to mix different entity types in the same worksheet. We also only want to include
   metadata-- quantitative data (exchanges and flow characterizations) are not meant to be manipulated in this way.
   The export to Excel should include the required columns to the leftmost, followed by nonrequired properties.  For a
   given entity/property pair, if the entity lacks that property the entry should be blank.

 * We want to be able to read in metadata from a spreadsheet as well- creating new entities as needed, and modifying
   existing entities nondestructively according to a policy with two options:
    - 'defer': if a property exists for a given entity, let its current value stand. If a property does not exist and
      the incoming specification is non-null, assign it..
    - 'overwrite': Assign all incoming non-null specifications

For the first one, the only capability of the archive that is needed is (arguably) external to the feature-- namely,
the ability to generate the entities. Therefore I think it should be a utility function and not an object at all.

For the second one here, this seems like more a capability of an archive rather than a separate class. Therefore, I
think this should be a mixin.  We (for now) want to be strict about the sheet naming- let's take that flexibility away
from the user.

The required fields should be: origin, external_ref, uuid[can be None], *signature_fields.  All others are optional.
"""
import os
import re
from xlsxwriter import Workbook
import xlrd
from synonym_dict.lower_dict import LowerDict


class XlsxEntityTypeWriter(object):
    _attribs = ('external_ref', 'uuid')

    def _next_name(self):
        name = self._etype
        k = 0
        while name in self._wb.sheetnames:
            name = '%s%d' % (self._etype, k)
            k += 1
        print('Writing entities to sheet %s' % name)
        return name

    def _add_column(self, col_name):
        if col_name not in self._rc:
            i = len(self._columns)
            self._columns.append(col_name)
            self._rc[col_name] = i
            self._sheet.write(0, i, col_name)
        return self._rc[col_name]

    def __init__(self, xls_workbook, etype):
        self._wb = xls_workbook
        self._etype = etype
        self._columns = []
        self._rc = LowerDict()
        self._count = 0
        self._sheet = self._wb.add_worksheet(self._next_name())
        self._sig = None
        self._ref_col = None

    def _grab_signature_fields(self, entity):
        if self._sig:
            return
        for k in self._attribs:
            self._add_column(k)
        for k in entity.signature_fields():
            i = self._add_column(k)
            # tag the reference column
            if k == entity.reference_field:
                self._ref_col = i
        self._sig = tuple(self._columns)

    def _write(self, row, column, value):
        """
        For other entities
        For non-writeable types, deliver their __repr__, preceded by an asterisk
        """
        if hasattr(value, 'entity_type'):
            value = '!%s' % value.external_ref
        elif isinstance(value, str) and (value.startswith('*') or value.startswith('!')):
            value = '*%s' % repr(value)
        try:
            self._sheet.write(row, column, value)
        except TypeError:
            self._sheet.write(row, column, '*%s' % repr(value))

    def add_entity(self, entity):
        if entity.entity_type != self._etype:
            print('Skipping entity %s of type %s' % (entity.external_ref, entity.entity_type))
            return False
        if self._sig is None:
            self._grab_signature_fields(entity)

        self._count += 1

        # first - signature fields
        for i, col in enumerate(self._sig):
            if col in self._attribs:
                val = getattr(entity, col)
            elif i == self._ref_col and self._etype == 'process':
                val = {repr(k) for k in entity.reference_entity}
            else:
                val = entity[col]
            self._write(self._count, i, val)

        # next - other properties
        p = set(entity.properties()) - set(self._sig)
        for k in p:
            i = self._add_column(k)
            self._write(self._count, i, entity[k])
        return True


def write_to_excel(filename, entity_iter, overwrite=False):
    if not bool(re.search('xlsx?$', filename, flags=re.I)):
        filename += '.xlsx'
    if os.path.exists(filename):
        if not overwrite:
            raise FileExistsError('Use overwrite=True')
    w = Workbook(filename)
    count = 0
    d = dict()
    for ent in entity_iter:
        etype = ent.entity_type
        if etype not in d:
            d[etype] = XlsxEntityTypeWriter(w, etype)
        sh = d[etype]
        if sh.add_entity(ent):
            count += 1
    print('Writing %d entities to %s' % (count, filename))
    w.close()


def _check_merge(merge):
    if merge in ('defer', 'overwrite'):
        return merge
    raise ValueError('Invalid merge strategy: %s' % merge)


class XlsxArchiveUpdater(object):
    def _grab_value(self, cell):
        value = cell.value
        if isinstance(value, str):
            if value.startswith('*'):
                value = eval(value[1:])
            elif value.startswith('!'):
                value = self._ar[value[1:]]
            elif value == '':
                value = None
        return value

    def _new_entity(self, etype, rowdata):
        rowdata['externalId'] = rowdata.pop('external_ref')
        rowdata['entityId'] = rowdata.pop('uuid')
        rowdata['entityType'] = etype
        self._ar.entity_from_json(rowdata)

    def _process_sheet(self, etype):
        try:
            sh = self._xl.sheet_by_name(etype)
        except xlrd.XLRDError:
            return
        self._print('Opened sheet %s' % etype)
        headers = [k.value for k in sh.row(0)]

        for row in range(1, sh.nrows):
            rowdata = {headers[i]: self._grab_value(k) for i, k in enumerate(sh.row(row))}
            ent = self._ar[rowdata['external_ref']]
            if ent is None:
                self._new_entity(etype, rowdata)
            else:
                rowdata.pop('external_ref')
                rowdata.pop('uuid')
                rowdata.pop(ent.reference_field)
                for k, v in rowdata.items():
                    if self._merge == 'defer':
                        if ent.has_property(k):
                            continue
                        elif v is None:
                            continue
                        else:
                            self._print('Updating %s[%s] -> %s' % (ent.external_ref, k, v))
                            ent[k] = v
                    else:
                        self._print('Updating %s[%s] -> %s' % (ent.external_ref, k, v))
                        ent[k] = v

    def _print(self, *args):
        if self._quiet:
            return
        print(*args)

    def __init__(self, archive, filename, merge='defer', quiet=True):
        """
        Updates entity meta-information from a spreadsheet created from above. external_ref is used as a key;
        if an entity is not found, a new one is created.  'uuid' and reference fields are only used for creation
        of new entities.
        Note: uses the default sheet names of 'flow', 'quantity', 'process'
        :param archive:
        :param filename:
        :param merge: ['defer'] - do not overwrite existing properties
                      ['overwrite'] - do overwrite existing properties
        :return:
        """
        self._quiet = quiet
        self._ar = archive
        self._xl = xlrd.open_workbook(filename)
        self._merge = _check_merge(merge)

    def apply(self):
        for etype in self._ar._entity_types:
            self._process_sheet(etype)

    def __exit__(self):
        self._xl.release_resources()
