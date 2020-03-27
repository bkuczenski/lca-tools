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

For the second one here, this seems like more a capability of an archive rather than a separate class. I think this
should be a context mgr.  We (for now) want to be strict about the sheet naming- let's take that flexibility away
from the user.

The required fields should be: external_ref, *signature_fields.  uuid is supported but not required
"""
import os
import re
from xlsxwriter import Workbook
import xlrd
from synonym_dict.lower_dict import LowerDict

from lcatools.interfaces import EntityNotFound
from lcatools.implementations.quantity import convert
from lcatools.characterizations import DuplicateCharacterizationError


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
    with Workbook(filename) as w:
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
        # w.close()  # happens automatically with context exit


def _check_merge(merge):
    if merge in ('defer', 'overwrite'):
        return merge
    raise ValueError('Invalid merge strategy: %s' % merge)


class XlsxUpdater(object):
    """
    This class uses the contents of a properly formatted XLS file (or XLS-file-like object) to create or update
    entities in an archive.

    Uses sheet names that match the archive's "_entity_types" property, so 'flow' and 'quantity' nominally.  'flows' 
    and 'quantities' also work.

    Each sheet must be in strict tabular format, with the first row being headers and each subsequent row being one
    entity.

    The only required column is 'external_ref', though 'name' and 'reference' are recommended for new entities.  NOTE:
    for the time being, quantities require a 'referenceUnit' column and flows require a 'referenceQuantity' column,
    though the hope is to eliminate that requirement in the future. For quantities, the 'referenceUnit' column should
    be a unit string; for flows the 'referenceQuantity' column should be a known quantity signifier (external_ref,
    link, or canonical name recognized by the term manager)

    Optional columns include 'uuid' and 'origin'

    All other columns are assigned as properties.

    FLOW PROPERTIES
    If there is a sheet called 'flowproperties', then it is interpreted as a list of flow characterizations.  This
    sheet can have the following columns:
     - 'flow'
     - 'ref_quantity' - (optional) if present, used only as a check against the flow's native reference_entity
     - 'ref_unit' - (optional) a convertible unit in ref_quantity
     - 'quantity' - the quantity being characterized
     - 'unit' - (optional) a convertible unit in quantity
     - 'value' - the characterization factor
     - 'context' - (optional) context for the characterization

    CELL CONTENTS
    Cells are read as strings, except for the following:
     - if a cell's first character is '!', the subsequent characters are interpreted as an entity reference
     - if a cell's first character is '*', the subsequent characters are EVALUATED, so obviously this is terribly
       insecure, but it was intended to allow people to store lists, sets, and dicts.

    MERGE STRATEGY
    The updater has two merge strategies:
      * "defer": (default) If an entity exists and has a property already defined, defer to the existing property
      * "overwrite": Any non-null incoming property is assigned to the entity, overwriting any existing value.
    """
    def __init__(self, xlrd_like, merge='defer', quiet=True):
        """
        Updates entity meta-information from a spreadsheet created from above. external_ref is used as a key;
        if an entity is not found, a new one is created.  'uuid' and reference fields are only used for creation
        of new entities.
        Note: uses the default sheet names of 'flow' and 'quantity'

        :param xlrd_like: XLRD workbook-like object OR filename of XLS file
        :param merge: ['defer'] - do not overwrite existing properties
                      ['overwrite'] - do overwrite existing properties
        :return:
        """
        self._quiet = quiet
        self._merge = _check_merge(merge)
        if isinstance(xlrd_like, str) and os.path.exists(xlrd_like):
            self._xl = xlrd.open_workbook(xlrd_like)
        else:
            self._xl = xlrd_like

    @property
    def ar(self):
        return NotImplemented

    @property
    def origin(self):
        return NotImplemented

    @property
    def qi(self):
        return NotImplemented

    def _new_entity(self, etype, rowdata):
        raise NotImplementedError

    def apply(self):
        raise NotImplementedError

    def get_context(self, cx):
        return NotImplemented

    def _grab_value(self, cell):
        value = cell.value
        if isinstance(value, str):
            if value.startswith('*'):
                value = eval(value[1:])
            elif value.startswith('!'):
                value = self.ar[value[1:]]
            elif value == '':
                value = None
        return value

    def _process_flow_properties(self):
        fp, headers = self._sheet_accessor('flowproperties')
        if fp is None:
            return

        for row in range(1, fp.nrows):
            rowdata = {headers[i]: self._grab_value(k) for i, k in enumerate(fp.row(row))}
            try:
                flow = self.ar.get(rowdata['flow'])
            except EntityNotFound:
                self._print('Skipping unknown flow %s' % rowdata['flow'])
                continue
            rq_spec = rowdata.pop('ref_quantity', None)
            if rq_spec is not None:
                try:
                    rq = self.qi.get_canonical(rq_spec)
                except EntityNotFound:
                    print('%s Skipping record with invalid ref quantity %s' % (rowdata['flow'], rq_spec))
                    continue
                if rq != flow.reference_entity:
                    print('%s ref quantity (%s) does not agree with spec %s: skipping cf' % (rowdata['flow'],
                                                                                             flow.reference_entity,
                                                                                             rq))
                    continue
            else:
                rq = flow.reference_entity

            qq = self.qi.get_canonical(rowdata['quantity'])

            value = rowdata.pop('value', None)

            cx = rowdata.pop('context', None)
            if cx is not None:
                cx = self.get_context(cx)
                # raise NotImplementedError('TODO contexts!')

            if value is None:
                continue
            try:
                value = float(value)
            except (TypeError, ValueError):
                print('Skipping non-numeric value entry %s' % value)
                continue

            refunit = rowdata.pop('ref_unit', None)
            if refunit is not None:
                value *= convert(rq, to=refunit)

            unit = rowdata.pop('unit', None)
            if unit is not None:
                value *= convert(qq, from_unit=unit)

            if self._merge == 'overwrite':
                flow.characterize(qq, value=value, context=cx, overwrite=True, origin=self.origin)
                self._print('Characterizing %s: %g %s / %s' % (flow, value, qq.unit(), rq.unit()))
            else:
                try:
                    flow.characterize(qq, value=value, context=cx)
                    self._print('Characterizing %s: %g %s / %s' % (flow, value, qq.unit(), rq.unit()))
                except DuplicateCharacterizationError:
                    self._print('Deferring to existing CF')
                    continue

    _vn = {'flow': 'flows',
           'quantity': 'quantities'}

    def _sheet_accessor(self, sheetname):
        """
        Returns a 2-tuple of sheet, headers
        'sheet' must be an "xlrd.Sheet-like" object that implements the following very simple API:
         sheet.nrows - returns number of rows
         sheet.row(n) - returns the contents of the nth row in a list of "cell-like" objects
        'cell-like' object has 2 properties:
         'ctype' - integer 0=empty, 1=text, 2=number, 3=date, 4=bool, 5=error, 6=debug
         'value' - contents of the cell
        'headers' is the contents of the 0th row in a list
        :param sheetname:
        :return: sheet, headers
        """
        if sheetname in self._xl.sheet_names():
            sh = self._xl.sheet_by_name(sheetname)
        elif self._vn[sheetname] in self._xl.sheet_names():
            sheetname = self._vn[sheetname]
            sh = self._xl.sheet_by_name(sheetname)
        else:
            return None, None
        try:
            headers = [k.value for k in sh.row(0)]
        except IndexError:
            self._print('Empty sheet %s' % sheetname)
            return sh, None
        self._print('\nOpened sheet %s' % sheetname)
        return sh, headers

    def _process_sheet(self, etype):

        sh, headers = self._sheet_accessor(etype)
        if sh is None:
            return

        for row in range(1, sh.nrows):
            rowdata = {headers[i]: self._grab_value(k) for i, k in enumerate(sh.row(row))}
            ent = self.ar[rowdata['external_ref']]
            if ent is None:
                self._new_entity(etype, rowdata)
            else:
                try:
                    rowdata.pop('external_ref')
                except KeyError:
                    continue
                rowdata.pop('uuid', None)
                ref = rowdata.pop(ent.reference_field, None)
                if self._merge == 'overwrite' and ref is not None:
                    if isinstance(ref, str) and etype == 'flow':
                        ref = self.qi.get_canonical(ref)
                    if ref is not None and ent.reference_entity != ref:
                        self._print('Updating reference entity %s -> %s' % (ent.reference_entity, ref))
                        ent[ent.reference_field] = ref
                for k, v in rowdata.items():
                    if v is None:
                        continue
                    if ent.has_property(k):
                        if self._merge == 'defer':
                            continue
                        elif ent[k] == v:
                            continue
                    self._print('Updating %s[%s] -> %s' % (ent.external_ref, k, v))
                    ent[k] = v

    def _print(self, *args):
        if self._quiet:
            return
        print(*args)

    def __enter__(self):
        """Return self object to use with "with" statement."""
        return self

    def __exit__(self, *args):
        if hasattr(self._xl, 'release_resources'):
            self._xl.release_resources()
        self._xl = None


class XlsxArchiveUpdater(XlsxUpdater):
    def __init__(self, archive, xlrd_like, merge='defer', quiet=True):
        """
        Updates entity meta-information from a spreadsheet created from above. external_ref is used as a key;
        if an entity is not found, a new one is created.  'uuid' and reference fields are only used for creation
        of new entities.
        Note: uses the default sheet names of 'flow' and 'quantity'

        :param archive:
        :param xlrd_like: XLRD workbook-like object OR filename of XLS file
        :param merge: ['defer'] - do not overwrite existing properties
                      ['overwrite'] - do overwrite existing properties
        :return:
        """
        self._ar = archive
        self._qi = archive.make_interface('quantity')
        super(XlsxArchiveUpdater, self).__init__(xlrd_like, merge=merge, quiet=quiet)

    @property
    def ar(self):
        return self._ar

    @property
    def origin(self):
        return self.ar.ref

    @property
    def qi(self):
        return self._qi

    def _new_entity(self, etype, rowdata):
        rowdata['externalId'] = rowdata.pop('external_ref')
        rowdata['entityId'] = rowdata.pop('uuid', None)
        rowdata['entityType'] = etype
        self._ar.entity_from_json(rowdata)

    def apply(self):
        for etype in ('quantity', 'flow'):  # these are the only types that are currently handled
            self._process_sheet(etype)
        self._process_flow_properties()

    def get_context(self, cx):
        return self.ar.tm[cx]
