import json
import os

from lcatools.entities import *
from lcatools.archives import LcArchive
from .file_store import FileStore


valid_types = {'processes', 'flows', 'flow_properties'}


class OpenLcaException(Exception):
    pass


class OpenLcaJsonLdArchive(LcArchive):
    """
    Opens JSON-LD archives formatted according to the OpenLCA schema
    """
    def _gen_index(self):
        self._print('Generating index')
        self._type_index = dict()
        for f in self._archive.listfiles():
            if f == 'context.json':
                continue
            ff = f.split('/')
            fg = ff[1].split('.')
            self._type_index[fg[0]] = ff[0]

    def __init__(self, source, prefix=None, skip_index=False, **kwargs):
        super(OpenLcaJsonLdArchive, self).__init__(source, **kwargs)

        self._drop_fields['process'].extend(['processDocumentation'])

        self._archive = FileStore(source, internal_prefix=prefix)

        self._type_index = None
        if not skip_index:
            self._gen_index()

    def _check_id(self, _id):
        return self[_id] is not None

    def _create_object(self, typ, key):
        return json.loads(self._archive.readfile(os.path.join(typ, key + '.json')))

    def _clean_object(self, typ, key):
        j = self._create_object(typ, key)
        j.pop('@context')
        j.pop('@id')
        name = j.pop('name')

        c_j = j.pop('category')
        cat = self._get_category_list(c_j['@id'])
        return j, name, cat

    def _get_category_list(self, cat_key):
        c_j = self._create_object('categories', cat_key)
        if 'category' in c_j:
            cat = self._get_category_list(c_j['category']['@id'])
        else:
            cat = []
        cat.append(c_j['name'])
        return cat

    def _create_unit(self, unit_id):
        try:
            u_j = self._create_object('unit_groups', unit_id)
        except KeyError:
            return LcUnit(unit_id), None
        unitconv = dict()
        unit = None

        for conv in u_j['units']:
            is_ref = conv.pop('referenceUnit', False)
            name = conv.pop('name')
            cf_i = conv.pop('conversionFactor')
            unitconv[name] = 1.0 / cf_i

            if is_ref:
                assert cf_i == 1, 'non-unit reference unit found! %s' % unit_id
                unit = LcUnit(name)

        if unit is None:
            raise OpenLcaException('No reference unit found for id %s' % unit_id)

        return unit, unitconv

    def _create_quantity(self, q_id):
        q = self[q_id]
        if q is not None:
            return q

        q_j, name, cat = self._clean_object('flow_properties', q_id)
        ug = q_j.pop('unitGroup')
        unit, unitconv = self._create_unit(ug['@id'])

        q = LcQuantity(q_id, Name=name, ReferenceUnit=unit, UnitConversion=unitconv, Category=cat, **q_j)

        self.add(q)
        return q

    def _create_flow(self, f_id):
        q = self[f_id]
        if q is not None:
            return q

        f_j, name, comp = self._clean_object('flows', f_id)
        cas = f_j.pop('cas', '')
        loc = f_j.pop('location', {'name': 'GLO'})['name']

        fps = f_j.pop('flowProperties')

        qs = []
        facs = []
        ref_q = None

        for fp in fps:
            q = self.retrieve_or_fetch_entity(fp['flowProperty']['@id'], typ='flow_properties')
            ref = fp.pop('referenceFlowProperty', False)
            fac = fp.pop('conversionFactor')
            if ref:
                assert fac == 1.0, 'Non-unit reference flow property found! %s' % f_id
                ref_q = q
            else:
                if q not in qs:
                    qs.append(q)
                    facs.append(fac)
        if ref_q is None:
            raise OpenLcaException('No reference flow property found: %s' % f_id)
        f = LcFlow(f_id, Name=name, Compartment=comp, CasNumber=cas, ReferenceQuantity=ref_q, **f_j)

        for i, q in enumerate(qs):
            f.add_characterization(q, value=facs[i], location=loc)

        self.add(f)
        return f

    def _add_exchange(self, p, ex):
        flow = self.retrieve_or_fetch_entity(ex['flow']['@id'], typ='flows')
        value = ex['amount']
        dirn = 'Input' if ex['input'] else 'Output'

        fp = self.retrieve_or_fetch_entity(ex['flowProperty']['@id'], typ='flow_properties')

        try:
            v_unit = ex['unit']['name']
        except KeyError:
            print('%s: %d No unit! using default %s' % (p.uuid, ex['internalId'], fp.unit()))
            v_unit = fp.unit()

        if v_unit != fp.unit():
            oldval = value
            value *= fp.convert(from_unit=v_unit)
            self._print('%s: Unit Conversion exch: %g %s to native: %g %s' % (p.uuid, oldval, v_unit, value, fp.unit()))

        if fp != flow.reference_entity:
            print('%s:\n%s flow reference quantity does not match\n%s exchange f.p. Conversion Required' % p.uuid,
                  flow.reference_entity.uuid,
                  fp.uuid)
            print('From %g %s' % (value, fp.unit()))
            value *= flow.cf(flow.reference_entity)
            print('To %g %s' % (value, flow.unit()))

        return p.add_exchange(flow, dirn, value=value, add_dups=True)

    def _create_process(self, p_id):
        q = self[p_id]
        if q is not None:
            return q

        p_j, name, cls = self._clean_object('processes', p_id)
        ss = p_j.pop('location', {'name': 'GLO'})['name']
        stt = {'begin': p_j['processDocumentation']['validFrom'],
               'end': p_j['processDocumentation']['validUntil']}

        exch = p_j.pop('exchanges')

        # leave allocation in place for now

        p = LcProcess(p_id, Name=name, Classifications=cls, SpatialScope=ss, TemporalScope=stt, **p_j)

        self.add(p)

        for ex in exch:
            self._add_exchange(p, ex)

        for ex in exch:
            ref = ex.pop('quantitativeReference', False)
            if ref:
                flow = self.retrieve_or_fetch_entity(ex['flow']['@id'], typ='flows')
                dirn = 'Input' if ex['input'] else 'Output'
                p.add_reference(flow, dirn)

        return p

    def _fetch(self, key, typ=None, **kwargs):
        if typ is None:
            if self._type_index is None:
                self._gen_index()
            typ = self._type_index[key]
        try:
            ent = {'processes': self._create_process,
                   'flows': self._create_flow,
                   'flow_properties': self._create_quantity}[typ](key)
        except KeyError:
            ent = self._create_object(typ, key)

        return ent

    def _load_all(self, **kwargs):
        for f in self._archive.listfiles(in_prefix='process'):
            ff = f.split('/')
            fg = ff[1].split('.')
            self._create_process(fg[0])
