import json
import os

from lcatools.entities import *
from lcatools.entities.processes import NoExchangeFound
from lcatools.archives import LcArchive
from .file_store import FileStore


valid_types = {'processes', 'flows', 'flow_properties'}


class OpenLcaException(Exception):
    pass


SKIP_DURING_INDEX = ('context.json', 'meta.info')


class OpenLcaJsonLdArchive(LcArchive):
    """
    Opens JSON-LD archives formatted according to the OpenLCA schema
    """
    def _gen_index(self):
        self._print('Generating index')
        self._type_index = dict()
        for f in self._archive.listfiles():
            if f in SKIP_DURING_INDEX:
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

    def _process_from_json(self, entity_j, uid):
        process = super(OpenLcaJsonLdArchive, self)._process_from_json(entity_j, uid)
        if process.has_property('allocationFactors'):
            # we do not need to replicate 0-valued allocation factors, esp. when there are thousands of them
            process['allocationFactors'] = [k for k in process['allocationFactors'] if k['value'] != 0]
        return process

    def _clean_object(self, typ, key):
        j = self._create_object(typ, key)
        j.pop('@context')
        j.pop('@id')
        name = j.pop('name')

        if 'category' in j:
            c_j = j.pop('category')
            cat = self._get_category_list(c_j['@id'])
        else:
            cat = []
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

    def _create_allocation_quantity(self, process, alloc_type):
        key = '%s_%s' % (process.name, alloc_type)
        name = '%s (%s)' % (alloc_type, process.name.strip())
        u = self._ref_to_nsuuid(key)
        q = self[u]
        if q is not None:
            return q

        unit, _ = self._create_unit('alloc')
        q = LcQuantity(key, Name=name, ReferenceUnit=unit)
        self.add(q)
        assert q.uuid == u
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
            self.tm.add_characterization(name, ref_q, q, facs[i], context=f.context, location=loc)

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
            print('%s: %d No unit! using default %s' % (p.external_ref, ex['internalId'], fp.unit()))
            v_unit = fp.unit()

        if v_unit != fp.unit():
            oldval = value
            value *= fp.convert(from_unit=v_unit)
            self._print('%s: Unit Conversion exch: %g %s to native: %g %s' % (p.uuid, oldval, v_unit, value, fp.unit()))

        if fp != flow.reference_entity:
            print('%s:\n%s flow reference quantity does not match\n%s exchange f.p. Conversion Required' %
                  (p.external_ref,
                   flow.reference_entity.external_ref,
                   fp.external_ref))
            print('From %g %s' % (value, fp.unit()))
            value /= fp.cf(flow)  # TODO: account for locale?  ## is this even right?
            print('To %g %s' % (value, flow.unit()))

        is_ref = ex.pop('quantitativeReference', False)
        if is_ref:
            term = None
        elif 'defaultProvider' in ex:
            term = ex['defaultProvider']['@id']
        else:
            term = self.tm[flow.context]

        exch = p.add_exchange(flow, dirn, value=value, termination=term, add_dups=True)
        if is_ref:
            p.set_reference(flow, dirn)

        if 'description' in ex:
            exch.comment = ex['description']

        return exch

    def _apply_olca_allocation(self, p):
        """
        For each allocation factor, we want to characterize the flow so that its exchange value times its
        characterization equals the stated factor.  Then we want to allocate the process by its default allocation
        property.
        :param p:
        :return:
        """
        if p.has_property('allocationFactors'):
            for af in p['allocationFactors']:
                if af['value'] == 0:
                    continue
                if af['allocationType'] == 'CAUSAL_ALLOCATION':
                    # not sure how to correctly interpret this
                    if af['value'] != 1.0:  # 1.0 is default NOP for causal allocation
                        print('Skipping Speculative CAUSAL_ALLOCATION of %g' % af['value'])
                    continue
                q = self._create_allocation_quantity(p, af['allocationType'])
                f = self.retrieve_or_fetch_entity(af['product']['@id'], typ='flows')
                try:
                    x = p.reference(f)
                except NoExchangeFound:
                    try:
                        x = next(rx for rx in p.exchange_values(f) if rx.termination is None)
                        p.set_reference(f, x.direction)
                    except StopIteration:
                        print('%s: Unable to find allocatable exchange for %s' % (p.external_ref, f.external_ref))
                        continue

                v = af['value'] / x.value

                self.tm.add_characterization(f.name, f.reference_entity, q, v, context=f.context, origin=self.ref)
                #f.add_characterization(q, value=v)

        if p.has_property('defaultAllocationMethod'):
            aq = self._create_allocation_quantity(p, p['defaultAllocationMethod'])
            p.allocate_by_quantity(aq)

    def _create_process(self, p_id):
        q = self[p_id]
        if q is not None:
            return q

        p_j, name, cls = self._clean_object('processes', p_id)
        ss = p_j.pop('location', {'name': 'GLO'})['name']
        stt = dict()
        for key, tgt in (('validFrom', 'begin'), ('validUntil', 'end')):
            try:
                stt[tgt] = p_j['processDocumentation'][key]
            except KeyError:
                pass

        exch = p_j.pop('exchanges')

        if 'allocationFactors' in p_j:
            p_j['allocationFactors'] = [v for v in p_j['allocationFactors'] if v['value'] != 0]

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
                p.set_reference(flow, dirn)

        self._apply_olca_allocation(p)

        return p

    def _create_lcia_quantity(self, l_j, method, **kwargs):
        q_id = l_j['@id']
        l = self[q_id]
        if l is not None:
            return l

        l_obj, l_name, cats = self._clean_object('lcia_categories', q_id)
        c_desc = l_obj.pop('description', None)
        ver = l_obj.pop('version', None)
        indicator = l_obj.pop('referenceUnitName')
        unit = LcUnit(indicator)

        q_name = ', '.join([method, l_name])

        q = LcQuantity(q_id, Name=q_name, ReferenceUnit=unit, Method=method, Category=l_name, Indicator=indicator,
                       CategoryDescription=c_desc, Version=ver, **kwargs)

        self.add(q)
        for factor in l_obj['impactFactors']:
            flow = self._create_flow(factor['flow']['@id'])
            ref_qty = self._create_quantity(factor['flowProperty']['@id'])
            assert flow.reference_entity == ref_qty
            self.tm.add_characterization(flow.name, ref_qty, q, factor['value'], context=flow.context, origin=self.ref)

        return q

    def _create_lcia_method(self, m_id):
        """
        Note: in OLCA archives, an "LCIA Method" is really a methodology with a collection of category indicators, which
        is what we colloquially call "methods". So every method includes zero or more distinct quantities.
        :param m_id:
        :return:
        """
        m_obj, method, cats = self._clean_object('lcia_methods', m_id)
        m_desc = m_obj.pop('description', None)
        for imp in m_obj.pop('impactCategories', []):
            self._create_lcia_quantity(imp, method, MethodDescription=m_desc)

    def _fetch(self, key, typ=None, **kwargs):
        if typ is None:
            if self._type_index is None:
                self._gen_index()
            typ = self._type_index[key]
        try:
            _ent_g = {'processes': self._create_process,
                   'flows': self._create_flow,
                   'flow_properties': self._create_quantity}[typ]
        except KeyError:
            print('Warning: generating generic object for unrecognized type %s' % typ)
            _ent_g = lambda x: self._create_object(typ, x)

        return _ent_g(key)

    def _load_all(self, **kwargs):
        self._print('Loading processes')
        for f in self._archive.listfiles(in_prefix='processes'):
            ff = f.split('/')
            fg = ff[1].split('.')
            self._create_process(fg[0])
        self._print('Loading LCIA methods')
        for f in self._archive.listfiles(in_prefix='lcia_methods'):
            ff = f.split('/')
            fg = ff[1].split('.')
            self._create_lcia_method(fg[0])
