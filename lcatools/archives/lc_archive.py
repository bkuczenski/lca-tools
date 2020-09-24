"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals

from antelope import LcQuery

from ..entities import LcProcess
from ..implementations import ExchangeImplementation, BackgroundImplementation, LcConfigureImplementation
from .basic_archive import BasicArchive, BASIC_ENTITY_TYPES


LC_ENTITY_TYPES = BASIC_ENTITY_TYPES + ('process', )


class LcArchive(BasicArchive):
    """
    A class meant for storing and managing LCA data collections.  Adds processes as a supported entity type (contrast
    with LcForeground which adds fragments).

    To support processes, adds inventory, background, and configure interfaces.
    """
    _entity_types = LC_ENTITY_TYPES

    @property
    def query(self):
        return LcQuery(self)

    def make_interface(self, iface):
        if iface == 'inventory' or iface == 'exchange':
            return ExchangeImplementation(self)
        elif iface == 'background':
            return BackgroundImplementation(self)
        elif iface == 'configure':
            return LcConfigureImplementation(self)
        else:
            return super(LcArchive, self).make_interface(iface)

    def _add_children(self, entity):
        if entity.entity_type == 'process':
            # need to import all the process's flows
            for x in entity.exchanges():
                self.add_entity_and_children(x.flow)
        else:
            super(LcArchive, self)._add_children(entity)

    @staticmethod
    def _lcia_key(quantity):
        return ', '.join([quantity['Method'], quantity['Category'], quantity['Indicator']])

    def load_from_dict(self, j, _check=True, jsonfile=None):
        """
        Archives loaded from JSON files are considered static.
        :param j:
        :param _check:
        :return:
        """
        super(LcArchive, self).load_from_dict(j, _check=False, jsonfile=jsonfile)
        if 'processes' in j:
            for e in j['processes']:
                self.entity_from_json(e)
        if _check:
            self.check_counter()
        if jsonfile is not None and jsonfile == self.source:
            self._loaded = True

    def _process_from_json(self, entity_j, ext_ref):
        # note-- we are officially abandoning referenceExchange notation
        if 'referenceExchange' in entity_j:
            entity_j.pop('referenceExchange')
        a_b_q = entity_j.pop('AllocatedByQuantity', None)
        exchs = entity_j.pop('exchanges', [])
        process = LcProcess(ext_ref, **entity_j)
        refs, nonrefs = [], []
        ref_x = dict()
        for i, x in enumerate(exchs):
            if 'isReference' in x and x['isReference'] is True:
                refs.append(i)
            else:
                nonrefs.append(i)
        # first add reference exchanges
        for i in refs:
            x = exchs[i]
            # eventually move this to an exchange classmethod - which is why I'm repeating myself for now
            v = None
            f = self._get_entity(x['flow'])
            d = x['direction']
            if 'value' in x:
                v = x['value']
            ref_x[x['flow']] = process.add_exchange(f, d, value=v)
            process.set_reference(f, d)
        # then add ordinary [allocated] exchanges
        for i in nonrefs:
            x = exchs[i]
            # is_ref = False
            f = self._get_entity(x['flow'])
            d = x['direction']
            if 'termination' in x:
                t = x['termination']
                cx = self.tm[t]
                if cx is not None:
                    t = cx
            else:
                t = self.tm[f.context]
            if 'value' in x:
                process.add_exchange(f, d, value=x['value'], termination=t)

            if 'valueDict' in x:
                for k, val in x['valueDict'].items():
                    drr, fuu = k.split(':')
                    try:
                        rx = ref_x[fuu]
                    except KeyError:
                        process.show()
                        print('key: %s' % k)
                        print('flow: %s' % self[fuu])
                        raise
                    # assert rx.direction == drr
                    process.add_exchange(f, d, reference=rx, value=val, termination=t)

        if a_b_q is not None:
            alloc_q = self[a_b_q['externalId']]  # allocation quantity must be locally present
            process.allocate_by_quantity(alloc_q)

        return process

    def _make_entity(self, e, etype, ext_ref):
        if etype == 'process':
            return self._process_from_json(e, ext_ref)
        return super(LcArchive, self)._make_entity(e, etype, ext_ref)

    def entities_by_type(self, entity_type):
        if entity_type not in self._entity_types:
            entity_type = {
                'p': 'process',
                'f': 'flow',
                'q': 'quantity'
            }[entity_type[0]]
        return super(LcArchive, self).entities_by_type(entity_type)

    def serialize(self, exchanges=False, characterizations=False, values=False, domesticate=False):
        """

        :param exchanges:
        :param characterizations:
        :param values:
        :param domesticate: [False] if True, omit entities' origins so that they will appear to be from the new archive
         upon serialization
        :return:
        """
        j = super(LcArchive, self).serialize(characterizations=characterizations, values=values,
                                             domesticate=domesticate)
        j['processes'] = sorted([p.serialize(exchanges=exchanges, values=values,
                                             domesticate=domesticate, drop_fields=self._drop_fields['process'])
                                 for p in self.entities_by_type('process')],
                                key=lambda x: x['externalId'])
        if self._descendant:
            j['dataSourceType'] = 'LcArchive'  # re-instantiate as base class
        return j

    def _serialize_all(self, **kwargs):
        return self.serialize(exchanges=True, characterizations=True, values=True, **kwargs)
