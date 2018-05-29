"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals


import six
import os

from lcatools.entities import LcEntity, LcProcess, BasicArchive
from lcatools.from_json import from_json
from lcatools.implementations import InventoryImplementation, BackgroundImplementation, ConfigureImplementation

if six.PY2:
    bytes = str
    str = unicode


class LcArchive(BasicArchive):
    """
    A class meant for storing and managing LCA data collections.  Adds processes as a supported entity type (contrast
    with LcForeground which adds fragments).

    To support processes, adds inventory, background, and configure interfaces.
    """
    _entity_types = {'quantity', 'flow', 'process'}

    def __getitem__(self, item):
        """
        Note: this user-friendliness check adds 20% to the execution time of getitem-- so avoid it if possible
        (use _get_entity directly -- especially now that upstream is now deprecated)

        :param item:
        :return:
        """
        if isinstance(item, LcEntity):
            item = item.uuid
        return super(LcArchive, self).__getitem__(item)

    def make_interface(self, iface):
        if iface == 'inventory':
            return InventoryImplementation(self)
        elif iface == 'background':
            return BackgroundImplementation(self)
        elif iface == 'configure':
            return ConfigureImplementation(self)
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

    def load_json(self, j, _check=True, **kwargs):
        """
        Archives loaded from JSON files are considered static.
        :param j:
        :param _check:
        :return:
        """
        super(LcArchive, self).load_json(j, _check=False, **kwargs)
        if 'processes' in j:
            for e in j['processes']:
                self.entity_from_json(e)
        if _check:
            self.check_counter()

    def _process_from_json(self, entity_j, uid):
        # note-- we are officially abandoning referenceExchange notation
        if 'referenceExchange' in entity_j:
            entity_j.pop('referenceExchange')
        a_b_q = entity_j.pop('AllocatedByQuantity', None)
        exchs = entity_j.pop('exchanges', [])
        process = LcProcess(uid, **entity_j)
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
            process.add_reference(f, d)
        # then add ordinary [allocated] exchanges
        for i in nonrefs:
            x = exchs[i]
            t = None
            # is_ref = False
            f = self._get_entity(x['flow'])
            d = x['direction']
            if 'termination' in x:
                t = x['termination']
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

    def _make_entity(self, e, etype, uid):
        if etype == 'process':
            return self._process_from_json(e, uid)
        return super(LcArchive, self)._make_entity(e, etype, uid)

    def entities_by_type(self, entity_type):
        if entity_type not in self._entity_types:
            entity_type = {
                'p': 'process',
                'f': 'flow',
                'q': 'quantity'
            }[entity_type[0]]
        return super(LcArchive, self).entities_by_type(entity_type)

    def serialize(self, exchanges=False, characterizations=False, values=False):
        """

        :param exchanges:
        :param characterizations:
        :param values:
        :return:
        """
        j = super(LcArchive, self).serialize(characterizations=characterizations, values=values)
        j['processes'] = sorted([p.serialize(exchanges=exchanges, values=values)
                                 for p in self.entities_by_type('process')],
                                key=lambda x: x['entityId'])
        if self._descendant:
            j['dataSourceType'] = 'LcArchive'  # re-instantiate as base class
        return j

    def _serialize_all(self, **kwargs):
        return self.serialize(exchanges=True, characterizations=True, values=True)

    def _load_all(self, **kwargs):
        if self.source is None:
            return
        if os.path.exists(self.source):
            self.load_json(from_json(self.source))
