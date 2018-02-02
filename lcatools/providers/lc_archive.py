"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals


import six

from lcatools.entities import LcEntity, LcProcess, BasicArchive
from lcatools.implementations import InventoryImplementation, BackgroundImplementation, ConfigureImplementation

if six.PY2:
    bytes = str
    str = unicode


class LcArchive(BasicArchive):
    """
    A class meant for building life cycle models. This takes the archive machinery and adds functions
    specific to processes, flows, and quantities. Creates an upstream lookup for quantities but no other entity types.

    Note: in lieu of having a @classmethod from_json, we have an archive factory that produces archives using the
    appropriate constructor given the data type.  This is found in lcatools.tools and calls the base entity_from_json
    method-- which itself should be offloaded to the entities wherever possible.

    """
    _entity_types = {'quantity', 'flow', 'process'}

    @classmethod
    def from_dict(cls, j):
        """
        LcArchive factory from minimal dictionary.  Must include at least one of 'dataSource' or 'dataReference' fields
        and 0 or more processes, flows, or quantities; but note that any flow present must have its reference quantities
        included, and any process must have its exchanged flows included.
        :param j:
        :return:
        """
        source = j.pop('dataSource', None)
        try:
            ref = j.pop('dataReference')
        except KeyError:
            if source is None:
                print('Dictionary must contain at least a dataSource or a dataReference specification.')
                return None
            else:
                ref = None
        ar = cls(source, ref=ref)
        ar.load_json(j)
        return ar

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

    def make_interface(self, iface, privacy=None):
        if iface == 'inventory':
            return InventoryImplementation(self, privacy=privacy)
        elif iface == 'background':
            return BackgroundImplementation(self, privacy=privacy)
        elif iface == 'configure':
            return ConfigureImplementation(self, privacy=privacy)
        else:
            return super(LcArchive, self).make_interface(iface, privacy=privacy)

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

    def load_json(self, j, _check=True):
        """
        Archives loaded from JSON files are considered static.
        :param j:
        :param _check:
        :return:
        """
        super(LcArchive, self).load_json(j, _check=False)
        if 'processes' in j:
            for e in j['processes']:
                self.entity_from_json(e)
        if _check:
            self.check_counter()

    def _process_from_json(self, entity_j, uid):
        # note-- we are officially abandoning referenceExchange notation
        if 'referenceExchange' in entity_j:
            entity_j.pop('referenceExchange')
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
        j = super(LcArchive, self).serialize()
        j['processes'] = sorted([p.serialize(exchanges=exchanges, values=values)
                                 for p in self.entities_by_type('process')],
                                key=lambda x: x['entityId'])
        return j

    def _serialize_all(self, **kwargs):
        return self.serialize(exchanges=True, characterizations=True, values=True)