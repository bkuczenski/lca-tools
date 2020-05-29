
from .base import BaseTableOutput
from collections import defaultdict


class FlowablesGrid(BaseTableOutput):
    """
    This table can be produced dynamically

    """
    _near_headings = 'Flowable', 'Compartment'  # should be overridden
    _returns_sets = True

    def _pull_row_from_item(self, item):
        """
        Items are either exchanges, factors, or flattened LciaResult components.
        The first two have a 'flow' attribute
        the last has an 'entity' which should be a flow.
        :param item:
        :return: 2-tuple: flowable name (string), compartment (Compartment)
        """
        if hasattr(item, 'flow'):
            flow = item.flow
        elif hasattr(item, 'entity'):
            flow = item.entity
        else:
            raise TypeError('Cannot find a flow in item of type %s' % type(item))
        return self._qdb.parse_flow(flow)

    def _generate_items(self, flow_collection):
        """
        Columns are: process ref, qty (lcia method) ref, lciaresult, or explicit iterator that provides exchanges,
        factors, or lciaresult components.

        :param flow_collection:
        :return:
        """
        if hasattr(flow_collection, 'flatten'):
            lcia = flow_collection.flatten()
            for component in lcia.components():
                if self._criterion(component.entity):
                    yield component
        elif hasattr(flow_collection, 'entity_type'):
            if flow_collection.entity_type == 'process':
                for exchange in flow_collection.inventory():
                    if self._criterion(exchange.flow):
                        yield exchange
            elif flow_collection.entity_type == 'quantity':
                if flow_collection.is_lcia_method:
                    if self._qdb.is_known(flow_collection):
                        for cf in self._qdb.factors(flow_collection):
                            if self._criterion(cf.flow):
                                yield cf
                    else:
                        for cf in flow_collection.factors():
                            if self._criterion(cf.flow):
                                yield cf
        else:
            # generic iterable-- assume exchange for now
            for x in flow_collection:
                if self._criterion(x.flow):
                    yield x

    def _canonical(self, flow):
        """

        use name: this is not quite right because parse_flow is too 'fuzzy'--
        add units to distinguish flows that get bypassed in convert()
        flowables because of CAS number; matches lots of different things that are all mapped to the same flowable (e.g.
        difference between fossil and biogenic CO2 is lost).  This is no longer WRONG because it adds instead of
        replacing.

        maybe that is the desirable course- but then I lose the ability to distinguish CO2 from peat, CO2 from gas, and
        co2 from land use change.  But maybe that is part of the meaning of a "flowables grid".

        still thinking.
        :param flow:
        :return:
        """
        name, _ = self._qdb.parse_flow(flow)
        return '[%s] %s' % (flow.unit, self._qdb.f_name(name))

    def _extract_data_from_item(self, objects):
        """
        We need to return dicts because we have _returns_sets = True.
        Items are either exchanges, factors, or flattened LciaResult components.
        First one is _value_dict (no existing public method to access)
        Second one is _locations (also no existing public method to access)
        Third one has no dict, it's just cumulative_result

        :param objects: dict item. list of objects
        :return:
        """
        d = defaultdict(float)
        if objects is None:
            return d
        for item in objects:
            if hasattr(item, 'entity_type'):
                if item.entity_type == 'exchange':
                    if hasattr(item, '_value_dict'):
                        for rx, v in item._value_dict.items():
                            key = '%s: %s' % (rx.direction, rx.flow['Name'])
                            d[key] += v
                    if item.value is not None:
                        d[self._canonical(item.flow)] += item.value
                elif item.entity_type == 'characterization':
                    for loc, v in item._locations.items():
                        if loc == 'GLO':
                            key = self._canonical(item.flow)
                        else:
                            # there 'should not be' different cf values for the same flowable- but we know it happens
                            key = loc
                        if d[key] == 0:
                            d[key] = v
                        elif d[key] != v:
                            new_key = '[%s] %s' % (loc, item.flow['Name'])
                            while d[new_key] != 0:
                                new_key += '.'
                            d[new_key] = v
                else:
                    raise TypeError('Unsure what to do with item %s' % item)
            elif hasattr(item, 'cumulative_result'):
                # print('%10.3g %s' % (item.cumulative_result, self._canonical(item.entity)))
                d[self._canonical(item.entity)] += item.cumulative_result
            else:
                raise TypeError('Unsure what to do with item %s' % item)
        return d

    def __init__(self, qdb, *args, include_flows=None, quell_locations=False):
        """

        :param qdb: Mandatory Qdb for resolving flowables and compartments
        :param args: supply process ref (exchanges), quantity ref (cfs), or lcia result (flows)
        :param include_flows: either None (include all flows) or an iterable of flowables to include
        :param quell_locations:  for characterizations, suppress reporting of locations other than GLO. [not yet impl.]
        """
        print('!!NOTE: _far_headings must be set')
        self._qdb = qdb
        if include_flows is not None:
            include_fbs = set([qdb.f_index(qdb.parse_flow(x)[0]) for x in include_flows])

            def criterion(x):
                return qdb.f_index(qdb.parse_flow(x)[0]) in include_fbs
        else:
            criterion = None

        super(FlowablesGrid, self).__init__(*args, criterion=criterion)
