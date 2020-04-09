class ComponentsMixin(object):
    """
    A method to add the 'components' option to LcaModelRunner to write unit scores and node weights to a components
    file.,
    """
    components_headings = ['scenario', 'stage', 'node', 'node_weight', 'ref_units', 'method', 'category', 'indicator', 'units', 'unit_score']

    def _csv_format_components(self):
        return self.components_headings, self._gen_component_entries

    def _gen_component_entries(self, scenario, q, _rec=None, include_total=False, expand=False):
        if _rec is None:
            _rec = self._results[scenario, q]
        if include_total:
            yield self._gen_row(q, {
                'scenario': scenario,
                'node': 'Net Total',
                'node_weight': self._format(_rec.scale),
                'ref_units': 'scale',
                'unit_score': self._format(_rec.total() / _rec.scale)
            })
        for c in sorted(_rec.components(), key=lambda x: self._agg(x.entity)):
            if expand and not c.static:
                # recurse
                for y in self._gen_component_entries(q, scenario, _rec=c, expand=expand,
                                                     include_total=False): # don't print subtotals in recursion
                    yield y
            else:
                yield self._gen_row(q, {
                    'scenario': scenario,
                    'node': c.entity.name,
                    'stage': self._agg(c.entity),
                    'node_weight': self._format(c.node_weight),  # every component MUST be a summary because agg scores don't show up in traversals
                    'ref_units': c.entity.ref_unit,
                    'unit_score': self._format(c.unit_score)  #
                })


