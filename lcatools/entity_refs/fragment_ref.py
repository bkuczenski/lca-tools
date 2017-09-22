from .base import EntityRef


class FragmentRef(EntityRef):
    """
    Fragments can lookup:
    """
    '''
    def __init__(self, *args, **kwargs):
        super(FragmentRef, self).__init__(*args, **kwargs)
        self._known_scenarios = dict()
    '''
    _etype = 'fragment'

    @property
    def _addl(self):
        return 'frag'

    def inventory(self, scenario=None, **kwargs):
        return self._query.inventory(self.external_ref, ref_flow=scenario, **kwargs)

    def traverse(self, scenario=None, **kwargs):
        return self._query.traverse(self.external_ref, scenario=scenario, **kwargs)

    def fragment_lcia(self, lcia_qty, scenario=None, **kwargs):
        return self._query.fragment_lcia(self.external_ref, lcia_qty, scenario=scenario, **kwargs)
