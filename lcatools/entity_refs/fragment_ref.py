from .base import EntityRef
from ..fragment_flows import group_ios


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
    _ref_field = 'parent'

    def __init__(self, *args, **kwargs):
        super(FragmentRef, self).__init__(*args, **kwargs)
        self._direction = None
        self._flow = None
        self._isset = False

    def set_config(self, flow, direction):
        if self._isset:
            raise AttributeError('Fragment Ref is already specified!')
        self._isset = True
        self._flow = flow
        self._direction = direction

    @property
    def direction(self):
        return self._direction

    @property
    def is_background(self):
        """
        Can't figure out whether it ever makes sense for a fragment ref to be regarded 'background'
        :return:
        """
        return False

    @property
    def flow(self):
        return self._flow

    @property
    def _addl(self):
        return 'frag'

    @property
    def name(self):
        if self.external_ref is None:
            return self['Name']
        return self.external_ref

    @property
    def is_conserved_parent(self):
        return None

    def set_name(self, name, **kwargs):
        return self._query.name_fragment(self, name, **kwargs)

    def inventory(self, scenario=None, **kwargs):
        return self._query.inventory(self.external_ref, scenario=scenario, **kwargs)

    def traverse(self, scenario=None, **kwargs):
        return self._query.traverse(self.external_ref, scenario=scenario, **kwargs)

    def fragment_lcia(self, lcia_qty, scenario=None, **kwargs):
        return self._query.fragment_lcia(self.external_ref, lcia_qty, scenario=scenario, **kwargs)

    def bg_lcia(self, lcia_qty, scenario=None, **kwargs):
        return self.fragment_lcia(self.external_ref, lcia_qty, scenario=scenario, **kwargs)

    def unit_inventory(self, scenario=None, observed=None):
        """

        :param scenario:
        :param observed: ignored; supplied only for signature consistency
        :return:
        """
        if observed is False:
            print('Ignoring false observed flag')
        ffs = self.traverse(scenario=scenario)  # in the future, may want to cache this
        return group_ios(self, ffs)
