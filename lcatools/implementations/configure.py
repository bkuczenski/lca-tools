from collections import namedtuple
from antelope import ConfigureInterface, check_direction

from .basic import BasicImplementation


ValidConfig = namedtuple('ValidConfig', ('nargs', 'argtypes'))

valid_configs = {  # 0_ indicates the arg is allowed to be None
    'hints': ValidConfig(3, ('hint', 'str', 'str')),
    'set_reference': ValidConfig(3, ('process', 'flow', '0_direction')),
    'unset_reference': ValidConfig(3, ('0_process', '0_flow', '0_direction')),
    'characterize_flow': ValidConfig(3, ('flow', 'quantity', 'float')),
    'allocate_by_quantity': ValidConfig(2, ('process', 'quantity'))
}


class ConfigureImplementation(BasicImplementation, ConfigureInterface):

    _config_options = ('characterize_flow', )

    def _apply_config(self, config, option, **kwargs):
        if option in config:
            for k in config[option]:
                getattr(self, option)(*k, **kwargs)

    def apply_config(self, config, **kwargs):
        """
        Apply a collection of configuration objects to the archive.

        All options should be supplied as a dict of iterables, where the keys are the configuration methods and
        the entries in each iterable are tuples corresponding to the configuration parameters, properly ordered.

        Configuration options should be idempotent; applying them several times should have the same effect as
        applying them once.
        :return:
        """
        print('Applying configuration to %s' % self._archive)
        # re_index = False
        '''
        # self._apply_config(config, 'add_terms')
        for k in _config['add_terms']:
            print('adding %s synonyms (%s|%d)' % (k[0], k[1], len(k) - 1))
            self.add_terms(*k)
        '''
        for opt in self._config_options:
            self._apply_config(config, opt, **kwargs)

        if hasattr(self._archive, 'ti'):
            self._archive.make_interface('index').re_index()

    def check_config(self, config, c_args, **kwargs):
        """
        Uses the config schema valid_configs to validate config arguments.  A leading '0_' indicates that the argument
        is permitted to be None.

        :param config: a configuration entry
        :param c_args: a tuple of args
        :param kwargs:
        :return:
        """
        vc = valid_configs[config]
        if len(c_args) != vc.nargs:
            raise ValueError('Wrong number of arguments (%d supplied; %d required)' % (len(c_args), vc.nargs))
        for i, t in enumerate(vc.argtypes):
            if t.startswith('0_'):
                if c_args[i] is None:
                    continue
                t = t.split('_')[1]
            if t == 'hint':
                if c_args[i] in ('context', 'flowable', 'quantity'):
                    continue
            elif t == 'float':
                if isinstance(c_args[i], float) or isinstance(c_args[i], int):
                    continue
            elif t == 'str':
                if isinstance(c_args[i], str):
                    continue
            elif t == 'direction':
                try:
                    check_direction(c_args[i])
                except KeyError:
                    raise ValueError('Argument %d [%s] is not a valid direction' % (i, c_args[i]))
                continue
            elif t == 'context':
                # check to ensure t is a recognized locally defined context
                cx = self._archive.tm[c_args[i]]
                if cx is None:
                    raise ValueError('Argument %d [%s] is not a recognized local context' % (i, c_args[i]))
                continue
            else:
                '''
                if not isinstance(c_args[i], str):
                    raise TypeError('%s [%d:%s]: Configuraton arguments must be strings and not entities' % (config, i,
                                                                                                             c_args[i]))
                '''
                e = self._archive.retrieve_or_fetch_entity(c_args[i])
                if e.entity_type == t:
                    continue
            raise TypeError('Argument %d should be type %s' % (i, t))
        return True

    def characterize_flow(self, flow_ref, quantity_ref, value, location='GLO', overwrite=False, **kwargs):
        """
        A ConfigFlowCharacterization provides a procedural mechanism for specifying flow quantity characterizations
        after loading an archive.  The 'flow_ref' and 'quantity_ref' have to lookup successfully in the archive.

        Not clear how to deal with contexts

        :param flow_ref:
        :param quantity_ref:
        :param value:
        :param location:
        :param overwrite:
        :param kwargs:
        :return:
        """
        print('Characterizing flow %s by %s: %g' % (flow_ref, quantity_ref, value))
        flow = self._archive.retrieve_or_fetch_entity(flow_ref)
        qty = self._archive.retrieve_or_fetch_entity(quantity_ref)
        self._archive.tm.add_characterization(flow.link, flow.reference_entity, qty, value, context=flow.context,
                                              location=location,
                                              origin=self.origin, overwrite=overwrite)


class LcConfigureImplementation(ConfigureImplementation):

    _config_options = ('set_reference', 'unset_reference', 'characterize_flow', 'allocate_by_quantity')

    @staticmethod
    def _check_direction(pr, fl):
        exs = [x for x in pr.exchanges(flow=fl) if x.termination is None]
        if len(exs) == 2:
            raise ValueError('Direction must be specified')
        elif len(exs) == 0:
            raise KeyError('No unterminated exchanges found for flow %s' % fl)
        else:
            return exs[0].direction

    def set_reference(self, process_ref, flow_ref, direction=None, **kwargs):
        """
        A ConfigSetReference indicates that a particular exchange should be marked a reference exchange.  All terms are
        required.  The flow and direction must uniquely identify an exchange.

        :param process_ref:
        :param flow_ref:
        :param direction:
        :param kwargs:
        :return:
        """
        print('Setting reference %s [%s] for %s' % (flow_ref, direction, process_ref))
        pr = self._archive.retrieve_or_fetch_entity(process_ref)
        fl = self._archive.retrieve_or_fetch_entity(flow_ref)
        if direction is None:
            direction = self._check_direction(pr, fl)

        pr.set_reference(fl, direction)

    def unset_reference(self, process_ref, flow_ref, direction=None, **kwargs):
        """
        A ConfigBadReference provides a procedural mechanism for removing automatically-tagged reference flows, or for
        marking a byproduct as non-reference or non-allocatable.  The parts are 'process_ref', 'flow_ref', and
        'direction', but if process_ref is None, then all instances of the flow_ref and direction will be marked
        non-reference.

        Difficult to validate because any of the three arguments is allowed to be None (and even in nontrivial
        combinations)

        :param process_ref:
        :param flow_ref:
        :param direction:
        :param kwargs:
        :return:
        """
        print('UnSetting reference %s [%s] for %s' % (flow_ref, direction, process_ref))
        if process_ref is None:
            fl = self._archive.retrieve_or_fetch_entity(flow_ref)
            for p in self._archive.entities_by_type('process'):
                for x in p.references():
                    if x.flow is fl and x.direction == direction:
                        x.process.remove_reference(x.flow, x.direction)
        else:
            pr = self._archive.retrieve_or_fetch_entity(process_ref)
            if flow_ref is None:
                fds = [(x.flow, x.direction) for x in pr.references()]
                if direction is not None:
                    fds = [fd for fd in fds if fd[1] == direction]
                for fd in fds:
                    pr.remove_reference(*fd)
                return
            fl = self._archive.retrieve_or_fetch_entity(flow_ref)
            if direction is None:
                direction = self._check_direction(pr, fl)
            pr.remove_reference(fl, direction)

    def allocate_by_quantity(self, process_ref, quantity_ref, overwrite=False, **kwargs):
        """
        A ConfigAllocation provides a procedural mechanism for specifying quantity-wise allocations of processes at
        load time.  All that is required is a quantity; the process knows how to perform the allocation.  Note that
        reference flows not characterized with respect to the quantity will receive zero allocation.  So apply flow
        config first.

        :param process_ref:
        :param quantity_ref:
        :param overwrite:
        :param kwargs:
        :return:
        """
        print('Allocating %s by %s' % (process_ref, quantity_ref))
        p = self._archive.retrieve_or_fetch_entity(process_ref)
        qty = self._archive.retrieve_or_fetch_entity(quantity_ref)
        is_alloc = False
        if overwrite:
            for rf in p.reference_entity:
                p.remove_allocation(rf)
        else:
            for rf in p.reference_entity:
                is_alloc |= p.is_allocated(rf)
                '''
                try:
                    is_alloc |= p.is_allocated(rf)
                except MissingAllocation:
                    is_alloc = True
                    break
                '''

        # now apply the allocation
        if is_alloc:
            print('Allocation already detected for %s. Skipping this configuration.' % p)
        else:
            p.allocate_by_quantity(qty)
