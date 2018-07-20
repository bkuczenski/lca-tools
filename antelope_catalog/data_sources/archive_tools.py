from collections import namedtuple
from lcatools.entities import MissingAllocation
from lcatools.exchanges import Exchange

'''
A ConfigFlowCharacterization provides a procedural mechanism for specifying flow quantity characterizations after
loading an archive.  The 'flow_ref' and 'quantity_ref' have to lookup successfully in the archive.  Note- provisioning
the archive itself is ad hoc. Don't have a structural way to do that yet, but someday it could be done with linked
data (maybe?)
'''
ConfigFlowCharacterization = namedtuple("ConfigFlowCharacterization", ('flow_ref', 'quantity_ref', 'value'))

'''
A ConfigAllocation provides a procedural mechanism for specifying quantity-wise allocations of processes at load time.
All that is required is a quantity; the process knows how to perform the allocation.  Note that reference flows not
characterized with respect to the quantity will receive zero allocation.  So apply_flow_config first.
'''
ConfigAllocation = namedtuple("ConfigAllocation", ('process_ref', 'quantity_ref'))

'''
A ConfigBadReference provides a procedural mechanism for removing automatically-tagged reference flows, or for marking
a byproduct as non-reference or non-allocatable.  The parts are 'process_ref', 'flow_ref', and 'direction', but if
process_ref is None, then all instances of the flow_ref and direction will be marked non-reference.'''
ConfigBadReference = namedtuple("ConfigBadReference", ('process_ref', 'flow_ref', 'direction'))


def apply_flow_config(archive, flow_characterizations, overwrite=False):
    """
    Applies a list of ConfigFlowCharacterizations to an archive.
    :param archive:
    :param flow_characterizations:
    :param overwrite: [False] overwrite if characterization is present
    :return: nothing- the changes are made in-place
    """
    for cfc in flow_characterizations:
        if not isinstance(cfc, ConfigFlowCharacterization):
            raise TypeError('Entry is not a ConfigFlowCharacterization\n%s' % cfc)
        flow = archive[cfc.flow_ref]
        qty = archive[cfc.quantity_ref]
        if flow.has_characterization(qty):
            if overwrite:
                flow.del_characterization(qty)
            else:
                print('Flow %s already characterized for %s. Skipping.' % (flow, qty))
                pass
        flow.add_characterization(qty, value=cfc.value)


def apply_allocation(archive, allocations, overwrite=False):
    """
    Applies a list of ConfigAllocations to an archive.

    If overwrite is True, the process's allocations are first removed.

    If overwrite is False, the process is tested for allocation under each of its reference flows- if any are
    already allocated, the allocation is aborted for the process.

    :param archive:
    :param allocations:
    :param overwrite: [False] whether to strike and re-apply allocations if they already exist.
    :return:
    """
    for al in allocations:
        if not isinstance(al, ConfigAllocation):
            raise TypeError('Entry is not a ConfigAllocation\n%s' % al)
        p = archive[al.process_ref]
        qty = archive[al.quantity_ref]
        is_alloc = False
        if overwrite:
            for rf in p.reference_entity:
                p.remove_allocation(rf)
        else:
            for rf in p.reference_entity:
                try:
                    is_alloc |= p.is_allocated(rf)
                except MissingAllocation:
                    is_alloc = True
                    break

        # now apply the allocation
        if is_alloc:
            print('Allocation already detected for %s. Skipping this configuration.' % p)
            continue
        else:
            p.allocate_by_quantity(qty)


def remove_bad_references(archive, bad_references):
    """

    :param archive:
    :param bad_references: (process_ref, flow_ref, direction) - simply unset the reference exchange on the process
      (None, flow_ref, direction) - unset the reference exchange on all processes in which it appears.
    :return:
    """
    for br in bad_references:
        if not isinstance(br, ConfigBadReference):
            raise TypeError('Entry is not a ConfigBadReference\n%s' % br)
        fl = archive[br.flow_ref]
        if br.process_ref is None:
            for x in archive.exchange_values(fl, direction=br.direction):
                if x.is_reference:
                    x.process.remove_reference(x)
        else:
            pr = archive[br.process_ref]
            pr.remove_reference(Exchange(pr, fl, br.direction))
