from lcatools.interact import ifinput, pick_one, pick_compartment, cyoa, parse_math, menu_list
from . import LcQuantity, LcFlow
# from antelope_interface import directions, comp_dir


class EntityEditor(object):
    """
    Abstract class with some common functions
    """
    @staticmethod
    def _edit_entity(entity):
        print('Select field to edit:')
        field = menu_list(*entity.keys())
        if field == -1 or field is None:
            return True
        new = ifinput('Enter new value for %s: ' % field, entity[field])
        if len(new) > 0:
            entity[field] = new
        else:
            print('Not updating.')

    def __init__(self, interactive=False):
        """

        :param interactive: whether to prompt the user for omitted data
        """
        self._interactive = interactive

    def set_interactive(self):
        self._interactive = True

    def unset_interactive(self):
        self._interactive = False

    def input(self, query, default):
        if self._interactive:
            return input(query)
        return default

    def ifinput(self, query, default):
        if self._interactive:
            return ifinput(query, default)
        return default

    def _print(self, string):
        if self._interactive:
            print(string)


class FlowEditor(EntityEditor):
    """
    Not even much of an object in itself, more like a collection of functions for creating and modifying LcEntities.
    I suppose it needs an instantiation so that it can store customization info. But really these are orphan
    functions and this class is just created to give them a home.

    Generally, the interactive portions should be removed and shifted to new code that uses the foreground interface
    to query + construct the foreground.  Currently the fragment editor is more programmatic where the flow editor is
     more interactive.  fragment stuff should probably be moved to the fg interface.
    """
    def __init__(self, qdb=None, **kwargs):
        """
        This class needs a quantity database to give the user access to properly defined quantities and compartments.
        :param qdb: [None] if None, interactive is forced to false
        """
        super(FlowEditor, self).__init__(**kwargs)
        if qdb is None:
            self.unset_interactive()
        self._qdb = qdb

    def new_quantity(self, name=None, unit=None, comment=None):
        name = name or self.input('Enter quantity name: ', 'New Quantity')
        unit = unit or self.input('Unit by string: ', 'unit')
        comment = comment or self.ifinput('Quantity Comment: ', '')
        q = LcQuantity.new(name, unit, Comment=comment)
        return q

    def new_flow(self, flow=None, name=None, cas=None, quantity=None, comment=None, compartment=None, local_unit=None,
                 **kwargs):
        if flow is None:
            name = name or self.input('Enter flow name: ', 'New flow')
            cas = cas or self.ifinput('Enter CAS number (or none): ', '')
            if quantity is None:
                if self._interactive:
                    print('Choose reference quantity or none to create new: ')
                    q = pick_one([fp for fp in self._qdb.flow_properties])
                    if q is None:
                        q = self.new_quantity()
                    quantity = q
                else:
                    print('Using mass as reference quantity')
                    quantity = self._qdb.get_canonical('mass')
            comment = comment or self.input('Enter flow comment: ', '')
            if compartment is None:
                if self._interactive:
                    print('Choose compartment:')
                    compartment = pick_compartment(self._qdb.c_mgr.compartments).to_list()
                else:
                    print('Designating Intermediate flow')
                    compartment = self._qdb.c_mgr.find_matching('Intermediate Flows').to_list()
            else:
                compartment = self._qdb.c_mgr.find_matching(compartment).to_list()
            if local_unit is not None:
                local_conv = quantity.convert(to=local_unit)
                if local_conv is None:
                    print('Falling back to default unit: %s' % quantity.unit())
                    local_unit = None

            flow = LcFlow.new(name, quantity, CasNumber=cas, Compartment=compartment, Comment=comment,
                              local_unit=local_unit, **kwargs)
        else:
            quantity = flow.reference_entity

        if self._interactive:
            flow.profile()
            while ifinput('Add characterizations for this flow? y/n', 'n') != 'n':
                ch = cyoa('[n]ew or [e]xisting quantity? ', 'en', 'e')
                if ch == 'n':
                    cq = self.new_quantity()
                else:
                    cq = pick_one(self._qdb.quantities())
                    if cq is None:
                        cq = self.new_quantity()
                val = parse_math(input('Value (1 %s = x %s): ' % (quantity.unit(), cq.unit())))
                flow.add_characterization(cq, value=val)

        return flow

    def edit_flow(self, flow):
        ch = cyoa('Edit (P)roperties or (C)haracterizations? ', 'pc').lower()
        if ch == 'p':
            self._edit_entity(flow)
        elif ch == 'c':
            self.edit_characterizations(flow)

    @staticmethod
    def edit_characterizations(flow):
        char = pick_one(cf for cf in flow.characterizations())
        val = float(ifinput('enter new characterization value: ', char.value))
        char.value = val
