"""


"""

import uuid

from lcatools.entities import LcEntity
from lcatools.literate_float import LiterateFloat


class InvalidParentChild(Exception):
    pass


class BalanceAlreadySet(Exception):
    pass


class LcFragment(LcEntity):
    """

    """

    _ref_field = 'parent'
    _new_fields = ['Parent', 'Flow', 'Direction', 'StageName']

    @classmethod
    def new(cls, name, *args, **kwargs):
        """
        :param name: the name of the process
        :param parent: the parent fragment, or None for reference flows
        :return:
        """
        return cls(uuid.uuid4(), Name=name, *args, **kwargs)

    def __init__(self, the_uuid, flow, direction, parent=None, private=False, balance_flow=False,
                 **kwargs):
        """
        Required params:
        :param the_uuid: use .new(Name, ...) for a random UUID
        :param flow: an LcFlow
        :param direction:
        :param parent:
        :param private:
        :param balance_flow:
        :param kwargs:
        """

        super(LcFragment, self).__init__('fragment', the_uuid, **kwargs)
        self._set_reference(parent)
        assert flow.entity_type == 'flow'
        self.flow = flow
        self.direction = direction  # w.r.t. parent

        self.private = private
        self._balance_flow = balance_flow

        self._conserved_quantity = None

        self.observed_magnitude = LiterateFloat(1.0)  # in flow's reference unit
        self.observed_exch_value = LiterateFloat(1.0)  # w.r.t parent activity level

        self._cached_exch_value = None
        self._cached_unit_scores = dict()  # of quantities

        if 'Flow' not in self._d:
            self._d['Flow'] = flow['Name']
        if 'Direction' not in self._d:
            self._d['Direction'] = direction
        if 'StageName' not in self._d:
            self._d['StageName'] = ''

    def __str__(self):
        if self.reference_entity is None:
            re = '(**) ref'
        else:
            re = self.reference_entity.get_uuid()[:7]
        return '(%s) [%s] -- %s : %s' % (re, self.direction, self.get_uuid()[:7], self.flow['Name'])

    def set_magnitude(self, magnitude, quantity=None):
        """
        Specify magnitude, optionally in a specified quantity. Otherwise a conversion is performed
        :param magnitude:
        :param quantity:
        :return:
        """
        if quantity is not None:
            magnitude = self.flow.convert(magnitude, fr=quantity)
        self.observed_magnitude = magnitude

    def set_exchange_value(self, exch_val, quantity=None):
        """
        Specify exchange value, in the specified quantity per unit activity level of the parent node.
         quantity defaults to flow's reference quantity
        :param exch_val:
        :param quantity:
        :return:
        """
        if quantity is not None:
            exch_val = self.flow.convert(exch_val, fr=quantity)
        self.observed_exch_value = exch_val

    def set_balance_flow(self):
        if self._balance_flow is False:
            self.reference_entity.set_conserved_quantity(self)
            self._balance_flow = True

    def unset_balance_flow(self):
        if self._balance_flow:
            self.reference_entity.unset_conserved_quantity()
            self._balance_flow = False

    def set_conserved_quantity(self, child):
        if child.parent != self:
            raise InvalidParentChild
        if self._conserved_quantity is not None:
            raise BalanceAlreadySet
        self._conserved_quantity = child.flow.reference_entity

    def unset_conserved_quantity(self):
        self._conserved_quantity = None


class FragmentFlow(object):
    """
    an immutable record of the termination of a traversal query. essentially an enhanced NodeCache record which
    can be easily serialized to an antelope fragmentflow record.
    """
    pass
