from lcatools.interfaces import PropertyExists
from synonym_dict.example_flowables import Flowable
from synonym_dict.example_compartments import Compartment

class FlowWithoutContext(Exception):
    pass


class FlowInterface(object):
    """
    An abstract class that establishes common functionality for OBSERVATIONS OF FLOWS.  A Flow consists of:
     - a reference quantity with a fixed unit [this is pending a bit of a refactor o reference_entity]
     - a flowable
     - a context (optionally a null context)
    """
    _context = None
    _flowable = None

    @property
    def reference_entity(self):
        return NotImplemented

    def unit(self):
        return self.reference_entity.unit()

    @property
    def configured(self):
        return not (self._flowable is None or self._context is None)

    @property
    def flowable(self):
        if self._flowable is None:
            raise FlowWithoutContext('Context was not set for flow %s!' % self)
        return self._flowable.name

    @flowable.setter
    def flowable(self, item):
        """
        allow to reassign flowable but not context
        :param item:
        :return:
        """
        # if self._flowable is not None:
        #     raise PropertyExists('Flowable already set! %s' % self.flowable)
        if not isinstance(item, Flowable):
            raise TypeError('not a flowable! (%s)' % type(item))
        self._flowable = item


    @property
    def context(self):
        """
        A flow's context needs to be set by its containing archive.  It should be an actual Context object.

        Legitimate question about whether this should raise an exception or return None. For now I think the safe thing
        is to catch the exception whenever it is noncritical.
        :return:
        """
        if self._context is None:
            raise FlowWithoutContext('Context was not set for flow %s!' % self)
        return self._context

    @context.setter
    def context(self, item):
        if self._context is not None:
            raise PropertyExists('Context already set! %s' % self.context)
        if not isinstance(item, Compartment):
            raise TypeError('Not a compartment! (%s)' % type(item))
        self._context = item

    def match(self, other):
        """
        Re-implement flow match method
        :param other:
        :return:
        """
        '''
        return (self.uuid == other.uuid or
                self['Name'].lower() == other['Name'].lower() or
                (trim_cas(self['CasNumber']) == trim_cas(other['CasNumber']) and len(self['CasNumber']) > 4) or
                self.external_ref == other.external_ref)  # not sure about this last one! we should check origin too
        '''
        if isinstance(other, str):
            return other in self._flowable
        return other.flowable in self._flowable

