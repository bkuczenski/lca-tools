from synonym_dict import SynonymSet


class FlowInterface(object):
    """
    An abstract class that establishes common functionality for OBSERVATIONS OF FLOWS.  A Flow consists of:
     - a reference quantity with a fixed unit
     - a flowable (a list of synonyms for the flowable substnce being described)
     - a context (a hierarchical list of strings designating the flows 'compartment' or category)
    """
    _context = ('cutoff', )
    _context_set_level = 0

    def _catch_context(self, key, value):
        """
        Add a hook to set context in __getitem__ or wherever is appropriate, to capture and automatically set context
        according to the following precedence:
         context > compartment > category > class | classification > cutoff (default)
        :param key:
        :param value:
        :return:
        """
        try:
            level = {'none': 0,
                     'class': 1,
                     'classification': 1,
                     'classifications': 1,
                     'category': 2,
                     'categories': 2,
                     'compartment': 3,
                     'compartments': 3,
                     'context': 4}[key.lower()]
        except KeyError:
            return
        if isinstance(value, str):
            value = (value, )
        if level > self._context_set_level:
            self._context_set_level = min([level, 3])  # always allow context spec to override
            self._context = tuple(value)

    def _catch_flowable(self, key, value):
        if key == 'name':
            self._flowable.add_term(value)
            self._flowable.set_name(value)
        elif key == 'casnumber':
            self._flowable.add_term(value)
        elif key == 'synonyms':
            if isinstance(value, str):
                self._flowable.add_term(value)
            else:
                for v in value:
                    self._flowable.add_term(v)

    __flowable = None

    @property
    def _flowable(self):
        if self.__flowable is None:
            self.__flowable = SynonymSet()
        return self.__flowable

    @property
    def reference_entity(self):
        return NotImplemented

    def unit(self):
        return self.reference_entity.unit()

    @property
    def name(self):
        return self._flowable.name

    @name.setter
    def name(self, name):
        """

        :param name:
        :return:
        """
        if name is not None and name in self.synonyms:
            self._flowable.set_name(name)

    @property
    def synonyms(self):
        for t in self._flowable.terms:
            yield t

    @property
    def context(self):
        """
        A flow's context is any hierarchical tuple of strings (generic, intermediate, specific).
        :return:
        """
        return self._context

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
        return any([t in self._flowable for t in other.synonyms])


class DummyFlow(FlowInterface):

    class DummyQuantity(object):
        origin = 'local.dummy.quantity'
        entity_type = 'quantity'
        external_ref = 'dummy'
        uuid = None
        is_entity = False

        def __getitem__(self, key):
            return 'Dummy Property'

        def has_property(self, key):
            return False

        @property
        def link(self):
            return '%s/%s' % (self.origin, self.external_ref)

        @staticmethod
        def unit():
            return 'd'

    _reference_entity = DummyQuantity()

    @property
    def reference_entity(self):
        return self._reference_entity
