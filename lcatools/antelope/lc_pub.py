"""
A data structure describing the publication of an LCA data resource.

Each publication has two forms:
 (1) the serialized form lives in the antelope directory and provides enough information to reconstitute the

This object is supposed to provide the basic information and functionality common to both v1 and v2 resources,
each of which is a subclass with specialized properties.


"""
from numbers import Number
import os
import json

from .authorization import allowed_interfaces, PrivacyDeclaration


class CatalogRequired(Exception):
    pass


class LcPub(object):
    """
    Abstract class that handles de/serialization and common features
    """
    _type = None

    @property
    def name(self):
        raise NotImplementedError

    def serialize(self):
        raise NotImplementedError

    def write_to_file(self, path):
        if os.path.exists(os.path.join(path, self.name)):
            raise FileExistsError('Resource is already specified')
        with open(os.path.join(path, self.name), 'w') as fp:
            json.dump(self.serialize(), fp, indent=2, sort_keys=True)


class AntelopeV1Pub(LcPub):
    """
    An Antelope V1 publication is a record of a ForegroundStudy and a list of supported LCIA methods.  In order to
    create it, we need to pass the things necessary to create the ForegroundStudy.  but since that class doesn't exist
    yet, neither does this.

    Conceptually, we need:
     - a CatalogRef for the study's top level fragment
     - an iterable of lcia methods, being either caby ref (or by uuid * given that lcia methods should be uniquely
        determined)
     - an optional mapping between entity refs and indices for 'flows', 'flowproperties', 'processes', 'fragments'
       : otherwise these are determined by the order encountered when traversing the top level fragment and children
    """
    _type = 'Antelope_v1'

    @property
    def name(self):
        return self._foreground

    def __init__(self, foreground, fragment_ref, lcia_methods=None, mapping=None):
        """

        :param foreground:
        :param fragment_ref:
        :param lcia_methods:
        :param mapping:
        """
        self._foreground = foreground

        if not fragment_ref.resolved:
            raise CatalogRequired('Fragment ref is not grounded!')

        self._fragment = fragment_ref

        self._lcia = lcia_methods or []

        mapping = mapping or dict()

        if not isinstance(mapping, dict):
            raise TypeError('Mapping must be a dict')

        self._mapping = mapping  # ultimately this needs to be populated by traversing the fragment

        self._reverse_mapping = dict()
        self._populate_mapping()
        self._reverse_map()

    def _populate_mapping(self):
        """
        Beginning at the top-level fragment, traverse the model and identify all local fragments (parent + child)
        encountered during a traversal. From that, derive a list of stage names, flows, processes, and flow properties,
        and ensure that all are present in the mapping.
        :return:
        """

    @staticmethod
    def _enum(lst):
        return {k: i for i, k in enumerate(lst)}

    def _reverse_map(self):
        self._reverse_mapping['lcia'] = self._enum(self._lcia)
        for k in 'flow', 'flowproperty', 'fragment', 'process', 'stage':
            self._reverse_mapping[k] = self._enum(self._mapping[k])

    def serialize(self):
        return {
            'type': self._type,
            'name': self.name,
            'fragment': self._fragment.link,
            'lcia': self._lcia,
            'mapping': self._mapping
        }


class AntelopeV2Pub(LcPub):
    """
    An Antelope V2 publication is a catalog-supported publication of a complete LCA data resource, denoted by semantic
    origin.  It is instantiated essentially in the form of a CatalogQuery, which very little else to do, other than
    a privacy specification.
    """
    _type = 'Antelope_v2'

    @property
    def name(self):
        return self._query.origin

    def __init__(self, query, interfaces=allowed_interfaces, privacy=None):
        """

        :param query: a grounded query
        :param interfaces: interfaces to allow access
        :param privacy: a privacy specification: either a blanket number or a dict.
          if None, all information is public (though limited to the named interfaces)
          if a number, all queries must be authorized with a privacy score lower than or equal to the number
          if a dict, queries having the specified scope must authorize with a privacy score lower than or equal to the
          corresponding value.  The lowest privacy score is 0, so a negative number means authorization is not possible.
          Only keys in the list of known scopes are retained
        """
        self._query = query
        if isinstance(interfaces, str):
            interfaces = (interfaces,)
        self._interfaces = tuple(k for k in interfaces if k in allowed_interfaces)
        if isinstance(privacy, dict):
            self._scopes = PrivacyDeclaration.from_dict(privacy)
        else:
            self._scopes = PrivacyDeclaration(privacy)

    def serialize(self):
        return {
            'type': self._type,
            'name': self.name,
            'interfaces': self._interfaces,
            'privacy': self._scopes.serialize()
        }
