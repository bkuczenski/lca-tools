"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals

import six
import uuid

from lcatools.interfaces import ArchiveInterface, to_uuid

if six.PY2:
    bytes = str
    str = unicode

    """
class PrefixArchive(ArchiveInterface):
    A class to handle the prefixing-- well maybe this is case specific----
    we can't multiply inherit this because both this and NsUuidArchive need to add to the
    serialization.

    ans: need a better way to handle serialization, and multiple inheritance
    """


class NsUuidArchive(ArchiveInterface):
    """
    A class that generates UUIDs in a namespace using a supplied key
    """
    def __init__(self, ref, ns_uuid=None, **kwargs):
        super(NsUuidArchive, self).__init__(ref, **kwargs)

        # internal namespace UUID for generating keys

        if ns_uuid is None:
            if self._upstream is not None:
                if isinstance(self._upstream, NsUuidArchive):
                    ns_uuid = self._upstream._ns_uuid

        ns_uuid = to_uuid(ns_uuid)  # if it's already a uuid, keep it; if it's a string, find it; else None

        self._ns_uuid = uuid.uuid4() if ns_uuid is None else ns_uuid

    def key_to_id(self, key):
        """
        Converts Ecospold01 "number" attributes to UUIDs using the internal UUID namespace.
        :param key:
        :return:
        """
        if isinstance(key, int):
            key = str(key)
        u = to_uuid(key)
        if u is not None:
            return u
        if six.PY2:
            return uuid.uuid3(self._ns_uuid, key.encode('utf-8'))
        else:
            return uuid.uuid3(self._ns_uuid, key)

    def serialize(self, **kwargs):
        j = super(NsUuidArchive, self).serialize(**kwargs)
        j['nsUuid'] = str(self._ns_uuid)


