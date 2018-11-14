import json
import os
from collections import defaultdict

from lcatools.interfaces import UnknownOrigin
from ..lc_resource import LcResource


class LcCatalogResolver(object):
    """
    The resolver maintains a collection of resources, and translates semantic references into physical archives.
    The Catalog supplies a request and a level requirement
     It also acts as a factory for those resources, so when a request is provided, it is answered with a live archive.

     Then the Catalog turns that into a static archive and keeps a list of it. The catalog also keeps a separate
     list of foreground foregrounds (which are not static; which contain fragments). These can be converted into static
     archives by turning the fragments into processes.


    """
    def __init__(self, resource_dir):
        self._resource_dir = resource_dir
        if not os.path.exists(resource_dir):
            os.makedirs(resource_dir)
        self._resources = defaultdict(list)
        self.index_resources()

    @property
    def references(self):
        """
        Generates pairs: reference, list of supported interfaces
        :return:
        """
        for k, v in self._resources.items():
            ints = set()
            for r in v:
                for t in r.interfaces:
                    ints.add(t)
            yield k, sorted(list(ints))

    @property
    def sources(self):
        seen = set()
        for k, v in self._resources.items():
            for res in v:
                if res.source not in seen:
                    seen.add(res.source)
                    yield res.source

    def _update_semantic_ref(self, ref):
        path = os.path.join(self._resource_dir, ref)
        try:
            resources = LcResource.from_json(path)
        except json.JSONDecodeError:
            print('Skipping Invalid resource file %s' % path)
            # os.remove(path)
            return
        self._resources[ref] = resources

    def index_resources(self):
        for res in os.listdir(self._resource_dir):
            self._update_semantic_ref(res)

    def add_resource(self, resource, store=True):
        """
        Add a resource to the resolver's list.  By default, save the resource permanently as a file in resources dir.
        :param resource:
        :param store: [True] if False, add the resource to memory only
        :return:
        """
        if resource.exists(self._resource_dir) or self.has_resource(resource):
            # do nothing
            print('Resource already exists')
            return
        if store:
            resource.write_to_file(self._resource_dir)
        self._resources[resource.reference].append(resource)

    def has_resource(self, resource):
        s = resource.serialize()
        return any(k.matches(s) for k in self._resources[resource.reference])

    def new_resource(self, ref, source, ds_type, store=True, **kwargs):
        new_res = LcResource(ref, source, ds_type, **kwargs)
        self.add_resource(new_res, store=store)
        return new_res

    def delete_resource(self, resource):
        ref = resource.reference
        res = self._resources[ref]
        if resource not in res:
            raise KeyError('Resource not found by resolver (ref: %s)' % ref)
        res.remove(resource)
        self._write_or_delete_resource_file(ref, res)
        if len(res) == 0:
            self._resources.pop(ref)

    def known_source(self, source):
        try:
            next(self.resources_with_source(source))
        except StopIteration:
            return False
        return True

    def resources_with_source(self, source):
        for ref, ress in self._resources.items():
            for r in ress:
                if r.source == source:
                    yield r

    def is_permanent(self, resource):
        return resource.exists(self._resource_dir)

    def resolve(self, req, interfaces=None, strict=False):
        """
        Fuzzy resolver returns all references that match the request and have equal or greater specificity.
        'uslci.clean' will match queries for 'uslci' but not for 'uslci.original' or 'uslci.clean.allocated'.
        However, 'uslci.clean.allocated' will match a query for 'uslci.clean'
        :param req:
        :param interfaces: could be a single interface specification or a list
        :param strict: [False] if true, only yields interface for which req matches ref
        :return:
        """
        terms = req.split('.')
        origin_found = False
        for ref, res_list in self._resources.items():
            if strict:
                if ref != req:
                    continue
            if ref.split('.')[:len(terms)] == terms:
                origin_found = True
                for res in res_list:
                    if res.satisfies(interfaces):
                        yield res
        if not origin_found:
            raise UnknownOrigin(req)

    def get_resource(self, ref=None, iface=None, source=None, strict=True, include_internal=True):
        """
        The purpose of this function is to allow a user to retrieve a resource by providing enough information to
        identify it uniquely.  If strict is True (default), then parameters are matched exactly and more than one
        match raises an exception. If strict is False, then origins are matched approximately and the first
        (lowest-priority) match is returned.

        The convention is that no two resources should have the same source, so if a source is provided then the
        output of resources_with_source() is used.  Otherwise, the output of resolve() is used.  Internal resources
        (indexes and archives) are never returned. [WHY?]
        :return: a single LcResource
        """
        if source is not None:
            _gen = self.resources_with_source(source)
        else:
            _gen = self.resolve(ref, interfaces=iface, strict=strict)
        if include_internal:
            matches = sorted([r for r in _gen], key=lambda x: x.priority)
        else:
            matches = sorted([r for r in _gen if not r.internal], key=lambda x: x.priority)
        if len(matches) > 1:
            if strict:
                for k in matches:
                    for i in k.interfaces:
                        print('%s:%s [priority %d]' % (k.source, i, k.priority))
                raise ValueError('Ambiguous matches for supplied parameters')
        elif len(matches) == 0:
            raise KeyError('no resource found; ref:%s iface:%s source=%s' % (ref, iface, source))
        return matches[0]

    def _write_or_delete_resource_file(self, ref, resources):
        j = [k.serialize() for k in resources if k.exists(self._resource_dir)]
        if len(j) == 0:
            os.remove(os.path.join(self._resource_dir, ref))
            return
        with open(os.path.join(self._resource_dir, ref), 'w') as fp:
            json.dump({ref: j}, fp)

    def write_resource_files(self):
        for ref, resources in self._resources.items():
            self._write_or_delete_resource_file(ref, resources)
