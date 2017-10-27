from collections import defaultdict
import os
import json

from lcatools.catalog.lc_resource import LcResource


class UnknownOrigin(Exception):
    pass


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
        resources = LcResource.from_json(os.path.join(self._resource_dir, ref))
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
        if store:
            resource.write_to_file(self._resource_dir)
            self._update_semantic_ref(resource.reference)
        else:
            self._resources[resource.reference].append(resource)

    def new_resource(self, ref, source, ds_type, store=True, **kwargs):
        new_res = LcResource(ref, source, ds_type, **kwargs)
        self.add_resource(new_res, store=store)
        return new_res

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

    def resolve(self, req, interfaces=None):
        """
        Fuzzy resolver returns all references that match the request and have equal or greater specificity.
        'uslci.clean' will match queries for 'uslci' but not for 'uslci.original' or 'uslci.clean.allocated'.
        However, 'uslci.clean.allocated' will match a query for 'uslci.clean'
        :param req:
        :param interfaces: could be a single interface specification or a list
        :return:
        """
        terms = req.split('.')
        origin_found = False
        for ref, res_list in self._resources.items():
            if ref.split('.')[:len(terms)] == terms:
                origin_found = True
                for res in res_list:
                    if res.satisfies(interfaces):
                        yield res
        if not origin_found:
            raise UnknownOrigin(req)

    def write_resource_files(self):
        for ref, resources in self._resources.items():
            j = [k.serialize() for k in resources]
            with open(os.path.join(self._resource_dir, ref), 'w') as fp:
                json.dump(fp, {ref: j})
