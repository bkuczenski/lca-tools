from collections import defaultdict
import os
import json

from lcatools.catalog.lc_resource import LcResource


class LcCatalogResolver(object):
    """
    The resolver maintains a collection of resources, and translates semantic references into physical archives.
    The Catalog supplies a request and a level requirement
     It also acts as a factory for those resources, so when a request is provided, it is answered with a live archive.

     Then the Catalog turns that into a static archive and keeps a list of it. The catalog also keeps a separate
     list of study foregrounds (which are not static; which contain fragments). These can be converted into static
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
        return list(self._resources.keys())

    def _update_semantic_ref(self, ref):
        resources = LcResource.from_json(os.path.join(self._resource_dir, ref))
        self._resources[ref] = resources

    def index_resources(self):
        for res in os.listdir(self._resource_dir):
            self._update_semantic_ref(res)

    def add_resource(self, resource):
        resource.write_to_file(self._resource_dir)
        self._update_semantic_ref(resource.reference)

    def new_resource(self, ref, source, ds_type, **kwargs):
        new_res = LcResource(ref, source, ds_type, **kwargs)
        self.add_resource(new_res)

    def resolve(self, ref, interfaces=None):
        for res in self._resources[ref]:
            if res.satisfies(interfaces):
                yield res

    def write_resource_files(self):
        for ref, resources in self._resources.items():
            j = [k.serialize() for k in resources]
            with open(os.path.join(self._resource_dir, ref), 'w') as fp:
                json.dump(fp, {ref: j})
