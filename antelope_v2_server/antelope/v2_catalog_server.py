import os
import json

from antelope_core import LcCatalog
from .lc_pub import AntelopeV1Pub, AntelopeV2Pub


class CatalogServer(LcCatalog):
    """
    Catalog subclass that is enabled to generate publications (both v1 and v2)

    pub declarations are stored in an antelope_declarations file as JSON and include the information required to
    recreate them.  At initialization, the servers are created and stored in a dict.

    Tentatively calling this an LcServer. HERE is where the privacy and authorization declarations go, so TEAR THOSE
    OUT of the resources + implementations.
    """
    @property
    def _antelope_path(self):
        return os.path.join(self._rootdir, 'antelope')

    @property
    def _dirs(self):
        for x in super(CatalogServer, self)._dirs:
            yield x
        yield self._antelope_path

    def _register_server(self, pub):
        self._servers[pub.name] = pub

    def _check_server(self, name):
        """
        Raises an error if the name already exists: KeyError if it is instantiated; FileExistsError if it is saved but
        not instantiated
        :param name:
        :return:
        """
        if name in self._servers:
            pub = self._servers[name]
            if pub.type == 'Antelope_v1':
                raise KeyError('%s is already associated with an Antelope V1 server' % name)
            elif pub.type == 'Antelope_v2':
                raise KeyError('%s is already associated with an Antelope V2 server' % name)
            else:
                raise ValueError('%s is associated with an unknown server type %s' % (name, pub.type))
        path = os.path.join(self._antelope_path, name)
        if os.path.exists(path):
            raise FileExistsError('File exists: %s' % path)

    def delete_server(self, name):
        path = os.path.join(self._antelope_path, name)
        if os.path.exists(path):
            os.remove(path)

    def _init_antelope(self, data):
        if data['type'] == 'Antelope_v1':
            frag = self.fetch_link(data['fragment'])
            pub = AntelopeV1Pub(data['name'], frag, lcia_methods=data['lcia'], mapping=data['mapping'])
        else:
            pub = AntelopeV2Pub(self.query(data['name']), interfaces=data['interfaces'],
                                privacy=data['privacy'])
        self._register_server(pub)

    def __init__(self, *args, **kwargs):
        super(CatalogServer, self).__init__(*args, **kwargs)
        self._servers = dict()

        for res in os.listdir(self._antelope_path):
            with open(os.path.join(self._antelope_path, res)) as fp:
                j = json.load(fp)
            self._init_antelope(j)

    @property
    def servers(self):
        for x in self._servers.keys():
            yield x

    def __getitem__(self, item):
        try:
            return self._servers[item]
        except KeyError:
            try:
                return next(v for k, v in self._servers.items()
                            if k.startswith(item))  # not sure if this or item.startswith(k)
            except StopIteration:
                raise KeyError

    def define_av2_instance(self, name, interfaces, **kwargs):
        self._check_server(name)
        av2 = AntelopeV2Pub(self.query(name), interfaces, **kwargs)
        av2.write_to_file(self._antelope_path)
        self._register_server(av2)

    def define_av1_instance(self, foreground, fragment, lcia_methods, **kwargs):
        self._check_server(foreground)
        av1 = AntelopeV1Pub(foreground, fragment, lcia_methods=lcia_methods, **kwargs)
        self._register_server(av1)
