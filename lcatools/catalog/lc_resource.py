import os
import json
from lcatools.catalog.interfaces import INTERFACE_TYPES
from lcatools.providers.interfaces import local_ref


class LcResource(object):
    """
    This is a record that links a semantic reference to a physical data source, and specifies the capabilities
    (and someday, access limitations) of the data source.

    The LcResource serializes to a json file with the following format:
    { ref: [ { "dataSource": source, "dataSourceType": ds_type, .... }, ... ] }
    where ref is the semantic reference.

    """
    @classmethod
    def from_archive(cls, archive, interfaces, **kwargs):
        source = archive.source
        if source in archive.get_names:
            ref = archive.get_names[source]
        else:
            ref = local_ref(source)
        ds_type = type(archive)  # static flag indicates whether archive is complete
        kwargs.update(archive.init_args)
        return cls(ref, source, ds_type, interfaces=interfaces, static=archive.static, **kwargs)

    @classmethod
    def from_dict(cls, ref, d):
        """
        Returns a single LcResource loaded from a dict.  only required fields are 'dataSource' and 'dataSourceType';
        other fields are passed to the constructor and either interpreted directly or added as supplemental args
        :param ref:
        :param d:
        :return:
        """
        source = d.pop('dataSource')
        ds_type = d.pop('dataSourceType')
        return cls(ref, source, ds_type, **d)

    @classmethod
    def from_json(cls, file):
        """
        generates LcResources contained in the named file, sorted by increasing priority.  The filename and
        the reference must be the same.
        :param file:
        :return: an ordered list of resources
        """
        ref = os.path.basename(file)
        with open(file, 'r') as fp:
            j = json.load(fp)

        return sorted([cls.from_dict(ref, d) for d in j[ref]], key=lambda x: x.priority)

    def __init__(self, reference, source, ds_type, interfaces=None, privacy=0, priority=0, static=False, **kwargs):
        """

        :param reference: semantic reference
        :param source: physical data source
        :param ds_type: data source type
        :param interfaces: list which can include 'entity', 'foreground', or 'background'. Default 'foreground'
        :param privacy: privacy level... TBD... 0 = public, 1 = exchange values private, 2 = all exchanges private
        :param priority: priority level.. 0-100 scale, lowest priority resource is loaded first
        :param static: [False] if True, load_all() after initializing
        :param kwargs: additional keyword arguments to constructor
        """
        self._ref = reference
        self._source = source
        self._type = ds_type
        self._static = static

        if interfaces is None:
            interfaces = ['foreground']

        if isinstance(interfaces, str):
            interfaces = [interfaces]

        for k in interfaces:
            if k not in INTERFACE_TYPES:
                raise ValueError('Unknown interface type %s' % k)

        self._interfaces = interfaces
        self._privacy = int(privacy)
        self._priority = int(priority)
        self._args = kwargs

    @property
    def reference(self):
        return self._ref

    @property
    def source(self):
        return self._source

    @property
    def ds_type(self):
        return self._type

    @property
    def interfaces(self):
        for k in self._interfaces:
            yield k

    @property
    def privacy(self):
        return self._privacy

    @property
    def priority(self):
        return self._priority

    @property
    def static(self):
        return self._static

    @property
    def init_args(self):
        return self._args

    def satisfies(self, ifaces):
        if ifaces is None:
            return True
        if isinstance(ifaces, str):
            ifaces = [ifaces]
        for i in ifaces:
            if i in self._interfaces:
                return True
        return False

    def serialize(self):
        j = {
            "dataSource": self.source,
            "dataSourceType": self.ds_type,
            "interfaces": [k for k in self.interfaces],
            "priority": self.priority,
            "privacy": self.privacy,
            "static": self.static
        }
        j.update(self._args)
        return j

    def write_to_file(self, path):
        """
        Adds the resource to a file whose name is the resource's semantic reference. If the same datasource is
        already present in the file, replace it with the current resource.  otherwise append.
        :param path: directory to store the resource file.
        :return:
        """
        if not os.path.isdir(path):
            if os.path.exists(path):
                raise ValueError('Please provide a directory path')
            os.makedirs(path)

        filename = os.path.join(path, self.reference)
        if os.path.exists(filename):
            with open(filename, 'r') as fp:
                j = json.load(fp)

            resources = [k for k in j[self.reference] if k['dataSource'] != self.source]
            resources.append(self.serialize())
        else:
            resources = [self.serialize()]
        with open(os.path.join(path, self.reference), 'w') as fp:
            json.dump({self.reference: resources}, fp, indent=2)
