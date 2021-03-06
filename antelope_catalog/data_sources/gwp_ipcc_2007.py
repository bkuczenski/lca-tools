from .data_source import DataSource
from antelope_utilities.lcia.ipcc_2007_traci import AUTHORIZED_REF
from lcatools.qdb import IPCC_2007_GWP


class GwpIpcc2007(DataSource):
    _ds_type = 'json'

    @property
    def references(self):
        """
        There's a precedence issue here, becaue the class MUST yield the same reference name as what's encoded in
        the JSON file, but it is wasteful to load the JSON file just to learn the canonical ref.  The "proper" way to
        do this is to [better] manage the workflow by which the JSON file is created (i.e. in antelope_utilities)
        and then use the same ref for both paths.

        heh- I hint at this challenge in the definite source by naming it AUTHORIZED_REF
        :return:
        """
        yield AUTHORIZED_REF

    def interfaces(self, ref=AUTHORIZED_REF):
        for i in ('index', 'quantity'):
            yield i

    def make_resources(self, ref=AUTHORIZED_REF):
        if ref in self.references:
            yield self._make_resource(ref, source=IPCC_2007_GWP, interfaces=self.interfaces(), static=True)
