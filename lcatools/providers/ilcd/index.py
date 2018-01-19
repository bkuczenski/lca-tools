from lcatools.implementations import IndexImplementation


class IlcdIndexImplementation(IndexImplementation):
    def lcia_methods(self, **kwargs):
        self._archive.load_lcia(load_all_flows=None)
        for l in self._archive.search('quantity', **kwargs):
            if l.is_lcia_method():
                yield l
