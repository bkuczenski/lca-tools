from lcatools.characterizations import Characterization

class QuelledCF(Characterization):

    @classmethod
    def from_cf(cls, cf, flowable=None):
        if flowable is None:
            flowable = cf.flowable
        quelled = cls(flowable, cf.ref_quantity, cf.quantity, cf.context, cf.origin)
        for l in cf.locations():
            quelled.add_value(value=0.0, location=l)
        return quelled

    @property
    def value(self):
        return 0.0

    @value.setter
    def value(self, val):
        raise TypeError('Cannot set value')

    def update_values(self, **kwargs):
        raise TypeError('Cannot set values')

    def __setitem__(self, item, value):
        self._locations[item] = 0.0

    def add_value(self, value=None, location=None, overwrite=False):
        super(QuelledCF, self).add_value(value=0.0, location=location)


