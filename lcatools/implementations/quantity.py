from .basic import BasicImplementation
from ..characterizations import QRResult
from ..interfaces import QuantityInterface, EntityNotFound, NoFactorsFound, ConversionReferenceMismatch
from ..lcia_results import LciaResult


class QuantityConversion(object):
    def __init__(self, *args):
        self._results = []
        for arg in args:
            self.add_result(arg)

    def add_result(self, qrr):
        if isinstance(qrr, QRResult):
            if len(self._results) > 0:
                if self.ref != qrr.query:
                    raise ConversionReferenceMismatch('%s != %s' % (self._results[-1].ref, qrr.query))
            self._results.append(qrr)
        else:
            raise TypeError('Must supply a QRResult')

    @property
    def query(self):
        return self._results[0].query

    @property
    def ref(self):
        return self._results[-1].ref

    @staticmethod
    def _invert_qrr(qrr):
        return QRResult(qrr.query, qrr.flowable, qrr.context, qrr.ref, qrr.locale, qrr.origin, 1.0 / qrr.value)

    def add_inverted_result(self, qrri):
        self.add_result(self._invert_qrr(qrri))

    @property
    def value(self):
        val = 1.0
        for res in self._results:
            val *= res.value
        return val

    def __getitem__(self, item):
        return self._results[item]

    # TODO: add serialization, other outputs


class QuantityImplementation(BasicImplementation, QuantityInterface):
    """
    Uses the archive's term manager to index cfs, by way of the canonical quantities
    """
    def quantities(self, **kwargs):
        for q_e in self._archive.search('quantity', **kwargs):
            yield q_e

    def get_canonical(self, quantity, **kwargs):
        """
        Retrieve a canonical quantity from a qdb
        :param quantity: external_id of quantity
        :return: quantity entity
        """
        return self.get(quantity)

    def profile(self, flow, **kwargs):
        """
        Generate characterizations for the named flow, with the reference quantity noted
        :param flow:
        :param kwargs:
        :return:
        """
        f = self.get(flow)
        for cf in f.characterizations():
            yield cf

    def factors(self, quantity, flowable=None, compartment=None, dist=0):
        q = self.get_canonical(quantity)
        for cf in self._archive.tm.factors_for_quantity(q, flowable=flowable, compartment=compartment, dist=dist):
            yield cf

    def _ref_qty_conversion(self, ref_quantity, flowable, compartment, res, locale):
        """
        Transforms a CF into a quantity conversion with the proper ref quantity
        :param ref_quantity:
        :param flowable:
        :param compartment:
        :param res: a QRResult
        :param locale:
        :return:
        """
        if res[0].ref != ref_quantity:
            try:
                res.add_result(self.quantity_relation(ref_quantity, flowable, compartment, res[0].ref, locale=locale,
                                                      dist=3))
            except NoFactorsFound:
                try:
                    res.add_inverted_result(self.quantity_relation(ref_quantity, flowable, compartment, res[0].ref,
                                                                   locale=locale, dist=3))
                except NoFactorsFound:
                    raise ConversionReferenceMismatch('Flow %s\nfrom %s\nto %s' % (flowable,
                                                                                   res.query,
                                                                                   ref_quantity))
        return res

    def _quantity_results(self, ref_quantity, flowable, compartment, query_quantity, locale='GLO', strategy='highest',
                          **kwargs):
        """
        self is the query quantity.
        :param ref_quantity:
        :param flowable: a string that is synonymous with a known flowable.
        :param compartment: a string that is synonymous with a known context
        :param query_quantity:
        :param locale: ['GLO']
        :param strategy: approach for resolving multiple-CF in dist>0.  ('highest' | 'lowest' | 'average' | ...? )
        :param kwargs:
         dist: CLookup distance (0=exact 1=subcompartments 2=parent 3=all parents)
        :return: 3-tuple: qr_results, qr_mismatch, qr_geog
         qr_results: valid conversions from query quantity to ref quantity
         qr_mismatch: conversions from query quantity to a different quantity that could not be further converted
         qr_geog: valid conversions which had a broader spatial scope than specified, if at least one narrow result

        """
        # TODO: port qdb functionality: detect unity conversions; quell biogenic co2; integrate convert()
        # first- check to see if the query quantity can be converted to the ref quantity directly:
        qq = self.get_canonical(query_quantity)
        rq = self.get_canonical(ref_quantity)
        if qq.has_property('UnitConversion'):
            try:
                fac = qq.convert(to=rq.unit())
                return fac
            except KeyError:
                pass

        cfs = [cf for cf in self.factors(qq, flowable, compartment, **kwargs)]
        if len(cfs) == 0:
            raise NoFactorsFound('%s [%s] %s', (flowable, compartment, self))

        qr_results = []
        qr_mismatch = []
        qr_geog = []
        for cf in cfs:
            res = QuantityConversion(cf.query(locale))
            try:
                qr_results.append(self._ref_qty_conversion(rq, flowable, compartment, res, locale))
            except ConversionReferenceMismatch:
                qr_mismatch.append(res)

        if len(qr_results) > 1:
            qr_geog = [k for k in filter(lambda x: x[0].locale != locale, qr_results)]
            qr_results = [k for k in filter(lambda x: x[0].locale == locale, qr_results)]

        return qr_results, qr_mismatch, qr_geog

    def cf(self, flow, quantity, locale='GLO', **kwargs):
        """

        :param flow:
        :param quantity:
        :param locale:
        :param kwargs:
        :return:
        """
        f = self.get(flow)
        if f is None:
            raise EntityNotFound(f)
        return self.quantity_relation(f.reference_entity, f.flowable, f.context, quantity, locale=locale,
                                      **kwargs)

    def quantity_relation(self, ref_quantity, flowable, compartment, query_quantity, locale='GLO', strategy='highest',
                          **kwargs):
        """
        Return a single number that converts the a unit of the reference quantity into the query quantity for the
        given flowable, compartment, and locale (default 'GLO').  If the locale is not found, this would be a great
        place to run a spatial best-match algorithm.
        :param ref_quantity: convert for 1 unit of this quantity
        :param flowable: a string that is synonymous with a flowable characterized by the query quantity
        :param compartment: a string synonym for a context / "archetype"? (<== locale-specific?)
        :param query_quantity: convert to this quantty
        :param locale: handled by CF; default 'GLO'
        :param strategy: approach for resolving multiple-CF in dist>0.  ('highest' | 'lowest' | 'average' | ...? )
        :param kwargs:
         dist: CLookup distance (0=exact 1=subcompartments 2=parent compartment 3=all parents)
        :return:
        """
        qr_results, qr_mismatch, _ = self._quantity_results(ref_quantity, flowable, compartment,
                                                            query_quantity, locale=locale, **kwargs)

        if len(qr_results) > 1:
            # this is obviously punting
            if strategy == 'highest':
                return max(v.value for v in qr_results)
            elif strategy == 'lowest':
                return min(v.value for v in qr_results)
            elif strategy == 'average':
                return sum(v.value for v in qr_results) / len(qr_results)
            else:
                raise ValueError('Unknown strategy %s' % strategy)
        elif len(qr_results) == 1:
            return qr_results[0].value
        else:
            if len(qr_mismatch) > 0:
                for k in qr_mismatch:
                    print('Flowable: %s\nfrom: %s\nto: %s' % (k[0].flowable, k.ref, ref_quantity))
                raise ConversionReferenceMismatch
            else:
                raise NoFactorsFound

    def do_lcia(self, quantity, inventory, locale='GLO', **kwargs):
        """
        Successively implement the quantity relation over an iterable of exchanges.

        man, WHAT is the qdb DOING with all those LOC? (ans: seemingly a lot)

        :param quantity:
        :param inventory:
        :param locale:
        :param kwargs:
        :return:
        """
        q = self.get_canonical(quantity)
        res = LciaResult(q)
        for x in inventory:
            if x.termination is None:
                res.add_cutoff(x)
                continue
            ref_q = self.get_canonical(x.flow.reference_entity)
            cf = self.quantity_relation(ref_q, x.flow['Name'], x.termination, locale=locale,
                                        **kwargs)
            res.add_score(x.process, x, cf, locale)
            # TODO: lcia_result remodel
            # should we characterize the flows? to save on lookups? no, leave that to the client
        return res
