"""
Each archive now has a TermManager which interprets query arguments as synonyms for canonical flows and contexts.  This
can also be upgraded to an LciaEngine, which extends the synonymization strategy to quantities as well
"""
from .basic import BasicImplementation
from ..characterizations import QRResult
from ..interfaces import QuantityInterface, NoFactorsFound, ConversionReferenceMismatch, FlowableMismatch
from ..lcia_results import LciaResult


class RefQuantityRequired(Exception):
    pass


class QuantityConversion(object):
    """
    A stack of Quantity Relation results that are composed sequentially in order to render a flow-quantity conversion.
    The first QRR added should report the query quantity (numerator) in terms of some reference quantity (denominator);
    then each subsequent QRR should include the prior ref quantity as the query quantity.

    The QuantityConversion has a subset of the interface of a QRResult (flowable, ref, query, context, value), leaving
    out locale and origin for the time being since they could vary across factors.

    For instance, a Quantity conversion from moles of CH4 to GWP 100 might include first the GWP conversion and then
    the mol conversion:
    QuantityConversion(QRResult('methane', 'kg', 'kg CO2eq', 'emissions to air', 'GLO', 'ipcc.2007', 25.0),
                       QRResult('methane', 'mol', 'kg', None, 'GLO', 'local.qdb', 0.016))
    giving the resulting value of 0.4.
    """
    def __init__(self, *args):
        self._results = []
        for arg in args:
            self.add_result(arg)

    def add_result(self, qrr):
        if isinstance(qrr, QRResult):
            if len(self._results) > 0:
                if self.flowable != qrr.flowable:
                    raise FlowableMismatch('%s != %s' % (self.flowable, qrr.flowable))
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

    @property
    def flowable(self):
        return self._results[0].flowable

    @property
    def context(self):
        return self._results[0].context

    @staticmethod
    def _invert_qrr(qrr):
        """
        swaps the ref and query quantities and inverts the value
        :param qrr:
        :return:
        """
        return QRResult(qrr.flowable, qrr.query, qrr.ref, qrr.context, qrr.locale, qrr.origin, 1.0 / qrr.value)

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
        return self._archive.tm.get_canonical(quantity)

    def factors(self, quantity, flowable=None, context=None, dist=0):
        q = self.get_canonical(quantity)
        for cf in self._archive.tm.factors_for_quantity(q, flowable=flowable, context=context, dist=dist):
            yield cf

    def characterize(self, flowable, ref_quantity, query_quantity, value, context=None, location='GLO', **kwargs):
        """
        We gotta be able to do this
        :param flowable: string
        :param ref_quantity: string
        :param query_quantity: string
        :param value: float
        :param context: string
        :param location: string
        :param kwargs: overwrite=False, origin=self.origin
        :return:
        """
        rq = self.get_canonical(ref_quantity)
        qq = self.get_canonical(query_quantity)
        return self._archive.tm.add_characterization(flowable, rq, qq, value, context=context, location=location, **kwargs)

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
        if ref_quantity is None:
            raise ConversionReferenceMismatch('Cannot convert to None')
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

    def _quantity_conversions(self, flowable, rq, qq, context, locale='GLO',
                             **kwargs):
        """
        This is the main "comprehensive" engine for performing characterizations

        :param flowable: a string that is synonymous with a known flowable.
        :param rq: a canonical ref_quantity or None
        :param qq: a canonical query_quantity or None
        :param context: a string that is synonymous with a known context
        :param locale: ['GLO']
        :param kwargs:
         dist: CLookup distance (0=exact 1=subcompartments 2=parent 3=all parents)
        :return: 3-tuple of lists of QRResults objects: qr_results, qr_mismatch, qr_geog
         qr_results: valid conversions from query quantity to ref quantity
         qr_geog: valid conversions which had a broader spatial scope than specified, if at least one narrow result
         qr_mismatch: conversions from query quantity to a different quantity that could not be further converted

        """
        # TODO: port qdb functionality: detect unity conversions; quell biogenic co2; integrate convert()
        # first- check to see if the query quantity can be converted to the ref quantity directly:
        if qq is not None and rq is not None:
            if qq.has_property('UnitConversion'):
                try:
                    fac = qq.convert(to=rq.unit())
                    return fac
                except KeyError:
                    pass

        cfs = [cf for cf in self._archive.tm.factors_for_flowable(flowable, quantity=qq, context=context, **kwargs)]
        if len(cfs) == 0:
            raise NoFactorsFound('%s [%s] %s', (flowable, context, self))

        qr_results = []
        qr_mismatch = []
        qr_geog = []
        for cf in cfs:
            res = QuantityConversion(cf.query(locale))
            try:
                qr_results.append(self._ref_qty_conversion(rq, flowable, context, res, locale))
            except ConversionReferenceMismatch:
                qr_mismatch.append(res)

        if len(qr_results) > 1:
            qr_geog = [k for k in filter(lambda x: x[0].locale != locale, qr_results)]
            qr_results = [k for k in filter(lambda x: x[0].locale == locale, qr_results)]

        return qr_results, qr_geog, qr_mismatch

    def _get_flowable_info(self, flow, ref_quantity, context):
        """
        We need all three defined at the end. So if all given, we take em and look em up.
        Basically we take what we get for flow, unless we're missing ref_quantity or context.
        If flow is not entity_type='flow', we try to fetch a flow and if that fails we return what we were given.
        If we're given a flow and/or an external ref that looks up, then flowable, ref qty, and context are taken from
        it (unless they were provided)
        :param flow:
        :param ref_quantity:
        :param context:
        :return:
        """
        # skip the lookup if all terms are given
        if ref_quantity is None or context is None:
            if hasattr(flow, 'entity_type') and flow.entity_type == 'flow':
                f = flow
            else:
                f = self.get(flow)

            if f is None:
                flowable = flow
            else:
                flowable = f.flowable
                if ref_quantity is None:
                    ref_quantity = f.reference_entity
                if context is None:
                    context = f.context
        else:
            flowable = flow
        if ref_quantity is None:
            raise RefQuantityRequired
        rq = self.get_canonical(ref_quantity)
        cx = self._archive.tm[context]
        return flowable, rq, cx

    def quantity_conversions(self, flow, query_quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        """
        Return a comprehensive set of conversion results for the provided inputs.
        :param flow: a string that is synonymous with a flowable characterized by the query quantity
        :param query_quantity: convert to this quantty
        :param ref_quantity: [None] convert for 1 unit of this quantity
        :param context: [None] a string synonym for a context / "archetype"? (<== locale-specific?)
        :param locale: handled by CF; default 'GLO'
        :param kwargs:
         dist: CLookup distance (0=exact 1=subcompartments 2=parent compartment 3=all parents)
        :return: a 3-tuple of lists of QuantityConversion objects:
         [valid conversions],
         [geographic proxy conversions],
         [mismatched ref unit conversions]
        """
        flowable, rq, cx = self._get_flowable_info(flow, ref_quantity, context)
        qq = self.get_canonical(query_quantity)

        return self._quantity_conversions(flowable, rq, qq, context, locale=locale, **kwargs)

    def cf(self, flow, quantity, ref_quantity=None, context=None, locale='GLO', strategy=None, allow_proxy=True, **kwargs):
        """
        Reports the first / best result of a quantity conversion.  Returns a single QRResult interface
        (QuantityConversion result) that converts unit of the reference quantity into the query quantity for the given
        flowable, context, and locale (default 'GLO').
        If the locale is not found, this would be a great place to run a spatial best-match algorithm.

        :param flow:
        :param quantity:
        :param ref_quantity:
        :param context:
        :param locale:
        :param strategy: approach for resolving multiple-CF in dist>0.  ('highest' | 'lowest' | 'average' | ...? )
          None = return first result
        :param allow_proxy: [True] in the event of 0 exact results but >0 geographic proxies, return a geographic
          proxy without error.
        :param kwargs:
        :return: a QRResult object or interface
        """
        qr_results, qr_geog, qr_mismatch = self.quantity_conversions(flow, quantity,
                                                                     ref_quantity=ref_quantity, context=context,
                                                                     locale=locale, **kwargs)

        if len(qr_results) == 0 and len(qr_geog) > 0 and allow_proxy:
            qr_results += qr_geog

        if len(qr_results) > 1:
            # this is obviously punting
            if strategy is None:
                return qr_results[0].value
            elif strategy == 'highest':
                return max(v.value for v in qr_results)
            elif strategy == 'lowest':
                return min(v.value for v in qr_results)
            elif strategy == 'average':
                return sum(v.value for v in qr_results) / len(qr_results)
            else:
                raise ValueError('Unknown strategy %s' % strategy)
        elif len(qr_results) == 1:
            return qr_results[0]
        else:
            if len(qr_geog) > 0:
                return qr_geog[0]
            elif len(qr_mismatch) > 0:
                for k in qr_mismatch:
                    print('Flowable: %s\nfrom: %s\nto: %s' % (k.flowable, k.ref, ref_quantity))
                raise ConversionReferenceMismatch
            else:
                raise NoFactorsFound

    def quantity_relation(self, flowable, ref_quantity, query_quantity, context, locale='GLO', **kwargs):
        cf = self.cf(flowable, query_quantity, ref_quantity=ref_quantity, context=context, locale=locale, **kwargs)
        return cf.value

    def flat_cf(self, flow, quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        flowable, rq, cx = self._get_flowable_info(flow, ref_quantity, context)
        val = self.quantity_relation(flowable, rq, quantity, cx, locale=locale, **kwargs)
        return QRResult(flowable, rq, quantity, cx, locale, self.origin, val)

    def profile(self, flow, ref_quantity=None, context=None, complete=False, **kwargs):
        """
        Generate characterizations for the named flow or flowable.  The positional argument is first used to retrieve
        a flow, and if successful, the reference quantity and context are taken for that flow.  Otherwise, the
        positional argument is interpreted as a flowable synonym and used to generate CFs, optionally filtered by
        context.  In that case, if no ref quantity is given then the CFs are returned as-reported; if a ref quantity is
        given then a ref quantity conversion is attempted and the resulting QRResult objects are returned.

        This desperately needs tested.

        :param flow:
        :param ref_quantity: [None]
        :param context: [None]
        :param complete: [False] if True, report all results including errors and geographic proxies
        :param kwargs:
        :return:
        """
        flowable, rq, cx = self._get_flowable_info(flow, ref_quantity, context)
        qrr, qrg, qrm = self._quantity_conversions(flowable, None, ref_quantity=rq, context=cx, **kwargs)

        for r in qrr:
            yield r

        if complete:
            for r in qrg + qrm:
                yield r

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
            try:
                cf = self.cf(ref_q, x.flow.flowable, x.termination, locale=locale,
                             **kwargs)
                res.add_score(x.process, x, cf)
            except NoFactorsFound:
                res.add_cutoff(x)
            except ConversionReferenceMismatch:
                res.add_error(x)
            # TODO: lcia_result remodel
            # should we characterize the flows? to save on lookups? no, leave that to the client
        return res
