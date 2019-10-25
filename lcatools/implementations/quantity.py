"""
Each archive now has a TermManager which interprets query arguments as synonyms for canonical flows and contexts.  This
can also be upgraded to an LciaEngine, which extends the synonymization strategy to quantities as well
"""
from .basic import BasicImplementation
from ..characterizations import QRResult
from ..interfaces import QuantityInterface, NoFactorsFound, ConversionReferenceMismatch, FlowableMismatch, EntityNotFound
from ..contexts import NullContext
from ..lcia_results import LciaResult
from ..entity_refs import FlowInterface, convert, NoUnitConversionTable


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
    @classmethod
    def null(cls, flowable, rq, qq, context, locale, origin):
        qrr = QRResult(flowable, rq, qq, context, locale, origin, 0.0)
        return cls(qrr)

    def __init__(self, *args, query=None):
        self._query = query
        self._results = []
        for arg in args:
            self.add_result(arg)

    def invert(self):
        inv_qrr = type(self)(query=self.ref)
        for res in self._results[::-1]:
            inv_qrr.add_inverted_result(res)
        return inv_qrr

    def flatten(self, origin=None):
        if origin is None:
            origin = self._results[0].origin
        return QRResult(self.flowable, self.ref, self.query, self.context, self.locale, origin)

    def add_result(self, qrr):
        if isinstance(qrr, QRResult):
            if qrr.query is None or qrr.ref is None:
                raise ValueError('Both ref and query quantity must be defined')
            if len(self._results) > 0:
                #if self.flowable != qrr.flowable:  # don't think we care
                #    raise FlowableMismatch('%s != %s' % (self.flowable, qrr.flowable))
                if self.ref != qrr.query:
                    raise ConversionReferenceMismatch('%s != %s' % (self.ref, qrr.query))
            self._results.append(qrr)
        else:
            raise TypeError('Must supply a QRResult')

    @property
    def query(self):
        if len(self._results) == 0:
            return self._query
        return self._results[0].query

    @property
    def ref(self):
        if len(self._results) == 0:
            return self._query
        return self._results[-1].ref

    @property
    def flowable(self):
        return self._results[0].flowable

    @property
    def context(self):
        if self._results[0].context is None:
            return NullContext
        return self._results[0].context

    @property
    def locale(self):
        locs = []
        for res in self._results:
            if res.locale not in locs:
                locs.append(res.locale)
        return '/'.join(locs)

    @property
    def origin(self):
        if len(self._results) > 0:
            return self._results[0].origin
        return ''

    @property
    def results(self):
        for res in self._results:
            yield res

    def seen(self, q):
        for res in self._results:
            if q == res.ref:
                return True
        return False

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

    def __str__(self):
        conv = ' x '.join(['%g %s/%s' % (res.value, res.query.unit(), res.ref.unit()) for res in self._results])
        return '%s [context %s]: 1 %s x %s [%s] (%s)' % (self.flowable, self.context,
                                                         self.ref.unit(), conv, self._results[-1].locale,
                                                         self._results[-1].origin)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__str__())

    # TODO: add serialization, other outputs


class QuantityConversionError(object):
    def __init__(self, qrresult, ref_quantity):
        self._qrr = qrresult
        self._ref = ref_quantity

    @property
    def ref(self):
        return self._ref

    @property
    def query(self):
        return self._qrr.query

    @property
    def fail(self):
        return self._qrr.ref

    @property
    def flowable(self):
        return self._qrr.flowable

    @property
    def context(self):
        return self._qrr.context

    @property
    def value(self):
        return self._qrr.value

    def __repr__(self):
        return '%s(%s/%s [%s] %g %s/%s || %s)' % (self.__class__.__name__, self.flowable, self.context,
                                                  self._qrr.origin,
                                                  self._qrr.value, self.query.unit(), self._qrr.ref.unit(), self.ref.link)


class NoConversion(Exception):
    pass


def try_convert(flowable, rq, qq, context, locale):

    if not qq.is_lcia_method():
        try:
            fac = convert(qq, from_unit=rq.unit())
            return QRResult(flowable, rq, qq, context, locale, qq.origin, fac)
        except (KeyError, NoUnitConversionTable):
            pass

    if not rq.is_lcia_method():
        try:
            fac = convert(rq, to=qq.unit())
            return QRResult(flowable, rq, qq, context, locale, rq.origin, fac)
        except (KeyError, NoUnitConversionTable):
            pass
    raise NoConversion


class QuantityImplementation(BasicImplementation, QuantityInterface):
    """
    Uses the archive's term manager to index cfs, by way of the canonical quantities
    """
    def quantities(self, **kwargs):
        for q_e in self._archive.search('quantity', **kwargs):
            yield q_e

    def get_canonical(self, quantity, **kwargs):
        """
        Retrieve a canonical quantity from a qdb; else raises EntityNotFound
        :param quantity: external_id of quantity
        :return: quantity entity
        """
        try:
            return self._archive.tm.get_canonical(quantity)
        except EntityNotFound:
            if isinstance(quantity, str):
                q = self._archive.retrieve_or_fetch_entity(quantity)
                if q is None:
                    raise
                return self._archive.tm.get_canonical(q.external_ref)
            elif hasattr(quantity, 'entity_type') and quantity.entity_type == 'quantity':
                self._archive.add_entity_and_children(quantity)
                return self._archive.tm.get_canonical(quantity)
            else:
                raise

    def factors(self, quantity, flowable=None, context=None, dist=0):
        q = self.get_canonical(quantity)
        for cf in self._archive.tm.factors_for_quantity(q, flowable=flowable, context=context, dist=dist):
            yield cf

    def characterize(self, flowable, ref_quantity, query_quantity, value, context=None, location='GLO', origin=None,
                     **kwargs):
        """
        We gotta be able to do this
        :param flowable: string
        :param ref_quantity: string
        :param query_quantity: string
        :param value: float
        :param context: string
        :param location: string
        :param origin: [self.origin]
        :param kwargs: overwrite=False
        :return:
        """
        rq = self.get_canonical(ref_quantity)
        qq = self.get_canonical(query_quantity)
        if qq.origin != self._archive.ref:
            self._archive.add_entity_and_children(qq)  # catches EntityExists
        if origin is None:
            origin = self.origin
        return self._archive.tm.add_characterization(flowable, rq, qq, value, context=context, location=location,
                                                     origin=origin, **kwargs)

    def _ref_qty_conversion(self, ref_quantity, flowable, compartment, res, locale):
        """
        Transforms a CF into a quantity conversion with the proper ref quantity. Does it recursively! watch with terror.
        :param ref_quantity:
        :param flowable:
        :param compartment:
        :param res: a QRResult
        :param locale:
        :return:
        """
        if ref_quantity is None:
            raise ConversionReferenceMismatch('Cannot convert to None')
        found_ref = self.get_canonical(res.ref)  # this is still necessary because CFs are stored with native ref quantities
        if found_ref != ref_quantity:
            # zero look for conversions
            try:
                qr_result = try_convert(flowable, ref_quantity, found_ref, compartment, locale)
                return res.add_result(qr_result)
            except NoConversion:
                pass

            # first look for forward matches
            cfs_fwd = [cf for cf in self._archive.tm.factors_for_flowable(flowable, quantity=found_ref,
                                                                          context=compartment, dist=3)
                       if not res.seen(cf.ref_quantity)]
            for cf in cfs_fwd:
                new_res = QuantityConversion(*res.results)
                new_res.add_result(cf.query(locale))
                try:
                    return self._ref_qty_conversion(ref_quantity, flowable, compartment, new_res, locale)
                except ConversionReferenceMismatch:
                    continue

            # then look for reverse matches
            cfs_rev = [cf for cf in self._archive.tm.factors_for_flowable(flowable, quantity=ref_quantity,
                                                                          context=compartment, dist=3)
                       if not res.seen(cf.quantity)]
            for cf in cfs_rev:
                if self.get_canonical(cf.ref_quantity) != found_ref:
                    continue
                new_res = QuantityConversion(*res.results)
                new_res.add_inverted_result(cf.query(locale))
                try:
                    return self._ref_qty_conversion(ref_quantity, flowable, compartment, new_res, locale)
                except ConversionReferenceMismatch:
                    continue

            raise ConversionReferenceMismatch('Flow %s\nfrom %s\nto %s' % (flowable,
                                                                           res.query,
                                                                           ref_quantity))
        return res

    def _quantity_engine(self, fb, rq, qq, cx, locale='GLO',
                         **kwargs):
        """
        This is the main "comprehensive" engine for performing characterizations.

        :param fb: a string that is synonymous with a known flowable.
        :param rq: a canonical ref_quantity or None
        :param qq: a canonical query_quantity or None
        :param cx: a known context
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
            try:
                qr_result = try_convert(fb, rq, qq, cx, locale)
                return [qr_result], [], []
            except NoConversion:
                pass

        qr_results = []
        qr_mismatch = []
        qr_geog = []

        for cf in self._archive.tm.factors_for_flowable(fb, quantity=qq, context=cx, **kwargs):
            res = QuantityConversion(cf.query(locale))
            try:
                qr_results.append(self._ref_qty_conversion(rq, fb, cx, res, locale))
            except ConversionReferenceMismatch:
                qr_mismatch.append(QuantityConversionError(res, rq))

        for cf in self._archive.tm.factors_for_flowable(fb, quantity=rq, context=cx, **kwargs):
            res = QuantityConversion(cf.query(locale))
            try:
                qr_results.append(self._ref_qty_conversion(qq, fb, cx, res, locale).invert())
            except ConversionReferenceMismatch:
                pass  # qr_mismatch.append(res.invert())  We shouldn't be surprised that there is no reverse conversion

        # TODO: should we try a last-ditch effort to find factors for flowables using target unit conversion quantities?
        # motivation: cf is in t --> Count; query is between kg and Count; t also has

        if len(qr_results + qr_geog + qr_mismatch) == 0:
            raise NoFactorsFound

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
        If a context query is specified and no canonical context is found, NullContext is returned (matches only
        NullContext characterizations).
        Otherwise, None is returned (matches everything).
        :param flow:
        :param ref_quantity:
        :param context:
        :return: flowable, canonical ref_quantity, canonical context or None
        """
        if isinstance(flow, str):
            f = self.get(flow)  # assume it's a ref

            if f is not None:  # lookup succeeded
                flow = f

        if isinstance(flow, FlowInterface):
            flowable = flow.link
            if ref_quantity is None:
                ref_quantity = flow.reference_entity
            if context is None:
                context = flow.context
        else:
            if hasattr(flow, 'link'):
                flowable = flow.link
            else:
                flowable = str(flow)

        if ref_quantity is None:
            raise RefQuantityRequired

        rq = self.get_canonical(ref_quantity)
        cx = self._archive.tm[context]  # will fall back to find_matching_context if tm is an LciaEngine
        if cx is None and context is not None:
            cx = NullContext
        return flowable, rq, cx

    def quantity_conversions(self, flow, query_quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        """
        Return a comprehensive set of conversion results for the provided inputs.  This method catches errors and
        returns a null result if no factors are found.

        This function is a wrapper to handle inputs.

        :param flow: a string that is synonymous with a flowable characterized by the query quantity
        :param query_quantity: convert to this quantity
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
        if query_quantity is None:
            qq = None
        else:
            qq = self.get_canonical(query_quantity)
            if qq is None:
                raise EntityNotFound(qq)

        if qq == rq:  # is?
            return [QRResult(flowable, rq, qq, context, locale, qq.origin, 1.0)], [], []

        try:
            return self._quantity_engine(flowable, rq, qq, cx, locale=locale, **kwargs)
        except NoFactorsFound:
            if qq is None:
                return [], [], []
            else:
                return [QuantityConversion.null(flowable, rq, qq, cx, locale, self.origin)], [], []

    def _quantity_relation(self, fb, rq, qq, cx, locale='GLO',
                           strategy=None, allow_proxy=True, **kwargs):
        """
        Reports the first / best result of a quantity conversion.  Returns a single QRResult interface
        (QuantityConversion result) that converts unit of the reference quantity into the query quantity for the given
        fb, context, and locale (default 'GLO').
        If the locale is not found, this would be a great place to run a spatial best-match algorithm.

        :param fb:
        :param rq:
        :param qq:
        :param cx:
        :param locale:
        :param strategy: approach for resolving multiple-CF in dist>0.  ('highest' | 'lowest' | 'average' | ...? )
          None = return first result
        :param allow_proxy: [True] in the event of 0 exact results but >0 geographic proxies, return a geographic
          proxy without error.
        :param kwargs:
        :return: a QRResult object or interface
        """
        try:
            qr_results, qr_geog, qr_mismatch = self._quantity_engine(fb, rq, qq, cx,
                                                                     locale=locale, **kwargs)
        except NoFactorsFound:
            qr_results, qr_geog, qr_mismatch = [QuantityConversion.null(fb, rq, qq, cx, locale, self.origin)], [], []

        if len(qr_results) == 0 and len(qr_geog) > 0:
            if allow_proxy:
                qr_results += qr_geog
            else:
                locs = ', '.join([qr.locale for qr in qr_geog])
                print('allow_proxy to show values with locales: %s' % locs)

        if len(qr_results) > 1:
            # this is obviously punting
            if strategy is None or strategy == 'first':
                best = qr_results[0]
            elif strategy == 'highest':
                val = max(v.value for v in qr_results)
                best = next(v for v in qr_results if v.value == val)
            elif strategy == 'lowest':
                val = min(v.value for v in qr_results)
                best = next(v for v in qr_results if v.value == val)
            # elif strategy == 'average':
            #     return sum(v.value for v in qr_results) / len(qr_results)
            else:
                raise ValueError('Unknown strategy %s' % strategy)
        elif len(qr_results) == 1:
            best = qr_results[0]
        else:
            if len(qr_mismatch) == 0:
                raise NoFactorsFound

            best = None
        return best, qr_mismatch

    def quantity_relation(self, flowable, ref_quantity, query_quantity, context, locale='GLO',
                          strategy=None, allow_proxy=True, **kwargs):

        fb, rq, cx = self._get_flowable_info(flowable, ref_quantity, context)  # call only for exception handling
        qq = self.get_canonical(query_quantity)
        result, mismatch = self._quantity_relation(fb, rq, qq, cx, locale=locale,
                                                   strategy=strategy, allow_proxy=allow_proxy, **kwargs)
        if result is None:
            if len(mismatch) > 0:
                for k in mismatch:
                    print('Conversion failure: Flowable: %s\nfrom: %s %s\nto: %s %s' % (fb, k.fail, k.fail.link,
                                                                                        rq, rq.link))
                raise ConversionReferenceMismatch
            else:
                raise AssertionError('Something went wrong')
        return result

    def cf(self, flow, quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        """
        :param flow:
        :param quantity:
        :param ref_quantity: [None] taken from flow.reference_entity if flow is entity or locally known external_ref
        :param context: [None] taken from flow.reference_entity if flow is entity or locally known external_ref
        :param locale:
        :param kwargs: allow_proxy [False], strategy ['first'] -> passed to quantity_relation
        :return: the value of the QRResult found by the quantity_relation
        """
        qr = self.quantity_relation(flow, ref_quantity, quantity, context=context, locale=locale, **kwargs)
        return qr.value

    def flat_qr(self, flow, quantity, ref_quantity=None, context=None, locale='GLO', **kwargs):
        val = self.quantity_relation(flow, ref_quantity, quantity, context, locale=locale, **kwargs)
        return val.flatten(origin=self.origin)

    def profile(self, flow, ref_quantity=None, context=None, complete=False, **kwargs):
        """
        Generate characterizations for the named flow or flowable.  The positional argument is first used to retrieve
        a flow, and if successful, the reference quantity and context are taken for that flow.  Otherwise, the
        positional argument is interpreted as a flowable synonym and used to generate CFs, optionally filtered by
        context.  In that case, if no ref quantity is given then the CFs are returned as-reported; if a ref quantity is
        given then a ref quantity conversion is attempted and the resulting QRResult objects are returned.

        This whole interface desperately needs testing.

        :param flow:
        :param ref_quantity: [None]
        :param context: [None]
        :param complete: [False] if True, report all results including errors and geographic proxies
        :param kwargs:
        :return:
        """
        qrr, qrg, qrm = self.quantity_conversions(flow, None, ref_quantity, context, **kwargs)

        for r in qrr:
            yield r

        if complete:
            for r in qrg + qrm:
                yield r

    def do_lcia(self, quantity, inventory, locale='GLO', group=None, dist=2, **kwargs):
        """
        Successively implement the quantity relation over an iterable of exchanges.

        man, WHAT is the qdb DOING with all those LOC? (ans: seemingly a lot)

        :param quantity:
        :param inventory: An iterable of exchange-like entries, having flow, direction, value, termination.  Currently
          also uses process.external_ref for hashing purposes, but that could conceivably be abandoned.
        :param locale: ['GLO']
        :param group: How to group scores.  Should be a lambda that operates on inventory items. Default x -> x.process
        :param dist: [2] controls how strictly to interpret exchange context.
          0 - exact context matches only;
          1 - match child contexts (code default)
          2 - match parent contexts [this default]
          3 - match any ancestor context, including Null
         exchange context; 3 - match any ancestor of exch
        :param kwargs:
        :return:
        """
        q = self.get_canonical(quantity)
        res = LciaResult(q)
        if group is None:
            group = lambda _x: _x.process
        for x in inventory:
            xt = x.type
            if xt in ('cutoff', 'reference'):
                res.add_cutoff(x)
                continue
            elif xt in ('node', 'self'):
                continue
            try:
                ref_q = self.get_canonical(x.flow.reference_entity)
            except EntityNotFound:
                ref_q = self._archive.tm.add_quantity(x.flow.reference_entity)
            try:
                cx = self._archive.tm[x.termination]
                qr, mis = self._quantity_relation(x.flow.name, ref_q, q, cx, locale=locale,
                                                  dist=dist, **kwargs)
                if qr is None and len(mis) > 0:
                    res.add_error(x, mis[0])
                    if len(mis) > 1:
                        print('omitting mismatches:')
                        for k in mis[1:]:
                            print(k)
                elif qr.value == 0:
                    res.add_zero(x, qr)
                else:
                    res.add_score(group(x), x, qr)
            except NoFactorsFound:
                res.add_cutoff(x)
        return res
