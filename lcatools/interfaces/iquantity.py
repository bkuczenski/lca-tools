"""
It's taken me a long time to figure out how the quantity interface should work, but it is finally coming together.

There are two quantitative relations common to all LCA implementations, which I have named the exchange relation and
the quantity relation.  The first relates magnitudes of flows to one another through a particular process or activity,
and includes essentially all inventory modeling.  The second relates magnitudes of quantities to one another through
a particular flow in a particular context / locale, and includes both quantity conversion (e.g. kg to MJ) and LCIA.

The quantity interface is meant to capture that entire second group. The core of it is the 'quantity relation', which
is a function that maps (flowable, reference quantity, query quantity, context, locale) => a real number

Of course, the quantity_relation can be messy "under the hood": the key question in designing the interface is how to
expose "fuzzy" matches to the user: matches that require reference quantity conversion, use of a proxy locale, or when
there are multiple matches (i.e. inexact or proxy flowable or context)

There are also multiple forms for the response to take: pure numeric (or numeric with uncertainty in some fantasy
future world), as a characterization factor or QuantityConversion object, as an LCIA Result object.

The proposed model for the quantity interface is as follows:

 quantity_relation (flowable, ref quantity, query quantity, context, locale='GLO', strategy='first')
     => number.  quantity_relation(*) = cf(*).value
        amount of the query quantity that corresponds to a unit of the ref quantity
 cf (flow[able], query quantity, ref quantity=None, context=None, locale='GLO', strategy='first')
     =>  single result, chosen by named strategy.  cf(*) = quantity_conversions(*)[0][0]
 quantity_conversions (flow[able], query quantity, ref quantity=None, context=None, locale='GLO') "comprehensive"
     => [list of valid results (flowable / context proxies),
         list of geographic proxies,
         list of mismatched results (ref quantity conversion error]
 do_lcia(query quantity, exchanges iterable, locale='GLO')
     => LciaResult object, with a detail for each exchange item.  locale is used if it is more specific than the

For cf and QuantityConversion, 'flowable' could be replaced with a flow entity, from which would be taken ref quantity
and context.  If ref quantity is not supplied, nor is flow an entity, QuantityConversion will return all valid results
and cf() will fail.  For the Quantity Relation, all 4 core arguments are explicitly required (locale still optional).

Additional methods:

 profile (flow[able], ref_quantity=None, context=None)
     => sequence of cfs from quantity conversions for all characterized quantities for the flow[able]
 factors (quantity, flowable=None, ref_quantity=None, context=None)
     => sequence of characterizations for the named quantity, optionally filtered by flowable and/or compartment,
        optionally converted to the named reference
"""

from .abstract_query import AbstractQuery


class QuantityRequired(Exception):
    pass


class NoFactorsFound(Exception):
    pass


class ConversionReferenceMismatch(Exception):
    pass


class FlowableMismatch(Exception):
    pass


_interface = 'quantity'


class QuantityInterface(AbstractQuery):
    """
    QuantityInterface
    """
    def synonyms(self, item, **kwargs):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment
        :param item:
        :return: list of strings
        """
        return self._perform_query(_interface, 'synonyms', QuantityRequired('Quantity interface required'), item,
                                   ** kwargs)

    def flowables(self, quantity=None, compartment=None, **kwargs):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment:
        :return: list of pairs: CAS number, name
        """
        return self._perform_query(_interface, 'flowables', QuantityRequired('Quantity interface required'),
                                   quantity=quantity, compartment=compartment, **kwargs)

    def compartments(self, quantity=None, flowable=None, **kwargs):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        return self._perform_query(_interface, 'compartments', QuantityRequired('Quantity interface required'),
                                   quantity=quantity, flowable=flowable, **kwargs)

    def profile(self, flow, **kwargs):
        """
        Generate characterizations for the named flow, with the reference quantity noted
        :param flow:
        :return:
        """
        return self._perform_query(_interface, 'profile', QuantityRequired('Must have quantity interface'),
                                   flow, **kwargs)

    def get_canonical(self, quantity, **kwargs):
        """
        Retrieve a canonical quantity based on a synonym or other distinguishable term.  In future this should be
        expanded to flows and contexts.
        :param quantity: external_id of quantity
        :return: quantity CatalogRef
        """
        return self.make_ref(self._perform_query(_interface, 'get_canonical',
                                                 QuantityRequired('Quantity interface required'),
                                                 quantity, **kwargs))

    def characterize(self, flowable, ref_quantity, query_quantity, value, context=None, location='GLO', **kwargs):
        """
        Add Characterization data for a flowable, reporting the amount of a query quantity that is equal to a unit
        amount of a reference quantity, for a given context and location
        :param flowable: string or flow external ref
        :param ref_quantity: string or external ref
        :param query_quantity: string or external ref
        :param value: float or dict of locations to floats
        :param context: string
        :param location: string, ignored if value is dict
        :param kwargs: overwrite=False, origin=query_quantity.origin, others?
        :return:
        """
        return self._perform_query(_interface, 'characterize', QuantityRequired,
                                   flowable, ref_quantity, query_quantity, value,
                                   context=context, location=location, **kwargs)

    def factors(self, quantity, flowable=None, compartment=None, **kwargs):
        """
        Return characterization factors for the given quantity, subject to optional flowable and compartment
        filter constraints. This is ill-defined because the reference unit is not explicitly reported in current
        serialization for characterizations (it is implicit in the flow)-- but it can be added to a web service layer.
        :param quantity:
        :param flowable:
        :param compartment:
        :return: a generator of Characterizations
        """
        return self._perform_query(_interface, 'factors', QuantityRequired('Quantity interface required'),
                                   quantity, flowable=flowable, compartment=compartment, **kwargs)

    def cf(self, flow, quantity, ref_quantity=None, context=None, locale='GLO', strategy=None, **kwargs):
        """
        Determine a characterization factor value for a given quantity by consulting the flow directly.  This will
        fall back to a Qdb lookup if the flow's originating resource cannot answer the question.
        :param flow: flow entity ref or flowable string
        :param quantity:
        :param ref_quantity: [None] if flow is entity, flow.reference_entity is used unless specified
        :param context: [None] if flow is entity, flow.context is used
        :param locale: ['GLO']
        :param strategy: [None] TBD by implementation
        :param kwargs:
        :return: a float
        """
        return self._perform_query(_interface, 'cf', QuantityRequired('Quantity interface required'), flow, quantity,
                                   ref_quantity=ref_quantity, context=context, locale=locale, strategy=strategy,
                                   **kwargs)

    def quantity_relation(self, flowable, ref_quantity, query_quantity, context, locale='GLO', **kwargs):
        """
        Return a single number that converts the a unit of the reference quantity into the query quantity for the
        given flowable, compartment, and locale (default 'GLO').  If no locale is found, this would be a great place
        to run a spatial best-match algorithm.
        :param flowable:
        :param ref_quantity:
        :param query_quantity:
        :param context:
        :param locale: ['GLO']
        :return:
        """
        return self._perform_query(_interface, 'quantity_relation', QuantityRequired('Quantity interface required'),
                                   flowable, ref_quantity, query_quantity, context, locale=locale, **kwargs)

    def do_lcia(self, quantity, inventory, locale='GLO', **kwargs):
        """
        Successively implement the quantity relation over an iterable of exchanges.

        :param quantity:
        :param inventory:
        :param locale:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'do_lcia', QuantityRequired('Quantity interface required'),
                                   quantity, inventory, locale=locale, **kwargs)
