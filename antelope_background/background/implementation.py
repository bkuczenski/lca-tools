from lcatools.implementations import BackgroundImplementation
from lcatools.interfaces import ExteriorFlow
from lcatools.exchanges import ExchangeValue

from .flat_background import FlatBackground


class InvalidRefFlow(Exception):
    pass


class TarjanBackgroundImplementation(BackgroundImplementation):
    """
    This is the class that does the background interfacing work for partially-ordered databases.  The plumbing is
    pretty complicated so it deserves some explanation.

    The MRO for this is:
    (antelope_background.background.implementation.TarjanBackgroundImplementation,
    lcatools.implementations.background.BackgroundImplementation,
    lcatools.implementations.basic.BasicImplementation,
    lcatools.interfaces.ibackground.BackgroundInterface,
    lcatools.interfaces.abstract_query.AbstractQuery,
    object)

    So ultimately this is a query object that implements the background interface.

    The __init__ comes from the BasicImplementation, which requires an _archive as first argument.  In the default
    BackgroundImplementation, this _archive is used to generally access all entities.  However, in the Tarjan background
    the FlatBackground provides all necessary information-- which boils down to external_refs that can be looked up
    using a separate implementation.

    The BackgroundImplementation subclasses BasicImplementation and adds an _index attribute.  This index is used for
    creating the flat background and also for accessing contexts when generating elementary exchanges.

    The necessary conditions to CREATE a flat Tarjan Background are: a valid [invertible] database with complete working
    index and inventory implementations. This flat background then gets serialized using a numpy [matlab] format, along
    with a separate index file as json.

    The necessary conditions to RESTORE a flat Tarjan Background are the serialization created above, and an index
    implementation for retrieving resources and contexts.

    Thus the two routes

    """

    @classmethod
    def from_file(cls, res, savefile, **kwargs):
        """

        :param res: data resource providing index information
        :param savefile: serialized flat background
        :return:
        """
        im = cls(res)
        im._index = res.make_interface('index')
        im._flat = FlatBackground.from_file(savefile, **kwargs)
        return im

    """
    basic implementation overrides
    """
    def __getitem__(self, item):
        return self._index.get(item)

    def _fetch(self, external_ref, **kwargs):
        return self._index.get(external_ref, **kwargs)

    """
    background implementation
    """
    def __init__(self, *args, **kwargs):
        super(TarjanBackgroundImplementation, self).__init__(*args, **kwargs)

        self._flat = None

    def setup_bm(self, index=None):
        if self._index is None:
            super(TarjanBackgroundImplementation, self).setup_bm(index)
            if hasattr(self._archive, 'create_flat_background'):
                self._flat = self._archive.create_flat_background(index)
            else:
                self._flat = FlatBackground.from_index(index)

    def _check_ref(self, arg, opt_arg):
        """
        Do argument handling.  Valid argument patterns:
        _check_ref(exchange) -> require is_reference, use process_ref and flow_ref
        _check_ref(process, <anything>) -> obtain process.reference(<anything>) and fall back to above
        :param arg:
        :param opt_arg:
        :return:
        """
        try:
            if isinstance(arg, str):
                process_ref = arg
                flow_ref = self.get(process_ref).reference(opt_arg).flow.external_ref
            elif hasattr(arg, 'entity_type'):
                if arg.entity_type == 'process':
                    process_ref = arg.external_ref
                    flow_ref = arg.reference(opt_arg)
                elif arg.entity_type == 'exchange':
                    if not arg.is_reference:
                        raise ValueError('Exchange argument must be reference exchange')
                    process_ref = arg.process.external_ref
                    flow_ref = arg.flow.external_ref
                else:
                    raise TypeError('Cannot handle entity type %s (%s)' % (arg, arg.entity_type))
            else:
                raise TypeError('Unable to interpret input arg %s' % arg)
            return process_ref, flow_ref
        except StopIteration:
            raise InvalidRefFlow('process: %s\nref flow: %s' % (arg, opt_arg))

    '''
    def _product_flow_from_term_ref(self, tr):
        p = self[tr.term_ref]
        f = self[tr.flow_ref]
        return ProductFlow(self.origin, f, tr.direction, p, tr.scc_id)
    '''

    def _exchange_from_term_ref(self, tr):
        p = self[tr.term_ref]
        return p.reference(tr.flow_ref)

    def foreground_flows(self, search=None, **kwargs):
        for fg in self._flat.fg:
            yield self._exchange_from_term_ref(fg)

    def background_flows(self, search=None, **kwargs):
        for bg in self._flat.bg:
            yield self._exchange_from_term_ref(bg)

    def exterior_flows(self, search=None, **kwargs):
        for ex in self._flat.ex:
            c = ex.term_ref
            f = self[ex.flow_ref]
            yield ExteriorFlow(self.origin, f, ex.direction, c)

    def is_in_scc(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        return self._flat.is_in_scc(process, ref_flow)

    def is_in_background(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        return self._flat.is_in_background(process, ref_flow)

    def foreground(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        for x in self._flat.foreground(process, ref_flow):
            yield ExchangeValue(self[x.process], self[x.flow], x.direction, termination=x.term, value=x.value)

    def _direct_exchanges(self, node, x_iter, context=False):
        for x in x_iter:
            if context is True:
                term = self._index.get_context(x.term)
            else:
                term = x.term
            yield ExchangeValue(node, self[x.flow], x.direction, termination=term, value=x.value)

    def consumers(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        for x in self._flat.consumers(process, ref_flow):
            yield self._exchange_from_term_ref(x)

    def dependencies(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.dependencies(process, ref_flow)):
            yield x

    def emissions(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.emissions(process, ref_flow)):
            yield x

    def ad(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.ad(process, ref_flow)):
            yield x

    def bf(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.bf(process, ref_flow)):
            yield x

    def lci(self, process, ref_flow=None, **kwargs):
        process, ref_flow = self._check_ref(process, ref_flow)
        node = self[process]
        for x in self._direct_exchanges(node, self._flat.lci(process, ref_flow, **kwargs)):
            yield x
