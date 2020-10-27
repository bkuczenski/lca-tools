"""
Marshmallow entities to be used in Antelope V2 hosting

The following types are returned by the Antelope V2 API:

R0: Result / .  Used to report entity properties and synonyms (str), and the results of relation computations (numeric)

R1: Entity.  Used to describe references to quantities, flows, processes, [contexts, fragments?]
R1a: abbreviated entity. just ID and attributes from search query, if relevant. not sure how to tackle this. Probably
 better to just use pagination instead of abbreviation.

R2: Exchange
R2v: Exchange with value information

R3: Characterization
R3v: Characterization with value information

R5: Lcia Results

R6: Flow Terminations (incl. fragment flows)

"""

from marshmallow_jsonapi import Schema, fields
from antelope import comp_dir


# R0
class ResultSchema(Schema):
    id = fields.Str(attribute='query')  # the ID of the result is the query that generated it

    class Meta:
        type_ = 'result'
        strict = True


class NumericResultSchema(ResultSchema):
    result = fields.List(fields.Number())


class StringResultSchema(ResultSchema):
    result = fields.List(fields.Str())


class LciaResultSchema(ResultSchema):
    lcia_method = fields.Relationship(
        related_url='/{ref_link}',
        related_url_kwargs={'ref_link': '<quantity.link>'},
        dump_to='lciaMethod'
    )

    scenario = fields.Str()
    components = fields.Function(lambda x: x.serialize_components(detailed=False))
    total = fields.Function(lambda x: x.total())

    class Meta:
        type_ = 'lcia_result'


class DetailedLciaResultSchema(LciaResultSchema):
    components = fields.Function(lambda x: x.serialize_components(detailed=True))


class ReferenceExchangeSchema(Schema):

    id = fields.Function(lambda x: '%s/reference/%s' % (x.process.link, x.flow.external_ref))
    flow = fields.Relationship(
        related_url='/{flow_link}',
        related_url_kwargs={'flow_link': '<flow.link>'},
        id_field='external_ref'
    )
    direction = fields.Str()

    parent = fields.Relationship(
        related_url='/{process_link}',
        related_url_kwargs={'process_link': '<process.link>'},
        id_field = 'external_ref'
    )

    '''#for debug
    def get_attribute(self, attr, obj, default):
        print('Rx g_a | %s, %s, %s' % (attr, obj, default))
        k = super(ReferenceExchangeSchema, self).get_attribute(attr, obj, default=default)
        print(k)
        return k
    '''

    class Meta:
        type_ = 'reference_exchange'
        strict = True


class ExchangeSchema(Schema):
    id = fields.Function(lambda x: '%s(%s, %s, %s)' % (x.process.external_ref,
                                                       x.flow.external_ref,
                                                       x.direction,
                                                       x.termination))
    process = fields.Relationship(
        related_url='/{process_link}',
        related_url_kwargs={'process_link': '<process.link'},
        id_field = 'external_ref'
    )
    flow = fields.Relationship(
        related_url='/{flow_link}',
        related_url_kwargs={'flow_link': '<flow.link>'},
        id_field = 'external_ref'
    )
    direction = fields.Str()
    termination = fields.Str()

    class Meta:
        type_ = 'exchange'
        strict = True


class ExchangeValueSchema(ExchangeSchema):
    value = fields.Number()

    class Meta:
        type_ = 'exchange_value'
        strict = True


class CharacterizationResult(Schema):
    id = fields.Function(lambda x: '/'.join(()))
    locale = fields.Str()
    value = fields.Number()



class CharacterizationSchema(Schema):

    id = fields.Function(lambda x: '/'.join((x.quantity.link,
                                    x.flowable,
                                    str(x.context),
                                    x.ref_quantity.external_ref)))

    flowable = fields.Function(lambda x: x.flowable)
    quantity = fields.Function(lambda x: x.quantity.name)
    ref_quantity = fields.Function(lambda x: x.ref_quantity.name)
    context = fields.Function(lambda x: x.context)

    # values = fields.Relationship


    #locations = fields.Function(lambda x: {k: x[k] for k in x.locations()})
    '''
    
    flow = fields.Relationship(
        related_url='/{flow_link}',
        related_url_kwargs={'flow_link': '<flow.link>'}
    )
    context = fields.List(fields.Str())
    quantity = fields.Relationship(
        related_url='/{quantity_link}',
        related_url_kwargs={'quantity_link': '<quantity.link>'}
    )
    value = fields.Dict(keys=fields.Str(), values=fields.Decimal(), attribute='_locations')
    '''
    class Meta:
        type_ = 'characterization'
        strict = True


class EntitySchema(Schema):  # still abstract, need Meta.type_
    id = fields.Str(attribute='external_ref')
    origin = fields.Str()
    entity_type = fields.Str(dump_to='entity_ype')
    link = fields.Str()

    name = fields.Str()
    comment = fields.Str()



    class Meta:
        strict = True
        self_url = '/{ref_link}'
        self_url_kwargs = {'ref_link': '<link>'}


# R1 - three different types
class QuantitySchema(EntitySchema):
    reference_entity = fields.Str(attribute='unit', dump_to='referenceUnit')

    class Meta:
        type_ = 'quantity'
        self_url_many = '/quantities/'


class FlowSchema(EntitySchema):

    casNumber = fields.Str(attribute='CasNumber')

    reference_entity = fields.Relationship(
        related_url='/{ref_link}',
        related_url_kwargs={'ref_link': '<reference_entity.link>'},
        id_field='external_ref'
    )

    profile = fields.Relationship(
        related_url='/{ref_link}/profile',
        related_url_kwargs={'ref_link': '<link>'},
        many=True, include_resource_linkage=False,  # for now cannot include because FlowRef.profile() is not an attr
        type_='characterization',
        id_field='quantity.external_ref'
    )

    class Meta:
        type_ = 'flow'
        self_url_many = '/flows/'


class ProcessSchema(EntitySchema):
    spatial_scope = fields.Str(attribute='SpatialScope')
    temporal_scope = fields.Str(attribute='TemporalScope')
    Classification = fields.List(fields.Str())

    reference_entity = fields.Relationship(
        self_url='/{ref_link}/reference',
        self_url_kwargs={'ref_link': '<link>'},
        related_url='/{ref_link}',
        related_url_kwargs={'ref_link': '<link>'},
        many=True, include_resource_linkage=True,
        type_='reference_exchange',
        id_field='link',
        schema=ReferenceExchangeSchema
    )

    exchanges = fields.Relationship(
        self_url='/{origin}/{id}/exchanges',
        self_url_kwargs={'id': '<external_ref>',
                         'origin': '<origin>'},
    )

    class Meta:
        type_ = 'process'
        self_url_many = '/processes/'


class FragmentSchema(EntitySchema):

    stage_name = fields.Str(attribute='StageName')

    flow = fields.Relationship(
        related_url='/{ref_link}',
        related_url_kwargs={'ref_link': '<link>'},
        include_resource_linkage=True,
        type_= 'flow',
        id_field='external_ref'
    )

    direction = fields.Str()

    class Meta:
        type_ = 'fragment'
        self_url_many = '/fragments/'


def _get_term_type(ft):
    if ft.is_null:
        return comp_dir(ft.direction)
    return ft.term_node.entity_type


def _get_term_name(ft):
    if ft.is_null:
        return None
    return ft.term_node.name


class FlowTermination(Schema):
    id = fields.Function(lambda x: (x.term_flow.external_ref, x.direction, x.term_node.external_ref))
    node_type = fields.Function(_get_term_type)
    node_name = fields.Function(_get_term_name)
    term_flow = fields.Function(lambda x: x.term_flow.link)
    direction = fields.Str()
    target = fields.Function(lambda x: x.term_node.link)

    class Meta:
        type_ = 'LinkTarget'
        strict = False


def _get_parent_id(ff):
    if ff.fragment.reference_entity is None:
        return None
    return ff.fragment.reference_entity.external_ref


class FragmentFlow(Schema):
    id = fields.Function(lambda x: x.fragment.external_ref)
    parent_fragment_flow = fields.Function(_get_parent_id)
    stage_name = fields.Function(lambda x: x.fragment['StageName'])
    flow = fields.Function(lambda x: x.fragment.flow.name)
    direction = fields.Function(lambda x: x.fragment.direction)
    magnitude = fields.Function(lambda x: {'amount': x.magnitude, 'unit': x.ref_unit})
    node_weight = fields.Number()
    is_background = fields.Function(lambda x: x.fragment.is_background)
    is_conserved = fields.Bool()

    term = fields.Function(
        lambda x: FlowTermination().dump(x.term)
    )
    class Meta:
        type_ = 'FragmentFlow'
        strict = True

