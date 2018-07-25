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
    lcia_score = fields.Function(lambda x: x.serialize_result(), dump_to='lciaScore')
    total = fields.Number()


class ReferenceExchangeSchema(Schema):

    id = fields.Function(lambda x: '%s/reference/%s' % (x.process.link, x.flow.external_ref))
    flow = fields.Relationship(
        related_url='/{flow_link}',
        related_url_kwargs={'flow_link': '<flow.link>'}
    )
    direction = fields.Str()

    parent = fields.Relationship(
        related_url='/{process_link}',
        related_url_kwargs={'process_link': '<process.link>'}
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
        related_url_kwargs={'process_link': '<process.link'}
    )
    flow = fields.Relationship(
        related_url='/{flow_link}',
        related_url_kwargs={'flow_link': '<flow.link>'}
    )
    direction = fields.Str()
    termination = fields.Str()

    value = fields.Number()

    class Meta:
        type_ = 'exchange'
        strict = True


class CharacterizationSchema(Schema):
    id = fields.Function(lambda x: '%s(%s, %s)' % (x.quantity.external_ref,
                                                   x.flow.external_ref,
                                                   x.flow['Compartment'][-1]))
    flow = fields.Relationship(
        related_url='/{flow_link}',
        related_url_kwargs={'flow_link': '<flow.link>'}
    )
    context = fields.List(fields.Str(), attribute='flow.Compartment')
    quantity = fields.Relationship(
        related_url='/{quantity_link}',
        related_url_kwargs={'quantity_link': '<quantity.link>'}
    )
    value = fields.Dict(keys=fields.Str(), values=fields.Decimal(), attribute='_locations')

    class Meta:
        type_ = 'characterization'
        strict = True


class EntitySchema(Schema):
    id = fields.Str(attribute='external_ref', dump_to='externalId')
    uuid = fields.Str()
    origin = fields.Str()
    entity_type = fields.Str(dump_to='entityType')
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
        related_url_kwargs={'ref_link': '<reference_entity.link>'}
    )

    profile = fields.Relationship(
        self_url='/{ref_link}/profile',
        self_url_kwargs={'ref_link': '<link>'},
        many=True, include_resource_linkage=True,
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


class FlowTermination(Schema):
    id = fields.Function(lambda x: '(%s, %s, %s)' % (x.flow.external_ref, x.termination.direction, x.termination))

    flow = fields.Relationship(
        related_url='/{flow_link}',
        related_url_kwargs={'flow_link': '<flow.link>'}
    )
    direction = fields.Str(attribute='termination.direction')
    termination = fields.Str()

    parent = fields.Relationship(
        related_url='/{parent_link}',
        related_url_kwargs={'parent_link': '<parent.link>'}
    )

    class Meta:
        type_ = 'termination'
        strict = True
