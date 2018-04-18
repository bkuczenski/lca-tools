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
        related_url='/{origin}/{q_id}',
        related_url_kwargs={'origin': '<quantity.origin>',
                            'q_id': '<quantity.external_ref>'},
        dump_to='lciaMethod'
    )

    scenario = fields.Str()
    lcia_score = fields.Function(lambda x: x.serialize_result(), dump_to='lciaScore')
    total = fields.Number()


class ReferenceExchangeSchema(Schema):
    flow = fields.Relationship(
        related_url='/{origin}/{flow_id}',
        related_url_kwargs={'origin': '<flow.origin>',
                            'process_id': '<flow.external_ref'}
    )
    direction = fields.Str()

    parent = fields.Relationship(
        related_url='{origin}/{id}',
        related_url_kwargs={'origin': '<process.origin>',
                            'id': '<process.external_ref>'}
    )

    class Meta:
        type_ = 'reference_exchange',
        strict = True


class ExchangeSchema(Schema):
    id = fields.Function(lambda x: '(%s, %s, %s, %s)' % (x.process.external_ref,
                                                         x.flow.external_ref,
                                                         x.direction,
                                                         x.termination))
    process = fields.Relationship(
        related_url='/{origin}/{process_id}',
        related_url_kwargs={'origin': '<process.origin>',
                            'process_id': '<process.external_ref'}
    )
    flow = fields.Relationship(
        related_url='/{origin}/{flow_id}',
        related_url_kwargs={'origin': '<flow.origin>',
                            'process_id': '<flow.external_ref'}
    )
    direction = fields.Str()
    termination = fields.Str()

    value = fields.Number()

    class Meta:
        type_ = 'exchange'
        strict = True


class CharacterizationSchema(Schema):
    id = fields.Function(lambda x: '(%s, %s, %s)' % (x.flow.external_ref,
                                                     x.flow['Compartment'][-1],
                                                     x.quantity.external_ref))
    flow = fields.Relationship(
        related_url='/{origin}/{flow_id}',
        related_url_kwargs={'origin': '<flow.origin>',
                            'flow_id': '<flow.external_ref>'}
    )
    context = fields.List(fields.Str(), attribute='flow.Compartment')
    quantity = fields.Relationship(
        related_url='/{origin}/{quantity_id}',
        related_url_kwargs={'origin': '<quantity.origin>',
                            'quantity_id': '<quantity.external_ref>'}
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

    name = fields.Str()
    comment = fields.Str()


# R1 - three different types
class QuantitySchema(EntitySchema):
    reference_entity = fields.Str(attribute='unit', dump_to='referenceUnit')

    class Meta:
        type_ = 'quantity'
        strict = True
        self_url = '/{origin}/{id}'
        self_url_kwargs = {'id': '<externalId>', 'origin': '<origin>'}
        self_url_many = '/{origin}/quantities/'


class FlowSchema(EntitySchema):

    casNumber = fields.Str(attribute='CasNumber')

    reference_entity = fields.Relationship(
        related_url='/{origin}/{q_id}',
        related_url_kwargs={'origin': '<origin>',
                            'q_id': '<reference_entity.external_ref>'}
    )

    profile = fields.Relationship(
        self_url='/{origin}/{id}/profile',
        self_url_kwargs={'id': '<external_ref>', 'origin': '<origin>'},
        many=True, include_resource_linkage=True,
        type_='characterization',
        id_field='quantity.external_ref'
    )

    class Meta:
        type_ = 'flow'
        strict = True
        self_url = '/{origin}/{id}'
        self_url_kwargs = {'id': '<externalId>', 'origin': '<origin>'}
        self_url_many = '/{origin}/quantities/'


class ProcessSchema(EntitySchema):
    spatial_scope = fields.Str(attribute='SpatialScope')
    temporal_scope = fields.Str(attribute='TemporalScope')
    Classification = fields.List(fields.Str())

    reference_entity = fields.Relationship(
        self_url='/{origin}/{id}/reference',
        self_url_kwargs={'id': '<external_ref>',
                         'origin': '<origin>'},
        many=True, include_resource_linkage=True,
        type_='reference_exchange'
    )

    exchanges = fields.Relationship(
        self_url='/{origin}/{id}/exchanges',
        self_url_kwargs={'id': '<external_ref>',
                         'origin': '<origin>'},
        related_url='/{origin}/{id}/exchanges',
        related_url_kwargs={'id': '<external_ref>',
                            'origin': '<origin>'}
    )

    class Meta:
        type_ = 'process'
        strict = True


class FlowTermination(Schema):
    id = fields.Function(lambda x: '(%s, %s, %s)' % (x.flow.external_ref, x.termination.direction, x.termination))

    flow = fields.Relationship(
        related_url='/{origin}/{flow_id}',
        related_url_kwargs={'origin': '<flow.origin>',
                            'process_id': '<flow.external_ref'}
    )
    direction = fields.Str(attribute='termination.direction')
    termination = fields.Str()

    parent = fields.Relationship(
        related_url='/{origin}/{parent_id}',
        related_url_kwargs={'origin': '<parent.origin>',
                            'parent_id': '<parent.external_ref>'}
    )

    class Meta:
        type_ = 'termination'
        strict = True
