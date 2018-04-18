"""
Marshmallow entities to be used in Antelope V2 hosting
"""

from marshmallow import Schema, fields


class EntitySchema(Schema):
    id = fields.Str(attribute='external_ref', dump_to='externalId')
    uuid = fields.Str()
    origin = fields.Str()
    entity_type = fields.Str(dump_to='entityType')

    name = fields.Function(lambda obj: obj['Name'])
    comment = fields.Function(lambda obj: obj['Comment'])


class QuantitySchema(EntitySchema):
    reference_entity = fields.Function(lambda obj: obj.unit(), dump_to='referenceUnit')

    class Meta:
        type_ = 'quantity'
        strict = True
        self_url = '/{origin}/{id}'
        self_url_kwargs = {'id': '<externalId>', 'origin': '<origin>'}
        self_url_many = '/{origin}/quantities/'
