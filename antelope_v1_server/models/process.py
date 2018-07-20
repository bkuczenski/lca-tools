from flask_marshmallow import Schema
from flask_marshmallow.fields import Hyperlinks, URLFor
from marshmallow import fields


class ProcessSchema(Schema):
    process_id = fields.Int(dump_to='processID')
    reference_year = fields.Function(serialize=lambda itm: itm['TemporalScope'],
                                     dump_to='referenceYear')
    geography = fields.Function(serialize=lambda itm: itm['SpatialScope'])
    # reference_type_id = SKIP
    # composition_flow_id = SKIP
    data_source = fields.Str(attribute='origin', dump_to='dataSource')
    has_elem = fields.Bool(dump_to='hasElementaryFlows')
    name = fields.Function(serialize=lambda itm: itm['Name'])
    uuid = fields.Str()
    version = fields.Function(serialize=lambda itm: itm['Version'])
    resource_type = fields.Str(attribute='entity_type', dump_to='resourceType')
    is_private = fields.Bool(dump_to='isPrivate')

    _links = Hyperlinks({
        'self': URLFor('processes', id='<id>')
    })
