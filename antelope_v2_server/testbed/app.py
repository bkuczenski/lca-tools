import os
from flask import Flask, Blueprint
from flask_restful import Resource, Api
from marshmallow import Schema, fields


# query = LcQuery



phony_flows = [
    'local.uslci/301',
    'local.uslci/1400',
    'local.uslci/72'
]


class EntitySchema(Schema):

    _etype = 'entity'

    data_source = fields.Str(attribute='origin', dump_to='dataSource')
    name = fields.Function(serialize=lambda itm: itm['Name'])
    uuid = fields.Str()
    version = fields.Function(serialize=lambda itm: itm['Version'])
    resource_type = fields.Str(attribute='entity_type', dump_to='resourceType')

    def dump(self, obj, _idx=None, **kwargs):
        ent = super(EntitySchema, self).dump(obj, **kwargs)
        if _idx:
            ent['%sID' % self._etype] = int(_idx)
        return ent


'''
class FlowSchema(EntitySchema):

    _etype = 'flow'

    category = fields.Function(serialize=lambda itm: itm['Compartment'][-1])
    cas = fields.Function(serialize=lambda itm: itm['CasNumber'], dump_to='casNumber')


class Av1Flow(Resource):
    def get(self, _id):
        if _id is None:
            return [FlowSchema.dump(f) for f in ]
'''


hw_bp = Blueprint('hw_bp', __name__)


@hw_bp.route('/')
def index():
    return 'Hello world!'


entities_bp = Blueprint('entities_bp', __name__)


antelope_api = Api(entities_bp)
# antelope_api.add_resource(Av1Flow)


def app_factory(cat_root=None):
    """Factory for generating Flask Apps
    :param cat_root: path to catalog root dir.  Defaults to 'catalog' subdirectory of current dir
    """

    if cat_root is None:
        cat_root = os.path.join(os.path.dirname(__file__), 'catalog')

    app = Flask(__name__)

    app.register_blueprint(hw_bp)

    return app
