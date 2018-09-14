"""
This is a minimal Antelope v2 server, providing the contents of ONE archive at the TOP LEVEL (i.e. resource origin
must be implicitly included in the API root)

Still learning here...
"""

import os

from flask import Flask, jsonify, request, abort, send_from_directory
# from flask_jsonapi import Api, ResourceRepository

from .av2_types import FlowSchema, QuantitySchema, ProcessSchema

from itertools import islice

from antelope_catalog import LcResource
from lcatools import LcQuery


list_fields = ('id', 'origin', 'name')


def _get_query():
    """
    User must write this code
    :return: an object suitable for use as an input argument to an LcQuery
    """
    a = LcResource.from_json('/data/LCI/cat-food/resources/local.uslci.olca')[0]
    a.check(None)
    a.archive.load_all()
    return LcQuery(a.archive)


app = Flask(__name__)
app.config['DEBUG'] = True


if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    query = _get_query()
else:
    query = False


@app.route('/')
def index():
    return jsonify(name="Antelope v2 Server",
                   maintainer="Brandon Kuczenski",
                   origin=query.origin)


flow_schema = FlowSchema()
process_schema = ProcessSchema()
quantity_schema = QuantitySchema()


@app.route('/flows')
def flows():
    count = request.args.get('count', 50)
    if len(request.args) > 0:
        only = list_fields + tuple(k.lower() for k in request.args.keys() if k not in ('count',))
    else:
        only = list_fields
    flows_schema = FlowSchema(many=True, only=only)

    f = query.flows(**request.args)
    if count > 0:
        gen = islice(f, count)
    else:
        gen = f
    return jsonify(flows_schema.dump(gen))


@app.route('/flows/<entity_id>')
def flow(entity_id):
    return show_entity(entity_id, _etype='flow')


def _get_right(entity_id, _etype=None):
    entity = query.get(entity_id)
    if entity is None:
        abort(404)
    if _etype and _etype != entity.entity_type:
        abort(400)
    return entity


def _marshal_right(entity):
    schema = {'flow': flow_schema,
              'process': process_schema,
              'quantity': quantity_schema}[entity.entity_type]
    return schema.dump(entity)


@app.route('/favicon.ico')
def _favicon():
    return send_from_directory(os.path.join(app.root_path, 'static', ),
                               'antelope-small-ico-v2.ico', mimetype='image/vnd.microsoft.icon')


@app.route('/<entity_id>')
def show_entity(entity_id, _etype=None):
    entity = _get_right(entity_id, _etype=_etype)
    return jsonify(_marshal_right(entity))


@app.route('/<entity_id>/tags')
def show_entity_tags(entity_id, _etype=None):
    entity = _get_right(entity_id, _etype=_etype)
    return jsonify(entity.get_properties())


@app.route('/<entity_id>/tags/<tag>')
def get_entity_tag(entity_id, tag, _etype=None):
    entity = _get_right(entity_id, _etype=_etype)
    return jsonify(entity[tag])
