"""
This is a minimal Antelope v2 server, providing the contents of ONE archive at the TOP LEVEL (i.e. resource origin
must be implicitly included in the API root)

Still learning here...
"""

from flask import Flask, jsonify, request
# from flask_rest_jsonapi import Api, ResourceDetail, ResourceList

from .av2_types import FlowSchema


from antelope_catalog import LcResource
from lcatools import LcQuery


def _get_query():
    """
    User must write this code
    :return: an object suitable for use as an input argument to an LcQuery
    """
    a = LcResource.from_json('/data/LCI/cat-food/resources/local.uslci.olca')[0]
    a.check(None)
    return LcQuery(a.archive)


app = Flask(__name__)

query = _get_query()


@app.route('/')
def index():
    return jsonify(name="Antelope v2 Server",
                   maintainer="Brandon Kuczenski",
                   origin=query.origin)


@app.route('/flows')
def flows():
