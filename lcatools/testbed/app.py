from flask import Flask, Blueprint
# from flask_restful import Resource, Api
# from marshmallow import Schema


hw_bp = Blueprint('hw_bp', __name__)


@hw_bp.route('/')
def index():
    return 'Hello world!'


def app_factory():
    app = Flask(__name__)
    app.config['CATALOG_ROOT'] = '/data/LCI/cat-food'  # set in environment

    app.register_blueprint(hw_bp)

    return app
