from flask import Flask
from flask_marshmallow import Marshmallow
from lcatools import LcCatalog


app = Flask(__name__)
app.config.
app.catalog = LcCatalog(app.config['catalog_rootdir'])


ma = Marshmallow(app)


def register_new_foreground(fname, study_pub):
    app.register_blueprint()