
from flask import Flask

app = Flask(__name__)
app.config.from_object('antelope_v2_server.antelope.local_config')
