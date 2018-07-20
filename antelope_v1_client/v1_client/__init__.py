from .antelope_v1 import AntelopeV1Client


def init_fcn(source, ref=None, **kwargs):
    if ref is None:
        ref = 'test.antelope.v1client'
