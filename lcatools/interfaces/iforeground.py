from .abstract_query import AbstractQuery


class ForegroundRequired(Exception):
    pass


class ForegroundInterface(AbstractQuery):
    _interface = 'foreground'
    pass
