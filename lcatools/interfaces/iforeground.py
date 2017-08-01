from .abstract_query import AbstractQuery


class ForegroundRequired(Exception):
    pass


_interface = 'foreground'


class ForegroundInterface(AbstractQuery):
    pass
