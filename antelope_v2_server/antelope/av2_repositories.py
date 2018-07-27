from flask_jsonapi import ResourceRepository


class AntelopeV2Repository(ResourceRepository):
    '''
    def create(self, data, **kwargs):
        raise exceptions.NotImplementedMethod('Creating is not implemented.')
    '''

    def __init__(self, query):
        self._query = query

    @property
    def origin(self):
        return self._query.origin

    def get_list(self, filters=None, pagination=None):
        raise exceptions.NotImplementedMethod('Getting list is not implemented.')

    def get_detail(self, id):
        raise exceptions.NotImplementedMethod('Getting object is not implemented.')

    '''
    def delete(self, id):
        raise exceptions.NotImplementedMethod('Deleting is not implemented')

    def update(self, data, **kwargs):
        raise exceptions.NotImplementedMethod('Updating is not implemented')
    '''

    def get_count(self, filters=None):
        raise NotImplementedError

