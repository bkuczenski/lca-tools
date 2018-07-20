"""
This file contains static variables that govern query authorization, with documentation

The basic concept here is that the antelope servers do not know anything about authorization or user accounts; all
they can do is validate an access token.  The access token is issued

"""


"""
Antelope servers are read-only.  Queries must belong to one of the below interfaces 
"""
allowed_interfaces = ('index', 'inventory', 'background', 'quantity')

"""
Different queries have different scope requirements-- these are cross-cutting relative to interfaces

index: all index queries have index scope.  If authorization fails, the index query is not allowed.  
info: foreground_flows, background_flows, exterior_flows, cutoffs, is_in_background have info scope.  If authorization 
      fails, the info query is not allowed.
exch: whether the list of exchanges / fragments can be known.  If the authorization fails,
      exchanges() and inventory() queries fail
      traverse() queries report only the reference node
      foreground() queries report only the reference node
      dependencies(), emissions(), ad(), bf(), queries fail
      fragment_lcia() queries are aggregated
exch_val: whether the exchange /lci values can be known.  If the authorization fails,
          exchange_values() and exchange_relation() queries fail
          lcia() queries are aggregated
          fragment_lcia() queries are flattened (???)
          exchanges(), inventory(), traverse() and foreground() queries are trimmed
          dependencies(), emissions(), ad(), bf(), queries are trimmed
origin: whether the true origins of entities are reported. If authorization fails, the publication origin is used as
        masquerade. Note that this may cause the entities to not be resolvable.
lci: whether LCI aggregation is permitted.  If the authorization fails, 
     lcia(), bg_lcia(), fragment_lcia() queries are aggregated
     lci() queries fail
"""
allowed_scopes = ('index', 'info', 'exch', 'exch_val', 'origin', 'lci')


class PrivacyDeclaration(object):
    """
    A specification of privacy scopes
    """
    @classmethod
    def from_int(cls, privacy):
        """
        Issue the specified score as a blanket privacy requirement for all scopes
        :param privacy:
        :return:
        """
        return cls(privacy)

    @classmethod
    def from_dict(cls, privacy):
        """

        :param privacy:
        :return:
        """
        return cls(**privacy)

    def __init__(self, base=None, **kwargs):
        """
        The Privacy Declaration specifies what information may be revealed in response to an authorized antelope
        query.

        Allowed scopes: index; info; exch; exch_val; origin; lci

        :param base: Base privacy level required for all queries.
        """
        self._base = base
        self._policy = dict()
        for kw, arg in kwargs.items():
            self._set_policy(kw, arg)

    def _set_policy(self, scope, score):
        if scope in allowed_scopes:
            self._policy[scope] = score

    def _get_policy(self, scope):
        if scope in self._policy:
            return self._policy[scope]
        return self._base

    def test(self, scope, score):
        if scope not in allowed_scopes:
            return False
        policy = self._get_policy(scope)
        if policy is None:
            return True
        score = max(score, 0)
        return score <= policy

    def serialize(self):
        ser = {kw: arg for kw, arg in self._policy.items()}
        if self._base is not None:
            ser['base'] = self._base
        return ser
