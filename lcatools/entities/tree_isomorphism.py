"""
A function to test whether two fragments are isomorphic.

We will say two fragments are isomorphic iff:
 - they have the same flow and direction
 - they have the same default exchange value
 - they have the same default termination
 - they have the same number of child flows
 - each child flow in one is isomorphic to a child flow in the other
 = recurse

What a mess this is to test.
"""
from collections import defaultdict


class TreeIsomorphismException(Exception):
    pass


class FlowMismatch(TreeIsomorphismException):
    pass


class DirectionMismatch(TreeIsomorphismException):
    pass


class ExchangeValueMismatch(TreeIsomorphismException):
    pass


class TermMismatch(TreeIsomorphismException):
    pass


class ChildFlowMismatch(TreeIsomorphismException):
    pass


def isomorphic(f1, f2, scenario=None):
    if f1.flow != f2.flow:
        raise FlowMismatch('%s | %s' % (f1, f2.flow))
    if f1.direction != f2.direction:
        raise DirectionMismatch('%s | %s' % (f1, f2.direction))
    if f1.exchange_value(scenario) != f2.exchange_value(scenario):
        if not(f1.is_reference ^ f2.is_reference):
            # if both references or neither references, the exchange values should match.
            raise ExchangeValueMismatch('%s | %s' % (f1, f2.exchange_value(scenario)))
    if f1.termination(scenario) != f2.termination(scenario):
        raise TermMismatch('%s | %s' % (f1, f2.termination(scenario)))

    '''
    Create two collections of child flows and compare them pairwise and recursively.  Build a list of matched 
    child flows from f1, deplete the list of available child flows from f2.  When the built list equals the total
    set of f1 child flows and the depleted list goes to zero, the child flows are identical.
    '''
    cf1 = defaultdict(set)
    cf2 = defaultdict(set)
    c1 = set(c for c in f1.child_flows)
    c2 = set(c for c in f2.child_flows)
    for c in c1:
        cf1[c.flow, c.direction].add(c)
    for c in c2:
        cf2[c.flow, c.direction].add(c)
    if set(cf1.keys()) != set(cf2.keys()):
        raise ChildFlowMismatch('%s\n%s' % (f1, f2))
    c1check = set()
    for c in c1:
        for x in cf2[c.flow, c.direction]:
            if x not in c2:
                continue
            try:
                # this is necessary in case fragments have multiple children with the same flow + direction
                isomorphic(c, x)
            except TreeIsomorphismException:
                # only problem with this is that it conceals the exception that caused the failure
                continue
            c2.remove(x)
            c1check.add(c)

    if len(c2) == 0:
        if c1check == c1:
            return True
        else:
            raise ChildFlowMismatch('missed: %s' % ('\n'.join(str(k) for k in c1 - c1check)))
    else:
        if c1check == c1:
            raise ChildFlowMismatch('unaccounted: %s' % ('\n'.join(str(k) for k in c2)))
        else:
            c1miss = c1 - c1check
            print('Left in f1:')
            for c in c1miss:
                print(c)
            print('Left in f2:')
            for c in c2:
                print(c)
            raise TreeIsomorphismException
