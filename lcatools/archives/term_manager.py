"""
Term Manager - used to handle string references to entities that are known by several synonyms.  Specifically:
 - contexts, which are drawn from flows' specified 'Compartment' attributes
 - flowables, which are drawn from flows' uuid, Name, CasNumber, and string representation

The TermManager is captive to a single archive and acts as a context manager as well as a flowables registry.

Quantity disambiguation is excluded from the Term Manager and is instead handled by the Qdb / LciaEngine, because it
is inherently useful only across data sources.
"""
from synonym_dict.example_compartments import CompartmentManager
from synonym_dict.example_flowables import FlowablesDict
from synonym_dict import MergeError


from collections import defaultdict
import re


def _flowable_terms(flow):
    yield flow['Name']
    yield str(flow)
    yield flow.uuid
    if flow.origin is not None:
        yield flow.link
    cas = flow.get('CasNumber')
    if cas is not None and len(cas) > 0:
        yield cas
    syns = flow.get('Synonyms')
    if syns is not None and len(syns) > 0:
        if isinstance(syns, str):
            yield syns
        else:
            for x in flow['Synonyms']:
                yield x


class TermManager(object):
    def __init__(self, contexts=None, flowables=None, merge_strategy='prune'):
        """
        So there are two connections to make: combine uuids and human-readable descriptors into a common database of
        synonyms, and connect those sets of synonyms to a set of entities known to the local archive.

        When harvesting terms from a new entity, the general approach is to merge the new entry with an existing entry
        if the existing one uniquely matches.  If there is more than one match, there are three general strategies:

          'prune': trim off the parts that match existing entries and make a new entry with only the distinct terms.
           this creates a larger more diffuse

          'merge': merge all the terms from the new entry and any existing entries together



        :param contexts:
        :param flowables:
        :param merge_strategy:
           'prune':

        """
        self._cm = CompartmentManager(contexts)
        self._fm = FlowablesDict(flowables)
        self._flow_map = defaultdict(set)

        self._merge_strategy = merge_strategy

    def __getitem__(self, item):
        """
        Getitem exposes only the contexts, since flow external_refs are used as flowable synonyms
        :param item:
        :return:
        """
        try:
            return self._cm.__getitem__(item)
        except KeyError:
            return None

    def get_flowable(self, term):
        return self._fm[term]

    def add_compartments(self, compartments):
        return self._cm.add_compartments(compartments)

    def add_flow(self, flow):
        """
        Add a flow's terms to the flowables list and link the flow to the flowable
        :param flow:
        :return:
        """
        try:
            fb = self._fm.new_object(*_flowable_terms(flow))
        except MergeError:
            if self._merge_strategy == 'prune':
                print('\nPruning entry for %s' % flow)
                s1 = set(_flowable_terms(flow))
                fb = self._fm.new_object(*_flowable_terms(flow), prune=True)
                s2 = set(self._fm.synonyms(fb.name))
                for k in sorted(s1.union(s2), key=lambda x: x in s2):
                    if k in s2:
                        print(k)
                    else:
                        print('*%s [%s]' % (k, self._fm[k]))
                self._flow_map[fb].add(flow)
            elif self._merge_strategy == 'merge':
                print('Merging')
                raise NotImplemented
            else:
                raise ValueError('merge strategy %s' % self._merge_strategy)

    def flowables(self, search=None):
        """
        WARNING: does not search on all synonyms, only on canonical terms
        :param search:
        :return:
        """
        for fb in self._fm.objects:
            if search is None:
                yield str(fb)
            else:
                if bool(re.search(search, str(fb), flags=re.IGNORECASE)):
                    yield str(fb)
