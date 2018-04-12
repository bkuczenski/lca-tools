"""
CalRecycle LCA
Fragment Importer

The CalRecycle fragment model is stored in four files:

 Fragment.csv - (F) names and uuids for 55 fragments
 FragmentFlow.csv - (FF) data for 888 fragment flows
 FragmentNodeProcess.csv - (FNP) data for 247 nodes terminated to foreground processes
 FragmentNodeFragment.csv - (FNF) data for 76 nodes terminated to sub-fragments

The process data all come from 5 different ILCD archives:
Full UO LCA Flat Export BK
Full UO LCA Flat Export Improper
Full UO LCA Flat Export Ecoinvent
Full UO LCA Flat Export PE-SP22
Full UO LCA Flat Export PE-SP24

FNP references unique processes by uuid + version, but the version distinction is only relevant for differentiating
PE-SP22 and PE-SP24.  In the future those will be distinguished by semantic references and not UUIDs.  For the present,
the version differences can be ignored as they can be corrected later on.

The objective here is to construct the 55 fragments and replicate the LCIA results.  Well, first to construct the
fragments. And then to tune the qdb + catalog interfaces until the LCIA results correspond.

What do we need to do for that?

"""
import os
import csv
from collections import namedtuple

from lcatools.catalog.lc_resource import LcResource
from lcatools.catalog.catalog import LcCatalog
from lcatools.entities.editor import FragmentEditor
from lcatools.exchanges import comp_dir


SemanticRoot = namedtuple('SemanticRoot', ('path', 'root', 'privacy', 'priority'))


DATA_ROOT = '/data/GitHub/CalRecycle/LCA_Data/'

data_main = SemanticRoot(os.path.join(DATA_ROOT, 'Full UO LCA Flat Export BK'), 'calrecycle.uolca.core', 0, 10)
data_improper = SemanticRoot(os.path.join(DATA_ROOT, 'Full UO LCA Flat Export Improper'), 'calrecycle.uolca.improper',
                             0, 20)
data_ecoinvent = SemanticRoot(os.path.join(DATA_ROOT, 'Full UO LCA Flat Export Ecoinvent'),
                              'calrecycle.uolca.ecoinvent.2.2', 1, 30)
data_pe24 = SemanticRoot(os.path.join(DATA_ROOT, 'Full UO LCA Flat Export PE-SP24'), 'calrecycle.uolca.pe.sp24', 1, 40)
data_pe22 = SemanticRoot(os.path.join(DATA_ROOT, 'Full UO LCA Flat Export PE-SP22'), 'calrecycle.uolca.pe.sp22', 1, 50)

lcia_elcd = SemanticRoot(os.path.join(DATA_ROOT, 'ELCD-LCIA'), 'calrecycle.lcia.elcd', 0, 70)
lcia_traci = SemanticRoot(os.path.join(DATA_ROOT, 'TRACI Core-4 Export'), 'calrecycle.lcia.traci.2.0', 0, 70)

sources = (data_main, data_improper, data_ecoinvent, data_pe22, data_pe24)

private_roots = [k.root for k in sources if k.privacy > 0]


def create_catalog(path):
    return LcCatalog(path)


def install_resources(cat):
    for a in sources:
        if a.privacy > 0:
            iface = ['inventory', 'background']
        else:
            iface = 'inventory'
        res = LcResource(a.root, a.path, 'IlcdArchive', interfaces=iface,
                         priority=a.priority, static=False)
        cat.add_resource(res)
    for q in (lcia_elcd, lcia_traci):
        res = LcResource(q.root, q.path, 'IlcdLcia', interfaces='quantity', priority=q.priority, static=False)
        cat.add_resource(res)


def direction_map(did):
    return {'1': 'Input',
            1: 'Input',
            '2': 'Output',
            2: 'Output'}[did]


def read_csv(file):
    """
    Creates a dict array where the keys are given in the first row of the CSV file
    :param file:
    :return: list of dicts.
    """
    with open(file, 'r') as fp:
        reader = csv.reader(fp, delimiter=',', quotechar='"')
        headers = next(reader)
        d = []
        for row in reader:
            d.append(dict((headers[i], v) for i, v in enumerate(row)))
    return d


def dict_from_csv(file):
    """
    Creates a dict of string first-column -> second column.
    :param file:
    :return:
    """
    with open(file, 'r') as fp:
        reader = csv.reader(fp, delimiter=',', quotechar='"')
        headers = next(reader)
        if len(headers) > 2:
            raise ValueError('This function only works with 2-column tables')
        d = dict()
        for row in reader:
            d[row[0]] = row[1]
        return d


class CalRecycleImporter(object):
    """
    Maybe we want a superclass; for now let's just put in what we need
    """
    @property
    def fragment_dir(self):
        return os.path.join(self._root, 'fragments')

    def __init__(self, data_root):
        self._root = data_root
        self._stages = dict_from_csv(os.path.join(self.fragment_dir, 'FragmentStage.csv'))
        self._f = read_csv(os.path.join(self.fragment_dir, 'Fragment.csv'))
        self._ff = read_csv(os.path.join(self.fragment_dir, 'FragmentFlowI.csv'))
        self._fnp = read_csv(os.path.join(self.fragment_dir, 'FragmentNodeProcess.csv'))
        self._fnf = read_csv(os.path.join(self.fragment_dir, 'FragmentNodeFragment.csv'))
        self._bg = read_csv(os.path.join(self.fragment_dir, 'Background.csv'))

        for term in self.fnp:
            ffid = int(term['FragmentFlowID']) - 1
            if 'Process' in self.ff[ffid]:
                raise ValueError('Fragment %d is multiply terminated' % term['FragmentFlowID'])
            self.ff[ffid]['Process'] = term
        for term in self.fnf:
            ffid = int(term['FragmentFlowID']) - 1
            if 'SubFragment' in self.ff[ffid]:
                raise ValueError('Fragment %d is multiply terminated' % term['FragmentFlowID'])
            self.ff[ffid]['SubFragment'] = term
        for frag in self.f:
            ffid = int(frag['ReferenceFragmentFlowID']) - 1
            if 'Fragment' in self.ff[ffid]:
                raise ValueError('Fragment %d is multiply referenced' % frag['ReferenceFragmentFlowID'])
            self.ff[ffid]['Fragment'] = frag

        self._ed = FragmentEditor(interactive=False)
        self._frags = dict()  # indexed with FragmentFlowID.str
        self._fragments = dict()  # indexed with FragmentID.str

    def stage(self, index):
        try:
            return self._stages[str(index)]
        except KeyError:
            return ''

    @property
    def f(self):
        return self._f

    @property
    def ff(self):
        return self._ff

    @property
    def fnp(self):
        return self._fnp

    @property
    def fnf(self):
        return self._fnf

    def fragment_by_index(self, index):
        return self._frags[int(index) - 1]

    def fragment_flow_by_index(self, index):
        return self.ff[int(index) - 1]

    def fragment_from_fragment_flow(self, qi, ff):
        """
        :param qi: query interface for entity lookup
        :param ff: should be a decorated entry from self._ff
        :return:
        """
        if ff['FragmentFlowID'] in self._frags:
            print('Already loaded: %s' % ff['FragmentFlowID'])
            return
        flow = qi.get(ff['FlowUUID'])
        direction = direction_map(ff['DirectionID'])
        stage = self.stage(ff['FragmentStageID'])
        name = ff['Name']

        # create the fragment, with or without a parent
        if ff['ParentFragmentFlowID'] == '':
            # must be a reference flow
            frag_uuid = ff['Fragment']['FragmentUUID']
            frag = self._ed.create_fragment(flow, comp_dir(direction), uuid=frag_uuid, StageName=stage, Name=name,
                                            FragmentFlowID=ff['FragmentFlowID'])
            self._fragments[ff['Fragment']['FragmentID']] = frag
        else:
            try:
                parent = self._frags[ff['ParentFragmentFlowID']]
            except KeyError:
                print('Recursing to %s' % ff['ParentFragmentFlowID'])
                self.fragment_from_fragment_flow(qi, self.fragment_flow_by_index(ff['ParentFragmentFlowID']))
                parent = self._frags[ff['ParentFragmentFlowID']]

            frag_uuid = ff['FragmentUUID']
            frag = self._ed.create_fragment(flow, direction, uuid=frag_uuid, Name=name, StageName=stage, parent=parent,
                                            FragmentFlowID=ff['FragmentFlowID'])

        # save the fragment
        if frag is None:
            raise TypeError
        self._frags[ff['FragmentFlowID']] = frag

    def terminate_fragments(self, qi, frags=None):
        for ff in self.ff:
            frag = self._frags[ff['FragmentFlowID']]
            if frags is not None:
                if ff['FragmentFlowID'] not in frags:
                    continue
                print('FragmentFlowID: %s' % ff['FragmentFlowID'])
                frag.set_debug_threshold(999)

            flow = frag.flow
            # terminate the fragment
            if ff['NodeTypeID'] == '1':
                term_node = qi.get(ff['Process']['ProcessUUID'])
                term_flow = qi.get(ff['Process']['FlowUUID'])
                frag.clear_termination()
                frag.terminate(term_node, term_flow=term_flow)
                if term_node.origin in private_roots:
                    frag.set_background()
                else:
                    frag.set_child_exchanges()
            elif ff['NodeTypeID'] == '2':
                term_frag = self._fragments[ff['SubFragment']['SubFragmentID']]
                term_flow = qi.get(ff['SubFragment']['FlowUUID'])
                if term_flow is term_frag.flow:
                    term_node = term_frag
                else:
                    term_node = None
                    for c in term_frag.io_flows(None):
                        if c.fragment.flow is term_flow:
                            term_node = c.fragment
                    if term_node is None:
                        raise ValueError('Could not find flow from inverse traversal')
                descend = {'1': True, '0': False}[ff['SubFragment']['Descend']]
                frag.clear_termination()
                frag.terminate(term_node, term_flow=term_flow, descend=descend)
            elif ff['NodeTypeID'] == '4':
                bg = next(row for row in self._bg
                          if row['FlowUUID'] == flow.uuid and row['DirectionID'] == ff['DirectionID'])
                if bg['TargetUUID'] != '':
                    if bg['NodeTypeID'] == '1':
                        term_node = qi.get(bg['TargetUUID'])
                        if term_node.origin in private_roots:
                            frag.set_background()
                    else:
                        term_node = next(_f for _f in self._fragments.values() if _f.uuid == bg['TargetUUID'])
                    frag.clear_termination()
                    frag.terminate(term_node, term_flow=flow, descend=False)

    def set_balances(self):
        for f in self.ff:
            if f['NodeTypeID'] == '1':
                if f['Process']['ConservationFFID'] != '':
                    self._frags[f['Process']['ConservationFFID']].set_balance_flow()


if __name__ == '__main__':
    print('foo')
