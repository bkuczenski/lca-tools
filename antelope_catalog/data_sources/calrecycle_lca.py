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

from lcatools.entities.fragment_editor import create_fragment, set_child_exchanges
from lcatools.interfaces import comp_dir
from antelope_catalog.catalog.lc_resolver import ResourceNotFound

from .data_source import DataSource, DataCollection
from lcatools.entities.fragments import BalanceAlreadySet


SemanticResource = namedtuple('SemanticResource', ('path', 'ref', 'privacy', 'priority'))


# DATA_ROOT = '/data/GitHub/CalRecycle/LCA_Data/'

data_main = SemanticResource('Full UO LCA Flat Export BK', 'calrecycle.uolca.core', 0, 10)
data_improper = SemanticResource('Full UO LCA Flat Export Improper', 'calrecycle.uolca.improper', 0, 20)
data_ecoinvent = SemanticResource('Full UO LCA Flat Export Ecoinvent', 'calrecycle.uolca.ecoinvent.2.2', 1, 30)
data_pe24 = SemanticResource('Full UO LCA Flat Export PE-SP24', 'calrecycle.uolca.pe.sp24', 1, 40)
data_pe22 = SemanticResource('Full UO LCA Flat Export PE-SP22', 'calrecycle.uolca.pe.sp22', 1, 30)  # default

lcia_elcd = SemanticResource('ELCD-LCIA', 'calrecycle.lcia.elcd', 0, 70)
lcia_traci = SemanticResource('TRACI Core-4 Export', 'calrecycle.lcia.traci.2.0', 0, 70)

sources = (data_main, data_improper, data_ecoinvent, data_pe22, data_pe24)
lcia_sources = (lcia_elcd, lcia_traci)

private_roots = [k.ref for k in sources if k.privacy > 0]


class CalRecycleArchive(DataSource):
    _ds_type = 'IlcdArchive'

    def __init__(self, data_root, semantic_resource):
        archive_path = os.path.join(data_root, semantic_resource.path)
        super(CalRecycleArchive, self).__init__(archive_path)
        self._info = semantic_resource

    @property
    def references(self):
        for k in (self._info.ref,):
            yield k

    def interfaces(self, ref):
        yield 'inventory'
        if self._info.privacy > 0 or ref == 'calrecycle.uolca.core':
            yield 'background'
        if self._info.privacy == 0:
            yield 'quantity'

    def make_resources(self, ref):
        if ref == self._info.ref:
            yield self._make_resource(ref, self.root, privacy=self._info.privacy, priority=self._info.priority,
                                      interfaces=[k for k in self.interfaces(ref)])


class CalRecycleLcia(CalRecycleArchive):
    _ds_type = 'IlcdLcia'

    def interfaces(self, ref):
        yield 'quantity'


class CalRecycleConfig(DataCollection):
    def factory(self, data_root, **kwargs):
        for source in sources:
            yield CalRecycleArchive(data_root, source)
        for source in lcia_sources:
            yield CalRecycleLcia(data_root, source)

    def foreground(self, cat, **kwargs):
        self.register_all_resources(cat)
        return CalRecycleImporter.run_import(self.root, cat, **kwargs)


'''
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
'''


def direction_map(dir_id):
    return {'1': 'Input',
            1: 'Input',
            '2': 'Output',
            2: 'Output'}[dir_id]


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
    runs to create
    """
    @classmethod
    def run_import(cls, data_root, cat, origin='calrecycle.uolca', fg_path='fg', **kwargs):
        """

        :param data_root:
        :param cat:
        :param origin: ['calrecycle.uolca']
        :param fg_path: ['fg'] either a directory name in the catalog root, or an absolute path
        :return: a foreground archive containing the model
        """
        imp = cls(data_root, origin=origin, **kwargs)

        qi = cat.query(origin)
        try:
            fg = cat.get_archive(origin, 'foreground')
        except ResourceNotFound:
            cat.foreground(fg_path, ref=origin)  # currently this returns an interface- but op requires archive
            fg = cat.get_archive(origin, 'foreground')

        if fg.count_by_type('fragment') < len(imp.ff):
            for f in imp.ff:
                imp.fragment_from_fragment_flow(qi, f)

            imp.terminate_fragments(qi)

            imp.pe_sp24_correction(cat)

            imp.uslci_bg_elec_correction()

            imp.inv_ethylene_glycol_correction()

            imp.set_balances()

            for f in imp.fragments:
                fg.add_entity_and_children(f)
                if f.reference_entity is None:
                    f_id = imp.fragment_flow_by_index(f['FragmentFlowID'])['FragmentID']
                    fg.name_fragment(f, 'fragments/%s' % f_id)

            fg.save()

        return fg

    @property
    def fragment_dir(self):
        return os.path.join(self._root, 'fragments')

    def _print(self, *args):
        if not self._quiet:
            print(*args)

    def __init__(self, data_root, origin, quiet=True):
        if origin is None:
            raise ValueError('origin is required')
        self._origin = origin
        self._quiet = quiet
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

    @property
    def fragments(self):
        for f in self._frags.values():
            yield f

    def fragment_by_index(self, index):
        return self._fragments[str(int(index))]

    def fragment_flow_by_index(self, index):
        return self.ff[int(index) - 1]

    def fragment_from_fragment_flow(self, qi, ff):
        """
        :param qi: query interface for entity lookup
        :param ff: should be a decorated entry from self._ff
        :return:
        """
        if ff['FragmentFlowID'] in self._frags:
            self._print('Already loaded: %s' % ff['FragmentFlowID'])
            return
        flow = qi.get(ff['FlowUUID'])
        direction = direction_map(ff['DirectionID'])
        stage = self.stage(ff['FragmentStageID'])
        name = ff['Name']

        # create the fragment, with or without a parent
        if ff['ParentFragmentFlowID'] == '':
            # must be a reference flow
            frag_uuid = ff['Fragment']['FragmentUUID']
            frag = create_fragment(flow, comp_dir(direction), uuid=frag_uuid, StageName=stage, Name=name,
                                   FragmentFlowID=ff['FragmentFlowID'], origin=self._origin)
            self._fragments[ff['Fragment']['FragmentID']] = frag
        else:
            try:
                parent = self._frags[ff['ParentFragmentFlowID']]
            except KeyError:
                self._print('Recursing to %s' % ff['ParentFragmentFlowID'])
                self.fragment_from_fragment_flow(qi, self.fragment_flow_by_index(ff['ParentFragmentFlowID']))
                parent = self._frags[ff['ParentFragmentFlowID']]

            frag_uuid = ff['FragmentUUID']
            frag = create_fragment(flow, direction, uuid=frag_uuid, Name=name, StageName=stage, parent=parent,
                                   FragmentFlowID=ff['FragmentFlowID'])

        # save the fragment
        if frag is None:
            raise TypeError
        self._frags[ff['FragmentFlowID']] = frag

    def terminate_fragments(self, qi, frags=None):
        for ff in self.ff:
            debug = False
            if frags is not None:
                if ff['FragmentFlowID'] not in frags:
                    continue
                self._print('FragmentFlowID: %s' % ff['FragmentFlowID'])
                debug = True

            try:
                self._terminate_fragment(qi, ff, debug=debug)
            except ZeroDivisionError:
                print('!! Skipping Frag: %s' % ff['FragmentFlowID'])

    def _terminate_fragment(self, qi, ff, debug=False):
        frag = self._frags[ff['FragmentFlowID']]
        if debug:
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
                set_child_exchanges(frag)
        elif ff['NodeTypeID'] == '2':
            term_frag = self._fragments[ff['SubFragment']['SubFragmentID']]
            term_flow = qi.get(ff['SubFragment']['FlowUUID'])
            if term_flow is term_frag.flow:
                term_node = term_frag
            else:
                term_node = None
                for c in term_frag.top().traverse(None):
                    if c.term.is_null and c.fragment.flow is term_flow:
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
        return frag

    def set_balances(self):
        for f in self.ff:
            if f['NodeTypeID'] == '1':
                if f['Process']['ConservationFFID'] != '':
                    try:
                        self._frags[f['Process']['ConservationFFID']].set_balance_flow()
                    except BalanceAlreadySet:
                        pass

    '''
    Data Corrections
    '''
    def pe_sp24_correction(self, cat):
        """
        Add an 'sp24' scenario that sets all PE processes to their SP-24 versions
        :param cat:
        :return:
        """
        sp24 = cat.query('calrecycle.uolca.pe.sp24')
        for f in self._frags.values():
            if f.term.is_process:
                if f.term.term_node.origin == 'calrecycle.uolca.pe.sp22':
                    f.terminate(sp24.get(f.term.term_node.external_ref), scenario='sp24',
                                term_flow=f.term.term_flow,
                                direction=f.term.direction,
                                descend=f.term.descend)

    def uslci_bg_elec_correction(self):
        """
        Set the rolled-up USLCI processes to be computed via the background interface, to keep their
        flows out of foreground disclosures
        :return:
        """
        uslci_bg_ffids = ('14', '26', '38', '50', '62', '74')
        for i in uslci_bg_ffids:
            frag = self._frags[i]
            frag.set_background()

    def inv_ethylene_glycol_correction(self):
        """
        Legacy Antelope online tool fails to generate nonzero impact scores for inverted processes because
        the directions don't match on the join.  The model has only one inverted process: avoided ethylene glycol,
        thinkstep process with UUID 'df09efef-4d73-4b45-a899-1c6d1ca97da0'
        :return:
        """
        for f in self._frags.values():
            if f.term.is_process:
                if f.term.term_node.uuid == 'df09efef-4d73-4b45-a899-1c6d1ca97da0':
                    f.set_exchange_value(scenario='quell_eg', value=0.0)


if __name__ == '__main__':
    print('foo')
