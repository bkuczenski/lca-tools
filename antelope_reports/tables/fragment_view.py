from collections import defaultdict
from pandas import DataFrame, MultiIndex


def _mag(ff):
    if ff.fragment.direction == 'Input':
        return ff.magnitude
    return -1.0 * ff.magnitude


class FragmentViewer(object):
    """
    Show the exterior flows coming out of a fragment
    """
    def _generate(self):
        tl = defaultdict(list)
        # sort fragment flows by flow
        for ff in self._frag.traverse(scenario=self._scen):
            if not ff.term.is_null:
                continue
            tl[ff.fragment.flow].append(ff)

        ks = list(tl.keys())  # list of flows
        self._mi = MultiIndex.from_tuples((k.name, k.unit()) for k in ks)
        self._df = DataFrame(({k.fragment.reference_entity: _mag(k) for k in tl[z]} for z in ks), index=self._mi)
        self._mc = MultiIndex.from_tuples((k['Name'], k['StageName']) for k in self._df.columns)
        self._df.columns = self._mc
        self._df.sort_index(axis=0, level=1, inplace=True)
        self._df.sort_index(axis=1, level=1, inplace=True)

    def __init__(self, fragment, scenario=None, total=False):
        self._frag = fragment
        self._scen = scenario

        self._df = None  # dataframe
        self._mc = None  # multicolumn
        self._mi = None  # multiindex

        self._generate()

        if total:
            self.add_total()

    @property
    def df(self):
        return self._df

    @property
    def index(self):
        return self._mi

    @property
    def columns(self):
        return self._mc

    def stages(self):
        return sorted(set(self._mc.get_level_values(1)))

    def stage_table(self, stage, dropna='all', total=True):
        dd = self._df.loc[:, self._df.columns.get_level_values(1) == stage].dropna(how=dropna)
        if total:
            dd['Total'] = dd.sum(axis=1, skipna=True)
        return dd

    def add_total(self):
        self._df['Total'] = self._df.sum(axis=1, skipna=True)
