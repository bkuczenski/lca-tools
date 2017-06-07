import os

# from lcatools.lcia_results import LciaResults
from lcatools.charts import scenario_compare_figure, save_plot
from lcatools.old_foreground.report import save_stages, grab_stages


def _to_tuple(frag):
    try:
        sn = frag['ShortName']
    except KeyError:
        sn = '%.30s' % frag['Name']
    return '%5.5s' % frag.get_uuid(), sn


class ForegroundQuery(object):
    """
    query a foreground to generate results
    """
    @classmethod
    def from_fragments(cls, fm, frags, quantities, **kwargs):
        frag_tuples = [_to_tuple(x) for x in frags]
        return cls(fm, frag_tuples, quantities, **kwargs)

    def _ensure_frag(self, reference):
        """
        This converts a reference to a query object-- either a fragment or a process catalog_ref
        :param reference: uuid abbrev for frag OR full uuid for fg process OR (index, uuid) for catalog ref OR a literal
         fragment OR a literal catalog_ref
        :return:
        """
        if isinstance(reference, str):
            try:
                frag = self._fm.frag(reference)
            except StopIteration:
                frag = self._fm.ref(0, reference)
        elif isinstance(reference, tuple):
            frag = self._fm.ref(*reference)
        else:
            frag = reference
        assert frag.entity_type in ('process', 'fragment')
        return frag

    def _do_weighting(self, res):
        """

        :param res: an LciaResults object
        :return:
        """
        for k in self._ws:
            res[k.q()] = k.weigh(res)
        return res

    def _do_fragment_lcia(self, frag):
        """

        :param frag:
        :return:
        """
        if frag.entity_type == 'fragment':
            res = self._fm.fragment_lcia(frag, **self._query_args)  # return LciaResults object
        elif frag.entity_type == 'process':
            res = self._fm.fg_lcia(frag)  # query_args currently not implemented-- since pf params and scales
        else:
            raise TypeError('unhandled  entity type %s' % frag.entity_type)
        res = self._do_weighting(res)

        return [res[k] for k in self._qs]

    @staticmethod
    def _do_stages(res):
        """
        sort by first quantity
        :param res: a 1d array of LciaResult objects for different quantities for the same fragment
        :return:
        """
        return sorted(grab_stages(res), key=lambda x: res[0][x].cumulative_result)

    def _do_all_stages(self):
        stgs = []
        stgs_seen = set()
        for i, res in enumerate(self.agg_results):
            for s in self.agg_stages[i]:
                if s not in stgs_seen:
                    stgs_seen.add(s)
                    stgs.append((s, res[0][s].cumulative_result))
        return [g[0] for g in sorted(stgs, key=lambda x: x[1])]

    def __init__(self, manager, frags, quantities, weightings=None, savepath='figures',
                 **kwargs):
        """

        :param manager: a Foreground interface of some kind e.g. ForegroundManager
        :param frags: a list of tuples (object, nickname) to query in sequence.
          'object' can be one of:
          * a literal fragment
          * a catalog_ref for a process
          * a fragment abbreviation -- F.frag(abbreviation) must resolve to a fragment
          * a UUID of a process in the foreground -- manager.ref(0, uuid) must resolve to a process)
          * a 2-tuple (index, uuid) -- manager.ref(index, uuid) must resolve to a process
          implementation in _ensure_frag
        :param quantities: a list of uuids for quantities to query (the ones in foreground??)
        :param weightings: a list of LciaWeighting objects (.weigh() and .q())
        :param savepath: defaults to 'figures' subdirectory in current directory
        :param kwargs: passed to manager.fragment_lcia:
         - scenario
         - observed (bool; False)
         - scale (float; 1.0)
         - normalize (bool; False)
        """
        self._fm = manager
        self._frag_names = [k[1] for k in frags]
        self._frag_entities = [self._ensure_frag(k[0]) for k in frags]
        self._qs = quantities
        self._ws = weightings or []
        self._query_args = kwargs

        self._res = None  # cache result

        self._stages = []
        self._agg_stages = []
        self._all_stages = []

        self.savepath = savepath

    @property
    def quantities(self):
        return [self._fm[0][q] for q in self._qs]

    @property
    def scenario(self):
        if 'scenario' in self._query_args:
            return self._query_args['scenario']
        return None

    @property
    def observed(self):
        if 'observed' in self._query_args:
            return self._query_args['observed']
        return False

    @property
    def scale(self):
        if 'scale' in self._query_args:
            return self._query_args['scale']
        return 1.0

    @property
    def normalize(self):
        if 'normalize' in self._query_args:
            return self._query_args['normalize']
        return False

    @property
    def results(self):
        """
        The query result is a 2d array, indexed by [frag in frags][q in quantities] of the query
        :return:
        """
        if self._res is None:
            self._run_query()
        return self._res

    @property
    def agg_results(self):
        if self._res is None:
            self._run_query()
        return [self.aggregate(self._res[i], key=self._key(i)) for i in range(len(self._frag_entities))]

    @property
    def stages(self):
        """
        2d array, of [frag][stagename], ordered by the stage's cumulative result for the first quantity
        :return:
        """
        if self._res is None:
            self._run_query()
        return self._stages

    @property
    def agg_stages(self):
        if self._res is None:
            self._run_query()
        return self._agg_stages

    @property
    def all_stages(self):
        if self._res is None:
            self._run_query()
        return self._all_stages

    @property
    def fragments(self):
        return self._frag_entities

    @property
    def frag_names(self):
        return self._frag_names

    def _key(self, i):
        if self._frag_entities[i].entity_type == 'process':
            return lambda x: x['Name']
        else:  # frag_entities are already _ensured fragment or process
            return lambda x: x.fragment['StageName']

    def aggregate(self, res, key=lambda x: x.fragment['StageName']):
        """

        :param res: a [LciaResult] inner list, by quantities
        :param key:
        :return:
        """
        return [res[i].aggregate(key=key) for i, q in enumerate(self._qs)]

    def _run_query(self):
        """
        :return:
        """
        self._res = [self._do_fragment_lcia(f) for f in self._frag_entities]  # this is a list of LciaResults objs
        self._stages = [self._do_stages(k) for k in self._res]
        agg = self.agg_results
        self._agg_stages = [self._do_stages(k) for k in agg]
        self._all_stages = self._do_all_stages()

    def lcia_fragments_compare(self, stages=None):
        """
        Show the fragments in the query side-by-side on the same axes (create a scenario_compare_figure, using the
        different fragments as scenarios)
        :param stages:
        :return:
        """
        if stages is None:
            stages = self._all_stages
            # agg_sort = self._res[0][self._qs[0]].aggregate()
            # stages = sorted(agg_sort.components(), key=lambda x: agg_sort[x].cumulative_result)

        scenario_compare_figure(self.agg_results, stages, scenarios=self._frag_names)
        return stages

    def save_figure(self, fname, stages=None):
        """
        to 'figures' subdirectory in current directory
        :param fname:
        :param stages:
        :return:
        """
        if not os.path.exists(self.savepath):
            os.makedirs(self.savepath)
        fname = os.path.join(self.savepath, fname)
        print('Saving figure %s' % fname)
        save_plot(fname + '.eps')
        if stages is not None:
            save_stages(fname, stages)
