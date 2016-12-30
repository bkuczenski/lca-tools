import os

from lcatools.lcia_results import LciaResults
from lcatools.charts import scenario_compare_figure, save_plot
from lcatools.foreground.report import stage_name_table


class ForegroundQuery(object):
    """
    query a foreground to generate results
    """
    def _ensure_frag(self, abbrev):
        if isinstance(abbrev, str):
            frag = self._fm.frag(abbrev)
        else:
            frag = abbrev
        return frag

    def _do_weighting(self, res):
        """

        :param res: an LciaResults object
        :return:
        """
        for k in self._ws:
            res[k.q()] = k.weigh(res)


    def _do_fragment_lcia(self, frag, **kwargs):
        frag = self._ensure_frag(frag)

        res = self._fm.fragment_lcia(frag, **kwargs)  # returns an LciaResults object
        self._do_weighting(res)
        return res

    def _stages(self):
        return

    def __init__(self, manager, frags, quantities, weightings=None, savepath='figures'):
        """

        :param manager: a Foreground interface of some kind e.g. ForegroundManager
        :param frags: a list of tuples (uuid, nickname) to query in sequence.
        :param quantities: a list of uuids for quantities to query (the ones in foreground??)
        :param weightings: a list of LciaWeighting objects (.weigh() and .q())
        :param savepath: defaults to 'figures' subdirectory in current directory
        """
        self._fm = manager
        self._frags = frags
        self._qs = quantities
        self._ws = weightings

        self._res = None  # cache result

        self.savepath = savepath

    @property
    def _frag_refs(self):
        return [f[0] for f in self._frags]

    @property
    def _frag_names(self):
        return [f[1] for f in self._frags]

    def aggregate(self, res, key=lambda x: x.fragment['StageName']):
        return [res[k].aggregate(key=key) for k in self._qs]

    def _run_query(self, **kwargs):
        self._res = [self._do_fragment_lcia(f, **kwargs) for f in self._frag_refs]  # this is a list of LciaResults objs

    def lcia_scenario_compare(self, stages=None, **kwargs):
        if self._res is None:
            self._run_query(**kwargs)

        if stages is None:
            agg_sort = self._res[0][self._qs[0]].aggregate()
            stages = sorted(agg_sort.components(), key=lambda x: agg_sort[x].cumulative_result)

        scenario_compare_figure([self.aggregate(k) for k in self._res], stages, scenarios=self._frag_names)
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
            out = stage_name_table(stages)
            with open(fname + '_stages', 'w') as fp:
                fp.write(out)




