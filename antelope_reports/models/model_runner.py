import csv

from collections import defaultdict
from pandas import DataFrame, MultiIndex

from lcatools.lcia_results import LciaResult


def weigh_lcia_results(quantity, *args, weight=None):
    """
    Merge together a collection of LciaResult objects
    :param quantity: the weighing indicator- should be distinct from the supplied results but doesn't have to be
    :param args: a list of LciaResult objects to weight
    :param weight: optional dict of quantity-to-weight (if omitted, all results will be given unit weight)
    :return:
    """
    if weight is None:
        weight = dict()
    scenarios = sorted(set(arg.scenario for arg in args))
    if len(scenarios) > 1:
        print('Warning: multiple scenarios being combined together; using first only\n%s' % scenarios)
    scenario = scenarios[0]

    result = LciaResult(quantity, scenario=scenario)

    for arg in args:
        if arg.quantity in weight:
            _w = weight[arg.quantity]
        else:
            _w = 1.0

        for k in arg.keys():
            c = arg[k]
            result.add_summary(k, c.entity, 1.0, c.cumulative_result * _w)

    return result


class LcaModelRunner(object):
    _agg = None
    _seen_stages = None
    _fmt = '%.10e'

    def __init__(self, agg_key=None):
        """

        :param agg_key: default is StageName
        """
        self._scenarios = []  # sequential list of scenario names

        self._lcia_methods = []
        self._weightings = dict()

        self._results = dict()
        self.set_agg_key(agg_key)

    def recalculate(self):
        self._results = dict()
        for l in self.lcia_methods:
            self.run_lcia(l)
        for w in self.weightings:
            self._run_weighting(w)

    def set_agg_key(self, agg_key=None):
        if agg_key is None:
            agg_key = lambda x: x.fragment['StageName']

        self._agg = agg_key
        self._seen_stages = defaultdict(set)  #reset

    @property
    def scenarios(self):
        """
        This returns keys for the 'scenarios' in the tool (first result index)
        Must be implemented in a subclass
        :return:
        """
        for k in self._scenarios:
            yield k

    def add_scenario(self, name):
        if name in self._scenarios:
            raise KeyError('Case already exists: %s' % name)
        self._scenarios.append(name)

    @property
    def quantities(self):
        """
        This returns known quantities (second result index)
        :return:
        """
        for k in self.lcia_methods:
            yield k
        for k in self.weightings:
            yield k

    @property
    def lcia_methods(self):
        """
        This returns the LCIA methods, which are a subset of quantities
        :return:
        """
        return self._lcia_methods

    @property
    def weightings(self):
        """
        This returns LCIA weightings, which are the complementary subset to lcia_methods
        :return:
        """
        return list(k for k in self._weightings.keys())

    @property
    def format(self):
        return self._fmt

    @format.setter
    def format(self, fmt):
        self._fmt = str(fmt)

    def add_weighting(self, quantity, *measures, weight=None):
        """
        Compute a weighted LCIA result
        :param quantity:
        :param measures: a list of LCIA quantities to be weighed
        :param weight: an optional dictionary of quantity: weight (default is equal weighting)
        :return:
        """
        if weight is None:
            weight = {m: 1.0 for m in measures}

        self._weightings[quantity] = weight
        self._run_weighting(quantity)

    def _run_weighting(self, quantity):
        ws = self._weightings[quantity]
        for q in ws.keys():
            self.run_lcia(q)
        for scen in self.scenarios:
            res = [self.result(scen, q) for q in ws.keys()]
            wgt = weigh_lcia_results(quantity, *res, weight=ws)
            self._results[scen, quantity] = wgt

    @property
    def stages(self):
        return sorted(k for k, v in self._seen_stages.items() if len(v) > 0)

    def scenarios_with_stage(self, stage):
        return self._seen_stages[stage]

    def result(self, scenario, lcia_method):
        return self._results[scenario, lcia_method]

    def run_lcia(self, lcia, **kwargs):
        if lcia not in self._lcia_methods:
            self._lcia_methods.append(lcia)
        for scen in self.scenarios:
            res = self._run_scenario_lcia(scen, lcia, **kwargs)
            res.scenario = scen
            for stg in list(res.aggregate(key=self._agg).keys()):
                self._seen_stages[stg].add(scen)
            self._results[scen, lcia] = res
        return self.lcia_results(lcia)

    def lcia_results(self, lcia):
        return [self._results[scenario, lcia] for scenario in self.scenarios]

    def _format(self, result):
        if self._fmt is None:
            return result
        return self._fmt % result

    def _csv_formatter(self, style):
        """
        Specifies how the rows should be constructed for the CSV result writer.
        Designed to be subclassed so that mix-ins can provide new output styles.  The proper way to do this is to
        define a method named '_csv_format_%s' % style which returns: (header_list, row_generator)
        The row_generator method should take as argument (scenario, quantity, **kwargs) and yield a dict whose keys are
        a subset of header_list and whose values are written into the file.

        Note: in practice it takes a lot of gymnastics to be able to take advantage of this (e.g. to debug /alter csv
        reports without exiting the session.
        :param style:
        :return:
        """
        st_name = '_csv_format_%s' % style
        if hasattr(self, st_name):
            return getattr(self, st_name)()
        return self.results_headings, self._gen_aggregated_lcia_rows

    results_headings = ['scenario', 'stage', 'method', 'category', 'indicator', 'result', 'units']

    # tabular for all: accept *args as result items, go through them one by one
    def results_to_csv(self, filename, scenarios=None, style=None, **kwargs):
        if scenarios is None:
            scenarios = sorted(self.scenarios)
        else:
            known = list(self.scenarios)
            scenarios = list(filter(lambda x: x in known, scenarios))

        headings, agg = self._csv_formatter(style)

        with open(filename, 'w') as fp:
            cvf = csv.DictWriter(fp, headings, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
            cvf.writeheader()

            for q in self.quantities:
                for scenario in scenarios:
                    for k in agg(scenario, q, **kwargs):
                        cvf.writerow(k)

    @staticmethod
    def _gen_row(q, k):
        k['method'] = q.get('method') or ''
        k['category'] = q.get('category') or q.name
        k['indicator'] = q['indicator']
        k['units'] = q.unit()
        return k

    def _gen_aggregated_lcia_rows(self, scenario, q, include_total=False):
        res = self._results[scenario, q]
        for c in sorted(res.aggregate(key=self._agg).components(), key=lambda x: x.entity):
            stage = c.entity
            result = c.cumulative_result
            yield self._gen_row(q, {
                'scenario': str(scenario),
                'stage': stage,
                'result': self._format(result)
            })
        if include_total:
            yield self._gen_row(q, {
                'scenario': str(scenario),
                'stage': 'Net Total',
                'result': self._format(res.total())
                 })

    def _finish_dt_output(self, dt, column_order, filename):
        """
        Add a units column, order it first, and transpose
        :param dt:
        :param column_order:
        :param filename:
        :return:
        """
        if column_order is None:
            ord_columns = list(dt.columns)
        else:
            ord_columns = [k for k in column_order if k in dt.columns]
            ord_columns += [k for k in dt.columns if k not in ord_columns]

        dto = dt[ord_columns].transpose()
        if filename is not None:
            dto.to_csv(filename, quoting=csv.QUOTE_ALL)

        return dto

    @property
    def _qty_tuples(self):
        for q in self.quantities:
            if q.has_property('ShortName'):
                yield q['ShortName'], q['Indicator'], q.unit()
            else:
                yield q['Name'], q['Indicator'], q.unit()

    def scenario_detail_tbl(self, scenario, filename=None, column_order=None):
        dt = DataFrame(({k.entity: self._format(k.cumulative_result)
                         for k in self.result(scenario, l).aggregate(key=self._agg).components()}
                        for l in self.quantities), index=MultiIndex.from_tuples(self._qty_tuples))
        return self._finish_dt_output(dt, column_order, filename)

    def scenario_summary_tbl(self, filename=None, column_order=None):
        if column_order is None:
            column_order = list(self.scenarios)
        dt = DataFrame(({k: self._format(self.result(k, l).total()) for k in column_order}
                        for l in self.quantities),
                       index=MultiIndex.from_tuples(self._qty_tuples))
        return self._finish_dt_output(dt, column_order, filename)

    '''
    Subclass must implement only one function: a mapping from scenario key and lcia method to result
    '''
    def _run_scenario_lcia(self, scenario, lcia, **kwargs):
        """
        Maps scenario name to LCIA Result. Must be implemented in a subclass
        :param scenario:
        :param lcia:
        :return: LciaResult
        """
        return NotImplemented
