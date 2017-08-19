"""
Class for making waterfall charts. There is an inheritance structure to be found in these charts somewhere..
"""

import matplotlib as mpl
import matplotlib.pyplot as plt
import colorsys
from itertools import accumulate

from math import floor

from lcatools.charts import save_plot, net_color

mpl.rcParams['patch.force_edgecolor'] = True
mpl.rcParams['errorbar.capsize'] = 3
mpl.rcParams['grid.color'] = 'k'
mpl.rcParams['grid.linestyle'] = ':'
mpl.rcParams['grid.linewidth'] = 0.5
mpl.rcParams['lines.linewidth'] = 1.0
mpl.rcParams['axes.autolimit_mode'] = 'round_numbers'
mpl.rcParams['axes.xmargin'] = 0
mpl.rcParams['axes.ymargin'] = 0


def _fade_color(color):
    hsv = colorsys.rgb_to_hsv(*color)
    return colorsys.hsv_to_rgb(hsv[0], hsv[1], hsv[2]*0.8)


def _random_color(uuid, sat=0.65, val=0.95):

    _offset = 14669
    hue = ((int('%.4s' % uuid, 16) + _offset) % 65536) / 65536
    return colorsys.hsv_to_rgb(hue, sat, val)


def _grab_stages(*results):
    stages = set()
    for r in results:
        stages = stages.union(r.keys())
    return list(stages)


def _data_range(data_array):
    mx = 0.0
    mn = 0.0
    for i, data in enumerate(data_array):
        for k in accumulate(data):
            if k > mx:
                mx = k
            if k < mn:
                mn = k
    return mn, mx


class WaterfallChart(object):
    """
    A WaterfallChart turns a collection of LciaResult objects into a collection of waterfall graphs that share an
    ordinal axis.
    """

    def __init__(self, *results, stages=None, color=None, color_dict=None, filename=None, width=6, **kwargs):
        """
        Create a waterfall chart that compares the stage contributions of separate LciaResult objects.

        The positional parameters must all be LciaResult objects.  The results should all share the same quantity.

        iterable 'stages' specifies the sequence of queries to the results and the corresponding sequence of waterfall
        steps.  The stage entries are used as an argument to contrib_query. If none is specified, then a collection is
        made of all components of all supplied results.  The collection will have random order.

        The color specification is used for all stages.  Exceptions are stored in color_dict, where the same key
        used for the contrib query, if present, is used to retrieve a custom color for the stage.

        If no default color is specified, a random hue is picked based on the UUID of the quantity.

        Each bar is drawn in the same default color, unless
        :param results: positional parameters must all be LciaResult objects having the same quantity
        :param stages:
        :param color:
        :param color_dict:

        :param filename:
        :param width: axes width in inches (default 8")
        :param kwargs:
        """

        self._q = results[0].quantity

        if color is None:
            color = _random_color(self._q.uuid)

        if color_dict is None:
            color_dict = dict()

        if stages is None:
            stages = _grab_stages(*results)

        if filename is None:
            filename = 'waterfall_%.3s' % self._q.uuid

        colors = []
        for stage in stages:
            if stage in color_dict:
                colors.append(color_dict[stage])
            else:
                colors.append(color)

        data_array = []
        scenarios = []
        _net_flag = False

        for res in results:
            scenarios.append(res.scenario)
            data, net = res.contrib_new(*stages)
            if _net_flag or net != 0:
                _net_flag = True
                data.append(net)
            data_array.append(data)

        if _net_flag:
            colors.append(net_color)
            stages.append('remainder')

        self._d = data_array
        self._span = _data_range(self._d)
        self._width = width

        self._waterfall_staging_horiz(scenarios, stages, data_array, colors, **kwargs)
        save_plot(filename)

    @property
    def int_threshold(self):
        return (self._span[1] - self._span[0]) / (self._width * 1.8)

    def _waterfall_staging_horiz(self, scenarios, stages, data_array, colors,
                                 aspect=0.1, panel_sep=0.5,
                                 **kwargs):
        """
        Creates a figure and axes and populates them with waterfalls.
        :param scenarios:
        :param stages:
        :param data_array:
        :param colors:
        :param horiz:
        :param aspect:
        :param panel_sep:
        :param kwargs: num_format=%3.2g, bar_width=0.85
        :return:
        """
        num_ax = len(data_array)
        num_steps = len(stages)
        height = num_ax * (self._width * aspect * num_steps) + (num_ax - 1) * panel_sep

        _ax_hgt = self._width * aspect * num_steps / height
        _gap_hgt = panel_sep / height

        fig = plt.figure(figsize=[self._width, height])
        top = 1.0

        _mn = _mx = 0.0
        axes = []
        for i in range(num_ax):
            bottom = top - _ax_hgt
            ax = fig.add_axes([0.0, bottom, 1.0, _ax_hgt])
            axes.append(ax)
            self._waterfall_horiz(ax, data_array[i], colors, **kwargs)
            ax.set_yticklabels(stages)
            xticklabels = [_i.get_text() for _i in ax.get_xticklabels()]
            xticklabels[-1] += ' %s' % self._q.unit()
            ax.set_xticklabels(xticklabels)

            xlim = ax.get_xlim()
            if xlim[0] < _mn:
                _mn = xlim[0]
            if xlim[1] > _mx:
                _mx = xlim[1]

            if scenarios[i] is not None or num_ax > 1:
                ax.set_title(scenarios[i], fontsize=12)

            top = bottom - _gap_hgt

        for ax in axes:
            ax.set_xlim([_mn, _mx])

    def _waterfall_horiz(self, ax, data, colors, num_format='%3.2g', bar_width=0.85):
        """

        :param ax:
        :param data:
        :param colors:
        :param num_format:
        :param bar_width:
        :return:
        """
        """
        The axes are already drawn. all we want to do is make and label the bars, one at a time.
        """
        _low_gap = 0.25

        _conn_color = (0.3, 0.3, 0.3)

        cum = 0.0
        center = 0.5 - _low_gap
        top = center - 0.6 * bar_width
        bottom = floor(center + len(data))

        # vertical axis
        ax.plot([0, 0], [top, bottom], linewidth=2, color=(0, 0, 0), zorder=-1)

        _h_gap = self.int_threshold * 0.11
        yticks = []

        mx = 0.0

        for i, dat in enumerate(data):
            yticks.append(center)

            color = colors[i]

            if dat < 0:
                color = _fade_color(color)

            ax.barh(center, dat, left=cum, height=bar_width, color=color)
            if self.int_threshold is not None and abs(dat) > self.int_threshold:
                # interior label
                x = cum + (dat / 2)
                ax.text(x, center, num_format % dat, ha='center', va='center')
            else:
                # end label
                x = cum + dat
                if x < self.int_threshold:
                    ax.text(x + _h_gap, center, num_format % dat, ha='left', va='center')
                else:
                    ax.text(x - _h_gap, center, num_format % dat, ha='right', va='center')

            # connector
            if cum != 0:
                ax.plot([cum, cum], [center, center - 1], color=_conn_color, zorder=-1)

            cum += dat
            if cum > mx:
                mx = cum

            center += 1
        ax.invert_yaxis()
        ax.spines['top'].set_visible(False)
        ax.spines['bottom'].set_visible(True)
        ax.spines['bottom'].set_linewidth(2)
        ax.spines['left'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_yticks(yticks)
        ax.tick_params(labelsize=10)
        ax.tick_params(axis='y', length=0)

        # cumsum marker
        ax.plot([cum, cum], [center - 1, bottom], color=_conn_color, zorder=-1)
        ax.plot(cum, bottom - 0.4 * _low_gap, marker='v', markerfacecolor=(1, 0, 0), markeredgecolor='none',
                markersize=8)

        # x labels
        if abs(cum) > self.int_threshold:
            xticks = [0]
            xticklabels = ['0']
        else:
            xticks = []
            xticklabels = []

        if abs(mx - cum) > self.int_threshold:
            xticks.extend([cum, mx])
            xticklabels.extend([num_format % cum, num_format % mx])
        else:
            xticks.append(cum)
            xticklabels.append(num_format % cum)

        ax.set_xticks(xticks)
        ax.set_xticklabels(xticklabels)
