"""
Class for making waterfall charts. There is an inheritance structure to be found in these charts somewhere..
"""

import matplotlib as mpl
import matplotlib.pyplot as plt
import colorsys
from itertools import accumulate

from math import floor

from lcatools.charts.base import save_plot, net_color

net_style = {
    'edgecolor': 'none'
}

# mpl.rcParams['patch.force_edgecolor'] = False
mpl.rcParams['errorbar.capsize'] = 3
mpl.rcParams['grid.color'] = 'k'
mpl.rcParams['grid.linestyle'] = ':'
mpl.rcParams['grid.linewidth'] = 0.5
# mpl.rcParams['lines.linewidth'] = 1.0
mpl.rcParams['axes.autolimit_mode'] = 'round_numbers'
mpl.rcParams['axes.xmargin'] = 0
mpl.rcParams['axes.ymargin'] = 0


def _fade_color(color):
    hsv = colorsys.rgb_to_hsv(*color)
    return colorsys.hsv_to_rgb(hsv[0], hsv[1], hsv[2]*0.8)


def random_color(uuid, sat=0.65, val=0.95, offset=14669):

    hue = ((int('%.4s' % uuid, 16) + offset) % 65536) / 65536
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
    def _stage_style(self, stage):
        if stage in self._color_dict:
            this_style = {'color': self._color_dict[stage]}
        else:
            this_style = {'color': self._color}
        if stage in self._style_dict:
            this_style.update(self._style_dict[stage])
        else:
            if self._style is not None:
                this_style.update(self._style)
        return this_style

    def _adjust_autorange(self, autorange, autounits):
        the_max = next(i for i, x in enumerate(autorange) if x == max(autorange))
        the_max_range = autorange[the_max]
        self._unit = autounits[the_max]
        for i, x in enumerate(autorange):
            if i != the_max:
                factor = the_max_range / x
                self._d[i] = [k * factor for k in self._d[i]]

    def __init__(self, *results, stages=None, color=None, color_dict=None,
                 style=None, style_dict=None,
                 include_net=True, net_name='remainder',
                 filename=None, size=6, autorange=False, **kwargs):
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

        :param style: default style spec for each bar (to override pyplot bar/barh default
        :param style_dict: dict of dicts for custom styles

        :param include_net: [True] whether to include a net-result bar, if a discrepancy exists between the stage query
         and the total result
        :param net_name: ['remainder'] what to call the net-result bar

        :param filename: default 'waterfall_%.3s.eps' % uuid.  Enter 'none' to return (and not save) the chart
        :param size: axes size in inches (default 6") (width for horiz bars; height for vert bars)
        :param autorange: [False] whether to auto-range the results
        :param kwargs: panel_sep [0.65in], num_format [%3.2g], bar_width [0.85]
        """

        self._q = results[0].quantity

        self._color = color or random_color(self._q.uuid)
        self._color_dict = color_dict or dict()
        self._style = style or None
        self._style_dict = style_dict or dict()

        if stages is None:
            stages = _grab_stages(*results)

        if filename is None:
            filename = 'waterfall_%.3s.eps' % self._q.uuid

        styles = []
        _stages = []
        for stage in stages:
            _stages.append(stage)
            styles.append(self._stage_style(stage))

        data_array = []
        scenarios = []
        _net_flag = False
        ar_scale = []
        ar_units = []

        for res in results:
            scenarios.append(res.scenario)
            data, net = res.contrib_new(*stages, autorange=autorange)
            ar_scale.append(res.autorange)
            ar_units.append(res.unit())
            self._unit = res.unit()  # only need to correct this if autounits are not all the same

            _span = _data_range([data])
            if abs(net) * 1e8 > (_span[1] - _span[0]):
                # only include remainder if it is greater than 10 ppb
                _net_flag = True
                data.append(net)
            data_array.append(data)

        if _net_flag and include_net:
            _stages.append(net_name)
            if net_name not in self._color_dict:
                self._color_dict[net_name] = net_color
            if net_name not in self._style_dict:
                self._style_dict[net_name] = net_style
            styles.append(self._stage_style(net_name))

        self._d = data_array
        self._span = _data_range(self._d)
        self._size = size

        # need to deal with inconsistent autoranges
        if len(set(ar_scale)) != 1:
            self._adjust_autorange(ar_scale, ar_units)

        self._fig = self._waterfall_staging_horiz(scenarios, _stages, styles, **kwargs)
        if filename != 'none':
            save_plot(filename)

    @property
    def fig(self):
        return self._fig

    @property
    def int_threshold(self):
        """
        Useful only for horiz charts
        :return: about 0.55" in axis units
        """
        return (self._span[1] - self._span[0]) / (self._size * 1.8)

    '''
    def _waterfall_staging_vert(self, scenarios, stages, styles, aspect=0.1, panel_sep=0.75, **kwargs):
        """
        For the vertical-bar waterfall charts, we make just one axes and position the waterfalls at different x
        positions. We may try to do the same thing for horiz waterfallos if we like it better.  This is nice because
        it automatically adjusts for results with

        :param scenarios:
        :param stages:
        :param styles:
        :param aspect:
        :param panel_sep:
        :param kwargs:
        :return:
        """
        num_ax = len(self._d)
        num_steps = len(stages)
        width = num_ax * (self._size * aspect * num_steps) + (num_ax - 1) * panel_sep

        _ax_wid = self._size * aspect * num_steps / width
        _gap_wid = panel_sep / width

        fig = plt.figure(figsize=[width, self._size])
        left = 0.0

        _mn = _mx = 0.0
        axes = []
        for i in range(num_ax):
            right = left + _ax_wid
            ax = fig.add_axes([left, 0.0, _ax_wid, 1.0])
            axes.append(ax)
            self._waterfall_vert(ax, self._d[i], styles, **kwargs)
            ax.set_xticklabels(stages)
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
    '''

    def _waterfall_staging_horiz(self, scenarios, stages, styles,
                                 aspect=0.1, panel_sep=0.65,
                                 **kwargs):
        """
        Creates a figure and axes and populates them with waterfalls.
        :param scenarios:
        :param stages:
        :param styles:
        :param aspect:
        :param panel_sep:
        :param kwargs: num_format=%3.2g, bar_width=0.85
        :return:
        """
        num_ax = len(self._d)
        num_steps = len(stages)
        height = num_ax * (self._size * aspect * num_steps) + (num_ax - 1) * panel_sep

        _ax_hgt = self._size * aspect * num_steps / height
        _gap_hgt = panel_sep / height

        fig = plt.figure(figsize=[self._size, height])
        top = 1.0

        _mn = _mx = 0.0
        axes = []
        for i in range(num_ax):
            bottom = top - _ax_hgt
            ax = fig.add_axes([0.0, bottom, 1.0, _ax_hgt])
            axes.append(ax)
            self._waterfall_horiz(ax, self._d[i], styles, **kwargs)
            ax.set_yticklabels(stages)
            xticklabels = [_i.get_text() for _i in ax.get_xticklabels()]
            xticklabels[-1] += ' %s' % self._unit
            ax.set_xticklabels(xticklabels)

            xlim = ax.get_xlim()
            if xlim[0] < _mn:
                _mn = xlim[0]
            if xlim[1] > _mx:
                _mx = xlim[1]

            if scenarios[i] is not None or num_ax > 1:
                sc_name = scenarios[i]
            else:
                sc_name = ''

            if i == 0:
                ax.set_title('%s [%s]\n%s' % (self._q['Name'], self._unit, sc_name), fontsize=12)
            else:
                ax.set_title('%s' % sc_name, fontsize=12)

            top = bottom - _gap_hgt

        for ax in axes:
            ax.set_xlim([_mn, _mx])
        return fig

    def _waterfall_horiz(self, ax, data, styles, num_format='%3.2g', bar_width=0.85):
        """

        :param ax:
        :param data:
        :param styles: a list of style kwargs to add to each bar.  must include 'color' key; all others extra.
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
            style = styles[i]

            color = style['color']

            if dat < 0:
                style['color'] = _fade_color(color)

            ax.barh(center, dat, left=cum, height=bar_width, **style)
            if self.int_threshold is not None and abs(dat) > self.int_threshold:
                if sum(style['color'][:2]) < 0.6:
                    text_color = (1, 1, 1)
                else:
                    text_color = (0, 0, 0)

                # interior label
                x = cum + (dat / 2)
                ax.text(x, center, num_format % dat, ha='center', va='center', color=text_color)
            else:
                '''# end label positioning-- this is complicated!
                IF the bar is positive and the result is not too far to the right, we want the label on the right
                IF the bar is too far to the right, we want the label on the left regardless of direction
                IF the bar is too far to the left, we want the label on the right regardless of direction
                BUT if the bar is close to 0, we want it printed on the far side from the y axis, to not overwrite
                We know if we're here, the bar is short.  so we only need to think about one end.
                '''
                if cum + dat > self._span[1] - self.int_threshold:
                    # must do left: too close to right
                    if 0 < cum < self.int_threshold:
                        anchor = 'zero left'
                    else:
                        anchor = 'left'
                elif cum + dat < self._span[0] + self.int_threshold:
                    # too close to left
                    if cum < 0 and abs(cum) < self.int_threshold:
                        anchor = 'zero right'
                    else:
                        anchor = 'right'
                elif abs(cum) < self.int_threshold:
                    if cum >= 0:
                        anchor = 'right'
                    else:
                        anchor = 'left'
                else:
                    # not in a danger zone
                    if dat >= 0:
                        anchor = 'right'
                    else:
                        anchor = 'left'

                if anchor == 'left':
                    x = min([cum, cum + dat]) - _h_gap
                    ha = 'right'
                elif anchor == 'zero left':
                    x = -_h_gap
                    ha = 'right'
                elif anchor == 'zero right':
                    x = _h_gap
                    ha = 'left'
                else:
                    x = max([cum, cum + dat]) + _h_gap
                    ha = 'left'

                ax.text(x, center, num_format % dat, ha=ha, va='center')

            # connector
            if cum != 0:
                ax.plot([cum, cum], [center - 0.5*bar_width, center - 1 + 0.5*bar_width],
                        color=_conn_color, zorder=-1, linewidth=0.5)

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
        ax.plot([cum, cum], [center - 1 + 0.5*bar_width, bottom], color=_conn_color, zorder=-1, linewidth=0.5)
        ax.plot(cum, bottom - 0.4 * _low_gap, marker='v', markerfacecolor=(1, 0, 0), markeredgecolor='none',
                markersize=8)

        # x labels
        if abs(cum) > self.int_threshold and abs(mx) > self.int_threshold:
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
