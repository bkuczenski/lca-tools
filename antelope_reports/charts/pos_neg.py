"""
Positive-negative chart.  Draws a vertically-oriented "waterfall-lite" chart with one bar pointing up containing
all the positive segments, and another bar pointing down containing all the negative segments, so that the negative
bar ends at the net result, which is annotated.
"""

import matplotlib as mpl
import matplotlib.pyplot as plt


from .base import save_plot, net_color, open_ylims, standard_labels
from .waterfall import random_color
from lcatools.autorange import AutoRange


mpl.rcParams['patch.force_edgecolor'] = True
mpl.rcParams['errorbar.capsize'] = 3
mpl.rcParams['grid.color'] = 'k'
mpl.rcParams['grid.linestyle'] = ':'
mpl.rcParams['grid.linewidth'] = 0.5
mpl.rcParams['lines.linewidth'] = 1.0
mpl.rcParams['axes.autolimit_mode'] = 'round_numbers'
mpl.rcParams['axes.xmargin'] = 0
mpl.rcParams['axes.ymargin'] = 0


class PosNegChart(object):
    """
    A PosNeg Chart draws the sum of forward and avoided burdens for each result object, together on the same
    axes, with an annotated net total.
    """

    @property
    def _tgap(self):
        """
        Useful only for vertical bars.  We want to add 3pt, which is 1/18in.  span / size" = x / 1/18"
        :return:
        """
        return (self._span[1] - self._span[0]) / (self._size * 18)

    def __init__(self, *args, color=None, horiz=False, size=4, aspect=0.4, bar_width=0.28, filename=None,
                 num_format='%3.2g', autorange=False, legend=True):
        """
        aspect reports the aspect ratio of a single chart.  aspect + bar_width together determine the aspect
        ratio of multi-arg charts.

        :param args:
        :param color:
        :param horiz:
        :param size:
        :param aspect:
        :param bar_width:
        :param filename:
        :param num_format:
        :param autorange:
        """
        self._bw = bar_width
        self._size = size

        qty = args[0].quantity
        self._color = color or random_color(qty.uuid)

        if filename is None:
            filename = 'pos_neg_%.3s.eps' % qty.uuid

        self._pos = []
        self._neg = []
        self._idx = []

        self._pos_handle = None
        self._neg_handle = None  # for legend

        ptr = 0.0
        for i, arg in enumerate(args):
            _pos = 0.0
            _neg = 0.0
            for c in arg.keys():
                val = arg[c].cumulative_result
                if val > 0:
                    _pos += val
                else:
                    _neg += val

            self._pos.append(_pos)
            self._neg.append(_neg)
            self._idx.append(ptr)

            if _neg != 0:
                ptr += 2 * bar_width

            ptr += (1 - 2 * bar_width)

        self._span = [min(self._neg), max(self._pos)]

        if autorange:
            a = AutoRange(self._span[1] - self._span[0])
            self._ar_scale = a.scale
            self._unit = a.adj_unit(qty.unit())
        else:
            self._ar_scale = 1.0
            self._unit = qty.unit()

        cross = size * aspect * (ptr + bar_width)
        if horiz:
            fig = plt.figure(figsize=[self._size, cross])
        else:
            fig = plt.figure(figsize=[cross, self._size])

        ax = fig.add_axes([0, 0, 1.0, 1.0])

        for i, arg in enumerate(args):
            if horiz:
                self._pos_neg_horiz(ax, i)
            else:
                self._pos_neg_vert(ax, i, num_format=num_format)
                standard_labels(ax, [arg.scenario for arg in args], ticks=self._idx, rotate=False, width=22)

                ax.spines['top'].set_visible(False)
                ax.spines['bottom'].set_visible(False)
                ax.spines['left'].set_visible(True)
                ax.spines['left'].set_linewidth(2)
                ax.spines['right'].set_visible(False)

        if horiz:
            pass
        else:
            ax.plot(ax.get_xlim(), [0, 0], 'k', linewidth=2, zorder=-1)
            open_ylims(ax, margin=0.05)
            ax.set_ylabel(self._unit)

        if legend and self._neg_handle is not None:
            ax.legend((self._pos_handle, self._neg_handle), ('Impacts', 'Avoided'))

        ax.set_title(qty['Name'])
        save_plot(filename)

    def _pos_neg_horiz(self, ax, i):
        pass

    def _pos_neg_vert(self, ax, i, num_format='%3.2g'):
        pos = self._pos[i] * self._ar_scale
        neg = self._neg[i] * self._ar_scale
        x = self._idx[i]

        h = ax.bar(x, pos, align='center', width=0.85 * self._bw, color=self._color)
        if self._pos_handle is None:
            self._pos_handle = h
        if neg != 0:
            ax.text(x + 0.5 * self._bw, pos + self._tgap, num_format % pos, ha='center', va='bottom')
            x += self._bw
            h = ax.bar(x, neg, bottom=pos, width=0.62 * self._bw, align='center', color=net_color,
                       linewidth=0)
            if self._neg_handle is None:
                self._neg_handle = h
            # edge line
            ax.plot([x, x - self._bw], [pos, pos], color=(0.3, 0.3, 0.3), zorder=-1, linewidth=0.5)

            x += self._bw
            tot = pos + neg
            ax.plot([x, x], [0, tot], color='k', marker='_')
            ax.text(x, 0.5 * tot, num_format % tot, ha='center', va='center',
                    bbox=dict(boxstyle='square,pad=0.02', fc='w', ec='none'))
            ax.plot([x, x + self._bw], [0, 0], linewidth=0)

            # edge line
            ax.plot([x, x - self._bw], [tot, tot], color=(0.3, 0.3, 0.3), zorder=-1, linewidth=0.5)
        else:
            ax.text(x, pos, '%3.2g' % pos)
