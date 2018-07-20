"""
Combination charts.
 - Horizontal aspect;
 - Positive + Negative bars + net bar;
 - multi-impact;
 - scenario comparisons.

These are expert charts. Nonexpert users have reported their "brain entirely shuts down" when presented with
more elaborate instances of these charts.
"""

import matplotlib.pyplot as plt
from .base import color_range, label_segment, has_pos_neg, has_nonzero, save_plot, hue_from_string


def _one_bar(ax, pos_y, neg_y, data, hue, units, threshold, legend_labels=None):
    """

    :param ax:
    :param pos_y:
    :param neg_y:
    :param data:
    :param hue: if hue is a float-- it's a gradient specifier
        if hue is a 2-tuple-- it's a gradient specifier with different-colored endpoints
        if hue is a tuple of 3-tuples-- treat it AS a color specifier
    :param units:
    :param threshold:
    :param legend_labels: if present, use as an iterable to assign labels to bars for legend purposes
    :return:
    """
    poss = sum([k for k in data if k > 0])
    negs = sum([k for k in data if k < 0])

    total = poss + negs

    left = 0.0
    right = 0.0

    if isinstance(hue, float):
        colors = color_range(len(data), hue)
    elif len(hue) == 2 and isinstance(hue[0], float):
        colors = color_range(len(data), hue)
    else:
        for k in hue:
            assert len(k) == 3, 'invalid color specifier'
        colors = (k for k in hue)

    total_y = None  # where to put the net marker
    total_ha = 'left'  #

    # position the net total indicator
    if poss != 0:
        if negs != 0:
            if total > 0:
                # total indicator is aligned with neg
                total_y = neg_y  # - 0.1
                if total > poss / 2:
                    total_ha = 'right'
            else:
                total_y = pos_y  # + 0.1
                if total > negs / 2:
                    total_ha = 'right'
        else:
            neg_y = pos_y
    else:
        if negs == 0:
            return pos_y  # nothing to do
        neg_y = pos_y

    # print('NY: %f  PY: %f  TY: %f  ha: %s' % (neg_y, pos_y, total_y, total_ha))

    # sparsify labels-- don't pile labels up if there are multiple tiny segments. see _label_segment
    sparse_label_flag_pos = True
    sparse_label_flag_neg = True

    for (i, d) in enumerate(data):
        color = next(colors)
        if legend_labels is None:
            _thelabel = chr(ord('A') + i)
        else:
            _thelabel = ''
        if d > 0:
            patch = ax.barh(pos_y, d, color=color, align='center', left=right, height=1)
            sparse_label_flag_pos = label_segment(patch[0], d, _thelabel, threshold, sparse_label_flag_pos)
            right += d
        elif d < 0:
            left += d
            patch = ax.barh(neg_y, abs(d), color=color, align='center', left=left, height=1)
            sparse_label_flag_neg = label_segment(patch[0], d, _thelabel, threshold, sparse_label_flag_neg)
        else:  # d == 0
            continue
        if legend_labels is not None:
            try:
                patch.set_label(legend_labels[i])
            except IndexError:
                pass

    if units is None:
        unitstring = ''
    else:
        unitstring = ' [%s]' % units

    if poss != 0:
        ax.text(right, pos_y, ' %6.3g%s' % (right, unitstring), fontsize=12, fontweight='bold',
                ha='left', va='center')

    if negs != 0:
        ax.text(right, neg_y, ' %6.3g%s' % (left, unitstring), ha='left', va='center', fontsize=12, fontweight='bold')

    if total_y is not None:
        ax.plot(total, total_y, 'd', markersize=10, markeredgecolor='k', markeredgewidth=0.5)
        ax.barh(total_y, total, color=(0.74, 0.74, 0.74), height=0.65, edgecolor='none')
        ax.text(total, total_y, '   %6.3g   ' % total, ha=total_ha, va='center')

    return neg_y - 0.55


def stack_bar(ax, data, hue, units, title='Contribution Analysis', subtitle='by stage', **kwargs):
    """
    Draws a single stack bar chart on the existing axes.
    ax = axes
    data = ordered data corresponding to the above contributions
    hue = float [0..1] used as the base color in shading the bars
          hue can also be (hue1, hue2), in which case the bars follow a gradient from hue1 to hue2
    units = appended to total in square brackets if present
    title, subtitle- drawn on graph

    If several stack bar charts are to be shown side by side, they should all have the data ordered the same.
    """
    stack_bars(ax, [data], hue, [units], title=title, subtitle=subtitle, **kwargs)


def stack_bars(ax, series, hue, units, labels=None, title='Scenario Analysis', subtitle='by stage', **kwargs):
    """

    :param ax:
    :param series: must be a list of iterables of numerical data, presumably all the same length
    :param hue:
    :param units:
    :param labels: None, or a list of text strings to label each bar set
    :param title:
    :param subtitle:
    :return:
    """
    # first, determine left and right ranges
    left = 0.0
    right = 0.0
    for data in series:
        poss = sum([k for k in data if k > 0])
        negs = sum([k for k in data if k < 0])

        if poss > right:
            right = poss
        if negs < left:
            left = negs

    data_range = right - left
    threshold = 0.07 * data_range
    top = 1.1
    bar_skip = 1.6

    # defaults
    pos_y = .55
    btm = 0

    # make the plots
    for i, data in enumerate(series):
        neg_y = pos_y - 1
        btm = _one_bar(ax, pos_y, neg_y, data, hue, units[i], threshold, **kwargs)
        if labels is not None:
            ax.text(left, pos_y, '%s  ' % labels[i], ha='right', va='center', fontsize=12)
        pos_y = btm - bar_skip

    # common to full plot

    ax.text(0, 2.3, subtitle, color=(0.3, 0.3, 0.3), size='small')
    ax.text(0, 2.8, title, size='large')

    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)

    ax.set_yticks([])
    if data_range == 0:
        ax.set_xticks([])
        ax.set_xlim(left, left + 1)
        ax.text(0, 1.1, 'No impacts', ha='left', va='bottom')
        ax.set_ylim(1, 3.6)
    else:
        ax.plot((0, 0), (btm, top), 'k-')  # line at origin

        ax.xaxis.set_ticks_position('bottom')
        ax.set_xlim(left - 0.02 * data_range, right + 0.08 * data_range)
        ax.set_ylim(btm, 3.6)


def stack_bar_figure(results, stages, hues=None, color_dict=None, legend=False):
    """
    Create a stack bar figure.
    :param results:
    :param stages:
    :param hues: by default, each stack bar is a gradient of a single hue- supply an array of hues matching length of
     results array, or else hue is generated automatically from quantity uuid / ext ref
    :param color_dict: optionally supply a dict mapping stage name to rgb triple, instead of hue gradient
    :param legend: [False] if true, draw a legend below the bottom bar
    :return:
    """

    if hues is None:
        hues = [None] * len(results)

    units = [r.quantity.unit() for r in results]

    # prepare data, determine figure height
    height = 0
    data = [None] * len(results)
    for i, r in enumerate(results):
        data[i] = r.contrib_query(stages)
        height += 1.8
        if has_pos_neg(r):
            height += 0.4

    fig = plt.figure(figsize=(8, height))

    if color_dict is None:
        colors = None
    else:
        colors = [color_dict[s] for s in stages]

    ax = []

    for i, r in enumerate(results):
        ax.append(fig.add_subplot(len(results), 1, i + 1))

        quantity = r.quantity

        if colors is None:
            hue = hues[i]
            if hue is None:
                hue = hue_from_string('%.4s' % quantity.uuid)
        else:
            hue = colors

        stack_bar(ax[i], data[i], hue, units[i], quantity['Name'], quantity['Indicator'], legend_labels=stages)

    def _zero_pos(_a):
        _xl = _a.get_xlim()
        return max(_xl) / (max(_xl) - min(_xl))

    _min = min(_zero_pos(a) for a in ax)

    for a in ax:
        """
        need to adjust axes limits to align zeros
        """
        _xl = a.get_xlim()
        _low = max(_xl) * (_min - 1.0) / _min
        a.set_xlim(_low, max(_xl))

    if legend:
        ax[0].legend(loc='upper right', bbox_to_anchor=(0, 1))

    return fig


def _stackbar_subfig_height(results):
    """
    returns the height of
    :param results: an array of LciaResult objects
    :return:
    """
    height = 0

    for r in results:
        if has_nonzero(r):
            height += 0.9
            if has_pos_neg(r):
                height += 0.7
        else:
            height += 0.6
    return height


def _scenario_fig_height(results, scenario=False):
    height = 0
    if scenario:
        for m, res in enumerate(results):
            height += _stackbar_subfig_height(res)
        height += len(results[0]) - 1
    else:
        for m, res in enumerate(results):
            height += _stackbar_subfig_height([res])
            height += 1

    print('### Height is: %f' % height)
    return height


def scenario_compare_figure(results, stages, hues=None, scenarios=None, savefile=None):
    """
    A bit of a swiss army knife.

    if scenarios is None, results is an n-array of LciaResult objects.  Creates an n-subplot figure of
    stacked pos/neg bars, annotated by reference to k ordered stages.

     if scenarios is not none, it must be an m-array of scenario names or labels.  results is interpreted as
     an m-array of n-arrays of LciaResult objects. Creates an n-subplot figure of m-stacked pos/neg bars, each
     featuring k stages

    :param results: either a n-array or a k-by-n array of results
    :param stages: k query terms to results
    :param hues: for n quantities represented (quick-compute from UUID)
    :param scenarios:
    :param savefile: if present, save the fig to the specified file
    :return:
    """

    if scenarios is None:
        height = _scenario_fig_height(results, scenario=False)
        fig = plt.figure(figsize=(8, height))
        quantities = [r.quantity for r in results]

    else:
        height = _scenario_fig_height(results, scenario=True)
        fig = plt.figure(figsize=(8, height))
        quantities = [r.quantity for r in results[0]]

    if hues is None:
        hues = [None] * len(quantities)

    for n, quantity in enumerate(quantities):
        hue = hues[n]
        ax = fig.add_subplot(len(quantities), 1, n + 1)

        if hue is None:
            hue = hue_from_string('%.4s' % quantity.uuid)

        if scenarios is None:
            series = results[n].contrib_query(stages)
            stack_bars(ax, [series], hue, [quantity.unit()],
                       title=quantity['Name'], subtitle=quantity['Indicator'])

        else:
            series = [r[n].contrib_query(stages) for r in results]
            units = [r[n].quantity.unit() for r in results]

            stack_bars(ax, series, hue, units, labels=scenarios,
                       title=quantity['Name'], subtitle=quantity['Indicator'])

        """
        if n == 0:
            colors = [k for k in color_range(len(stages), hue)]
            handles = [mpatches.Patch(color=colors[i], label='%s -- %s' % (chr(ord('A') + i), k))
                       for i, k in enumerate(stages)]

            ax.legend(handles=handles, bbox_to_anchor=(1.75, 0.75))
            """
    leg = 'Stages: \n' + '\n'.join(['%s -- %s' % (chr(ord('A') + i), k) for i, k in enumerate(stages)])
    print(leg)

    if savefile is not None:
        save_plot(savefile, close_after=False)

    return fig
