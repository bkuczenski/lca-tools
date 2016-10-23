from __future__ import division
import colorsys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from textwrap import wrap


prefab_colors = ((0.1, 0.6, 0.7), (0.9, 0.3, 0.4), (0.3, 0.9, 0.6), (0.1, 0.1, 0.8), (0.4, 0.9, 0.3), (0.3, 0.4, 0.9))
net_color = (0.6, 0.6, 0.6)


def linspace(a, b, num):
    if num < 2:
        yield a
    else:
        dif = float(b - a) / (num - 1)
        for i in range(num - 1):
            yield a
            a += dif
    yield b


def color_range(num, hue, sat=0.5):
    """
    Generator for a series of num RGB tuples, derived from the color provided in hue.
    If hue is a 2-tuple, provides a gradation from [0] to [1]. Uses either hsv or hsl to rgb.
    """
    if not isinstance(hue, tuple):
        hue = (hue, hue)
    h = linspace(hue[0], hue[1], num)
    v = linspace(0.95, 0.45, num)
    for i in range(num):
        yield colorsys.hsv_to_rgb(next(h), sat, next(v))


def _label_bar(patch, value=None, label=None, valueformat='%5.3g', labelformat='%s'):
    bl = patch.get_xy()
    x = 0.5 * patch.get_width() + bl[0]
    y = 0.5 * patch.get_height() + bl[1]
    if value is not None:
        patch.axes.text(x, y, valueformat % value, ha='center', va='center')
    if label is not None:
        patch.axes.text(x, y + (0.52* patch.get_height()), labelformat % label, ha='center', va='bottom')


def _label_segment(patch, data, label, threshold):
    if abs(data) > threshold:
        _label_bar(patch, value=data, label=label)
    else:
        _label_bar(patch, label=label)


def _label_vbar(patch, value, valueformat='%6.3g', sep=0):
    (y0, y1) = patch.axes.get_ylim()
    vgap = 0.02 * (y1 - y0)
    bl = patch.get_xy()
    x = 0.5 * patch.get_width() + bl[0]

    y = bl[1] + patch.get_height() + sep + vgap

    patch.axes.text(x, y, valueformat % value, ha='center', va='bottom')


def _has_nonzero(res):
    data = sum([abs(i) for i in res.contrib_query(res.components())])
    return data != 0


def _has_pos_neg(res):
    """
    Returns true if the input contains both positive and negative data points
    :param res:
    :return:
    """
    data = res.contrib_query(res.components())
    poss = sum([k for k in data if k > 0])
    negs = sum([k for k in data if k < 0])
    if poss != 0 and negs != 0:
        return True
    return False


def save_plot(file, close_after=True):
    plt.savefig(file, format='eps', bbox_inches='tight')
    if close_after:
        plt.close()


def standard_labels(ax, stages):
    ax.set_xticks(range(len(stages)))
    if max([len(l) for l in stages]) > 12:
        labels = ['\n'.join(wrap(l, 25)) for l in stages]
        rotation = 90
    else:
        labels = stages
        rotation = 0

    ax.set_xticklabels(labels, rotation=rotation)


def stack_bar_figure(results, stages, hues=None):

    if hues is None:
        hues = [None] * len(results)

    units = [r.quantity.unit() for r in results]

    # prepare data, determine figure height
    height = 0
    data = [None] * len(results)
    for i, r in enumerate(results):
        data[i] = r.contrib_query(stages)
        height += 1.8
        if _has_pos_neg(r):
            height += 0.4

    fig = plt.figure(figsize=(8, height))
    for i, r in enumerate(results):
        ax = fig.add_subplot(len(results), 1, i + 1)

        quantity = r.quantity

        hue = hues[i]
        if hue is None:
            hue = int('%.4s' % quantity.get_uuid(), 16) / 65536

        stack_bar(ax, data[i], hue, units[i], quantity['Name'], quantity['Indicator'])

    return fig


def _stackbar_subfig_height(results):
    height = 1.3

    for r in results:
        if _has_nonzero(r):
            height += 0.8
            if _has_pos_neg(r):
                height += 0.6
    return height


def _scenario_fig_height(results, scenario=False):
    height = 0
    if scenario:
        for m, res in enumerate(results):
            height += _stackbar_subfig_height(res)
    else:
        for m, res in enumerate(results):
            height += _stackbar_subfig_height([res])
    return height


def spread_contrib_figure(results, stages, colors=None, scenarios=None, results_hi=None, results_lo=None,
                          match_y=False):
    """

    :param results: m-array of queryable results
    :param stages: k-array of stages
    :param colors: should match k
    :param scenarios: should match m (used for subtitles)
    :param results_hi: either None or an m-array of high values
    :param results_lo: either None or an m-array of low values
    :param match_y: [False] if True, set all axes to have the same [maximal] y limits
    :return:
    """
    if not isinstance(results, list):
        # assume that all the m-arrays are non-arrays
        results = [results]
        results_hi = [results_hi]
        results_lo = [results_lo]
        scenarios = [scenarios]

    if len(results) == 1:
        f = plt.figure(figsize=(6, 5))
        wid = 1

    else:
        height = 5 * round(len(results)/2)
        f = plt.figure(figsize=(12, height))
        wid = 2

    if colors is None:
        colors = (0.1, 0.6, 0.7)

    if results_hi is None:
        results_hi = [None] * len(results)

    if results_lo is None:
        results_lo = [None] * len(results)

    ax = []

    for i, result in enumerate(results):
        ax.append(f.add_subplot(len(results), wid, i + 1))
        data = result.contrib_query(stages)
        if results_hi[i] is None:
            hi = data
        else:
            hi = results_hi[i].contrib_query(stages)

        if results_lo[i] is None:
            lo = data
        else:
            lo = results_lo[i].contrib_query(stages)

        spread_bars(ax[-1], data, colors, lo=lo, hi=hi)
        standard_labels(ax[-1], stages)

        t = result.quantity['Indicator']

        if scenarios is None:
            ax[-1].set_title(t, fontsize=14)
        else:
            ax[-1].set_title('%s\n%s' % (t, scenarios[i]), fontsize=14)

    if match_y:
        y_lo = 0
        y_hi = 0
        for x in ax:
            (y0, y1) = x.get_ylim()
            if y0 < y_lo:
                y_lo = y0
            if y1 > y_hi:
                y_hi = y1
        for x in ax:
            x.set_ylim(y_lo, y_hi)


def spread_scenario_compare(ax, results, stages, colors=None, scenarios=None, results_hi=None, results_lo=None,
                            net=False, labels=False, legend=True):
    """

    :param ax: axes already created
    :param results:
    :param stages:
    :param colors:
    :param scenarios:
    :param results_hi:
    :param results_lo:
    :param net:
    :return:
    """
    barwidth = 0.8  # use most of the space for multiple scenarios

    if not isinstance(results, list):
        # assume that all the m-arrays are non-arrays
        results = [results]
        results_hi = [results_hi]
        results_lo = [results_lo]
        scenarios = [scenarios]
        barwidth = 0.65

    m = len(results)

    barwidth /= m

    x_0 = -(m + 1) * barwidth / 2

    if colors is None:
        colors = prefab_colors[:len(results)]

    if results_hi is None:
        results_hi = [None] * len(results)

    if results_lo is None:
        results_lo = [None] * len(results)

    if scenarios is None:
        scenarios = ['S%d' % (i+1) for i, _ in enumerate(results)]

    unit = list(set([(r.quantity['Indicator'], r.quantity.unit()) for r in results]))
    if len(unit) > 1:
        print('Warning: multiple units found in result sets!!')

    for i, r in enumerate(results):
        x_0 += barwidth

        data = r.contrib_query(stages)
        if net:
            data.append(r.total())

        if results_hi[i] is None:
            hi = data
        else:
            hi = results_hi[i].contrib_query(stages)
            if net:
                hi.append(results_hi[i].total())

        if results_lo[i] is None:
            lo = data
        else:
            lo = results_lo[i].contrib_query(stages)
            if net:
                lo.append(results_lo[i].total())

        mycolors = (colors[i],) * len(stages)
        if net:
            mycolors += (net_color,)

        spread_bars(ax, data, mycolors, lo=lo, hi=hi, barwidth=barwidth, x_offset=x_0, labels=labels)

    # graph general parts
    ax.plot(ax.get_xlim(), (0, 0), 'k')

    if max([len(l) for l in stages]) > 12:
        lbls = ['\n'.join(wrap(l, 25)) for l in stages]
        rotation = 90
    else:
        lbls = [l for l in stages]
        rotation = 0

    if net:
        lbls.append('NET')

    ax.set_xticks(range(len(lbls)))
    ax.set_xticklabels(lbls, rotation=rotation)
    ax.set_ylabel(unit[0][1])

    if len(scenarios) > 1 and legend:
        handles = [mpatches.Patch(color=colors[i], label=scenarios[i]) for i in range(len(scenarios))]

        ax.legend(handles=handles, loc='best')
    ax.set_title(unit[0][0], fontsize=14)


def scenario_compare_figure(results, stages, hues=None, scenarios=None):
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
            hue = int('%.4s' % quantity.get_uuid(), 16) / 65536

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

    return fig


def _one_bar(ax, pos_y, neg_y, data, hue, units, threshold):
    poss = sum([k for k in data if k > 0])
    negs = sum([k for k in data if k < 0])

    total = poss + negs

    left = 0.0
    right = 0.0

    patch_handles = []
    colors = color_range(len(data), hue)

    total_y = None  # where to put the net marker

    if poss != 0:
        if negs != 0:
            if total > 0:
                total_y = neg_y
            else:
                total_y = pos_y
        else:
            neg_y = pos_y

    else:
        if negs == 0:
            return pos_y  # nothing to do
        neg_y = pos_y

    for (i, d) in enumerate(data):
        if d > 0:
            patch = ax.barh(pos_y, d, color=next(colors), align='center', left=right, height=1)
            patch_handles.append(patch)
            _label_segment(patch[0], d, chr(ord('A') + i), threshold)
            right += d
        elif d < 0:
            left += d
            patch = ax.barh(neg_y, abs(d), color=next(colors), align='center', left=left, height=1)
            patch_handles.append(patch)
            _label_segment(patch[0], d, chr(ord('A') + i), threshold)

    if units is None:
        unitstring = ''
    else:
        unitstring = ' [%s]' % units

    if poss != 0:
        ax.text(right, pos_y, ' %6.3g%s' % (right, unitstring), fontsize=12, fontweight='bold',
                ha='left', va='center')

    if negs != 0:
        ax.text(right, neg_y, '%6.3g%s' % (left, unitstring), ha='left', va='center', fontsize=12, fontweight='bold')

    if total_y is not None:
        ax.plot(total, total_y, 'd', markersize=10)
        ax.text(total, total_y, '  %6.3g' % total, ha='left', va='center')

    return neg_y - 0.55


def stack_bar(ax, data, hue, units, title='Contribution Analysis', subtitle='by stage'):
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
    stack_bars(ax, [data], hue, [units], title=title, subtitle=subtitle)


def stack_bars(ax, series, hue, units, labels=None, title='Scenario Analysis', subtitle='by stage'):
    """

    :param ax:
    :param series: must be a list of iterables, presumably all the same length
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
    bar_skip = 1.4

    # defaults
    pos_y = .55
    btm = 0

    # make the plots
    for i, data in enumerate(series):
        neg_y = pos_y - 1
        btm = _one_bar(ax, pos_y, neg_y, data, hue, units[i], threshold)
        if labels is not None:
            ax.text(left, pos_y, '%s  ' % labels[i], ha='right', va='center', fontsize=12)
        pos_y = btm - bar_skip

    # common to full plot

    ax.text(left, 2.3, subtitle, color=(0.3, 0.3, 0.3), size='small')
    ax.text(left, 2.8, title, size='large')

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


def spread_bars(ax, data, color_gen, hi=None, lo=None, y_lim=None, barwidth=0.65, x_offset=0, labels=True, **kwargs):
    """

    :param ax:
    :param data:
    :param color_gen: iterator that yields color for data points
    :param hi: data vector for high errorbars (passed on to pyplot)
    :param lo: data vector for low errorbars (passed on to pyplot)
    :param y_lim: y axis limits, if to be specified
    :param barwidth: [0.65]
    :param x_offset: [0]
    :param labels: [True] whether to label the top of each bar
    :return:
    """
    x = [i - barwidth/2 + x_offset for i in range(len(data))]

    kwargs['ecolor'] = (0,0,0)
    kwargs['width'] = barwidth
    kwargs['color'] = [k for k in color_gen]

    if hi is None:
        hi_d = [0] * len(data)
    else:
        hi_d = [hi[i] - data[i] for i, _ in enumerate(data)]
    if lo is None:
        lo_d = [0] * len(data)
    else:
        lo_d = [data[i] - lo[i] for i, _ in enumerate(data)]

    kwargs['yerr'] = [lo_d, hi_d]

    patches = ax.bar(x, data, **kwargs)
    if labels:
        for i, patch in enumerate(patches):
            if data[i] != 0:
                _label_vbar(patch, data[i], sep=hi_d[i])

    ax.set_xlim(-0.5, len(data)-0.5)
    if y_lim is not None:
        ax.set_ylim(y_lim)

    ax.plot((-0.5, len(data)), (0, 0), 'k')


"""
people = ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H')
segments = 4

# generate some multi-dimensional data & arbitrary labels
data = 3 + 10 * np.random.rand(segments, len(people))
percentages = (np.random.randint(5,20, (len(people), segments)))
y_pos = np.arange(len(people))

fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111)

colors = 'rgbwmc'
patch_handles = []
left = np.zeros(len(people))  # left alignment of data starts at zero
for i, d in enumerate(data):
    patch_handles.append(ax.barh(y_pos, d,
                                 color=colors[i % len(colors)], align='center',
                                 left=left))
    # accumulate the left-hand offsets
    left += d

# go through all of the bar segments and annotate
for j in range(len(patch_handles)):
    for i, patch in enumerate(patch_handles[j].get_children()):
        bl = patch.get_xy()
        x = 0.5*patch.get_width() + bl[0]
        y = 0.5*patch.get_height() + bl[1]
        ax.text(x, y, "%d%%" % (percentages[i, j]), ha='center')

ax.set_yticks(y_pos)
ax.set_yticklabels(people)
ax.set_xlabel('Distance')

plt.show()
"""
