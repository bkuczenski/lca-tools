import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from .base import standard_labels, label_vbar, prefab_colors, net_color, wrap


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

    kwargs['ecolor'] = (0, 0, 0)
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
                label_vbar(patch, data[i], sep=hi_d[i])

    ax.set_xlim(-0.5, len(data)-0.5)
    if y_lim is not None:
        ax.set_ylim(y_lim)

    ax.plot((-0.5, len(data)), (0, 0), 'k')
    # _open_ylims(ax)


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

        spread_bars(ax, data, mycolors, lo=lo, hi=hi, barwidth=barwidth, x_offset=x_0, labels=labels, align='edge')

    # graph general parts
    ax.plot(ax.get_xlim(), (0, 0), 'k', linewidth=0.8)

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
