from __future__ import division
import colorsys
import matplotlib.pyplot as plt


def linspace(a, b, num):
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
        patch.axes.text(x, y + patch.get_height(), labelformat % label, ha='center', va='center')


def _label_segment(patch, data, label, threshold):
    if data > threshold:
        _label_bar(patch, value=data, label=label)
    else:
        _label_bar(patch, label=label)


def _has_pos_neg(data):
    """
    Returns true if the input contains both positive and negative data points
    :param data:
    :return:
    """
    poss = sum([k for k in data if k > 0])
    negs = sum([k for k in data if k < 0])
    if poss != 0 and negs != 0:
        return True
    return False


def save_plot(file):
    plt.savefig(file, format='eps', bbox_inches='tight')


def stack_bar_figure(results, stages, hues=None, units=None):

    if hues is None:
        hues = [None] * len(results)

    if units is None:
        units = [r.quantity.unit() for r in results]

    # prepare data, determine figure height
    height = 0
    data = [None] * len(results)
    for i, r in enumerate(results):
        data[i] = r.contrib_query(stages)
        height += 1.8
        if _has_pos_neg(data[i]):
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


def stack_bar(ax, data, hue, units, title='Contribution Analysis', subtitle='by stage'):
    """
    Draws a stack bar chart on the existing axes.
    ax = axes
    data = ordered data corresponding to the above contributions
    hue = float [0..1] used as the base color in shading the bars
          hue can also be (hue1, hue2), in which case the bars follow a gradient from hue1 to hue2
    units = appended to total in square brackets if present
    title, subtitle- drawn on graph

    If several stack bar charts are to be shown side by side, they should all have the data ordered the same.
    """
    poss = sum([k for k in data if k > 0])
    negs = sum([k for k in data if k < 0])
    total = poss + negs
    data_range = poss - negs

    threshold = 0.07 * data_range
    bar_height = 20

    # defaults
    pos_y = (1.05 * bar_height) / 2
    neg_y = pos_y - bar_height
    total_y = None  # where to put the net marker

    if poss != 0:
        if negs != 0:
            if total > 0:
                total_y = neg_y
            else:
                total_y = pos_y
        else:
            neg_y = 0

    else:
        if negs == 0:
            return None  # nothing to do
        neg_y = 0

    left = 0.0
    right = 0.0
    patch_handles = []
    colors = color_range(len(data), hue)

    for (i, d) in enumerate(data):
        if d > 0:
            patch = ax.barh(pos_y, d, color=next(colors), align='center', left=right, height=bar_height)
            patch_handles.append(patch)
            _label_segment(patch[0], d, chr(ord('A') + i), threshold)
            right += d
        elif d < 0:
            left += d
            patch = ax.barh(neg_y, abs(d), color=next(colors), align='center', left=left, height=bar_height)
            patch_handles.append(patch)
            _label_segment(patch[0], d, chr(ord('A') + i), threshold)

    ax.plot((0, 0), (neg_y, bar_height), 'k-')  # line at origin

    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)

    ax.text(left, 2.3 * bar_height, subtitle, color=(0.3, 0.3, 0.3), size='small')
    ax.text(left, 2.8 * bar_height, title, size='large')

    ax.set_yticks([])
    # ax.set_position([0.125, 0.125, 0.7, 0.9])
    ax.xaxis.set_ticks_position('bottom')
    ax.set_xlim(left * 1.08, right * 1.08)
    ax.set_ylim(neg_y - (0.5 * bar_height), 3.6 * bar_height)

    if units is None:
        unitstring = ''
    else:
        unitstring = ' [%s]' % units

    if poss != 0:
        ax.text(right, pos_y, ' %6.3g%s' % (right, unitstring), fontsize=12, fontweight='bold',
                ha='left', va='center')

    if negs != 0:
        ax.text(right, neg_y, '%6.3g%s' % (left, unitstring), ha='left', fontsize=12, fontweight='bold')

    if total_y is not None:
        ax.plot(total, total_y, 'd')


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
