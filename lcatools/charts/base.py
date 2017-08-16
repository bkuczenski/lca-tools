from __future__ import division
import colorsys
import matplotlib as mpl
import matplotlib.pyplot as plt
from textwrap import wrap

# grumble something about breaking styles for no good reason with 2.0.0
mpl.rcParams['patch.force_edgecolor'] = True
mpl.rcParams['errorbar.capsize'] = 3
mpl.rcParams['grid.color'] = 'k'
mpl.rcParams['grid.linestyle'] = ':'
mpl.rcParams['grid.linewidth'] = 0.5
mpl.rcParams['lines.linewidth'] = 1.0
mpl.rcParams['axes.autolimit_mode'] = 'round_numbers'
mpl.rcParams['axes.xmargin'] = 0
mpl.rcParams['axes.ymargin'] = 0


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


def _label_bar(patch, value=None, label=None, valueformat='%4.3g', labelformat='%s'):
    bl = patch.get_xy()
    x = 0.5 * patch.get_width() + bl[0]
    y = 0.5 * patch.get_height() + bl[1]
    if value is not None:
        patch.axes.text(x, y, valueformat % value, ha='center', va='center')
    if label is not None:
        patch.axes.text(x, y + (0.52 * patch.get_height()), labelformat % label, ha='center', va='bottom')


def label_segment(patch, data, label, threshold, sparse_flag):
    """
    Label the segment if it's big enough. The threshold is the minimum size for a value statement; one-quarter the
    threshold is the minimum size for a bar label. Segments smaller than one-quarter the threshold are considered
    non-sparse, and if there is more than one non-sparse segment in succession, only the first will be labeled.
    A single non-sparse segment resets the labeling.  Positive and negative sparse_flags should be tracked separately.
    :param patch:
    :param data:
    :param label:
    :param threshold:
    :param sparse_flag:
    :return:
    """
    if abs(data) > threshold:
        _label_bar(patch, value=data, label=label)
    if sparse_flag or abs(data) > (threshold / 4):
        _label_bar(patch, label=label)
    if abs(data) < (threshold / 4):
        return False
    return True


def label_vbar(patch, value, valueformat='%6.3g', sep=0):
    (y0, y1) = patch.axes.get_ylim()
    vgap = 0.02 * (y1 - y0)
    bl = patch.get_xy()
    x = 0.5 * patch.get_width() + bl[0]

    y = bl[1] + patch.get_height() + sep + vgap

    patch.axes.text(x, y, valueformat % value, ha='center', va='bottom')


def has_nonzero(res):
    data = sum([abs(i) for i in res.contrib_query()])
    return data != 0


def has_pos_neg(res):
    """
    Returns true if the input contains both positive and negative data points
    :param res:
    :return:
    """
    data = res.contrib_query()
    try:
        poss = next(k for k in data if k > 0)
        negs = next(k for k in data if k < 0)
    except StopIteration:
        return False
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


def _open_ylims(ax, margin=0.1):
    """
    workaround for buggy Axes.margins() (https://github.com/matplotlib/matplotlib/pull/7995 and others)
    :param ax:
    :param margin:
    :return:
    """
    bottom, top = ax.get_ylim()
    yr = top - bottom
    if bottom < 0 < top:
        bottom -= margin * yr
        top += margin * yr
        ax.set_ylim(bottom=bottom, top=top)


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
