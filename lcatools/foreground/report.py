"""
This file is used to cast a fragment to TeX.  The output is a TeX file in an include folder which draws
a fragment as a tree diagram using PSTricks.

I guess for research on this, I should start by actually drawing a fragment by hand
"""
import os
import re
from lcatools.exchanges import comp_dir
from lcatools.charts import scenario_compare_figure, save_plot


def _grab_stages(results):
    stages = set()
    for r in results:
        stages = stages.union(r.components())
    return list(stages)


"""
drawing 'primitives'
"""


def stage_name_table(stages):
    out = '\\scriptsize\n\\begin{tabular}[b]{c@{ -- }l}\n'
    for i, k in enumerate(stages):
        out += '%s & %s\\\\\n' % (chr(ord('A') + i), k)
    out += '\\end{tabular}\n'
    return out


def fragment_header(frag, scenario=None):
    return '{\\Large \\texttt{%.5s}}\\subsection{~-- %s}\n{\\large %s: %g %s %s}\\\\[8pt]\n%s\n' % (
        frag.get_uuid(), frag['Name'], comp_dir(frag.direction), frag.exchange_value(scenario),
        frag.flow.unit(),
        frag.flow['Name'], frag['Comment']
    )


def frag_drawing_opener(max_size):
    return '''
\\begin{pspicture}(0,1)(17,%f)\\showgrid
  \\small\\sffamily
  \\rput(0,0.5){
    \\psset{xunit=1.2cm,yunit=1cm,style=standard}''' % (-max_size)


def frag_drawing_closer():
    return '  }\n\\end{pspicture}\n'


def frag_pnodes(coords):
    pnodes = ''
    for c in coords:
        pnodes += '\\pnode(%f,%f){px%s}\n' % (c[1], -c[2], c[0])
    return pnodes


def cutoff_box(uid):
    return '\\rput(px%.5s){\\rput(1,0){\\rnode[style=phantom]{nx%.5s}{CUTOFF}}}' % (uid, uid)


def bg_box(uid):
    return '\\rput(px%.5s){\\pnode(1.1,0){nx%.5s}\\psframe*(1,-0.25)(1.1,0.25)}' % (uid, uid)


def subfrag_box(uid, tgt_id):
    return ('\\rput(px%.5s){\\rput(1.25,0){\\nodebox{nx%.5s}{style=process,style=dashed}'
            '{1.15cm}{0.6cm}{\\texttt{%.5s}}}}' % (uid, uid, tgt_id))


def fg_box(uid):
    return '\\rput(px%.5s){\\rput(1,0){\\circlenode{nx%.5s}{}}}' % (uid, uid)


def io_box(uid):
    return '\\rput(px%.5s){\\pnode(1,0){nx%.5s}}\\psdots[dotsize=4pt](nx%.5s)' % (uid, uid, uid)


def process_box(uid):
    return '\\rput(px%.5s){\\rput(1,0){\\nodebox{nx%.5s}{style=process}{0.8cm}{0.2cm}{}}}' % (uid, uid)


class TeXAuthor(object):
    """
    We create a TeXAuthor object to write TeX files about fragments.

    """
    @property
    def img_folder(self):
        return os.path.join(self.folder, 'img')

    @staticmethod
    def _img_fname(frag):
        return '%s_img.eps' % frag.get_uuid()

    @staticmethod
    def _stg_fname(frag):
        return '%s_stages' % frag.get_uuid()

    @property
    def _wrapper_fname(self):
        return os.path.join(self.folder, 'wrapper.tex')

    def _read_wrapper(self):
        with open(self._wrapper_fname) as fp:
            return fp.read().splitlines()

    def img_rel_path(self, frag):
        return os.path.join(self.img_folder, self._img_fname(frag))

    def stg_rel_path(self, frag):
        return os.path.join(self.img_folder, self._stg_fname(frag))

    def _write_stage_names(self, frag, stages):
        out = stage_name_table(stages)
        with open(self.stg_rel_path(frag), 'w') as fp:
            fp.write(out)

    @staticmethod
    def _tex_sanitize(tex):
        tex = re.sub('%', '\\%', tex)
        # tex = re.sub('_', '\\\\textunderscore', tex)  # this doesn't work bc filenames have underscores
        return tex

    def __init__(self, folder, overwrite=True):
        """
        This function

        :param folder:
        :param overwrite: [True] whether to overwrite existing files in the folder
        """
        self.folder = folder
        self.overwrite = overwrite
        if os.path.exists(folder):
            if not os.path.isdir(folder):
                raise ValueError('Path is not a directory.')
        else:
            os.makedirs(folder)
        if not os.path.exists(self.img_folder):
            os.makedirs(self.img_folder)

        self._file_list = []
        if os.path.exists(self._wrapper_fname):
            for file in self._read_wrapper():
                try:
                    fname = re.search('\\\input\{(.*/)?(.*).tex\}', file).group(2)
                except AttributeError:
                    continue
                if fname not in self._file_list:
                    self._file_list.append(fname)

    def frag_layout_recurse(self, fragment, level=0, depth=0, scenario=None):
        """
        This pass creates a list of coordinates for pnode declarations for the reference points of all fragment flows.
        returns a 3-tuple of (pnode name, x-coord, y-coord)

        At the end of it, coords[-1][2] is the vertical position of the last child

        Each pnode's name is equal to the first 5 characters of the fragment's uuid, same as in the drawing.
        :param fragment:
        :param level: [0] vertical traversal height, used for positioning the pnodes
        :param depth: [0] horizontal traversal depth
        :return:
        """
        coords = [('%.5s' % fragment.get_uuid(), depth, level)]

        children = [c for c in fragment.child_flows(fragment)]
        if len(children) > 0:
            # setup coords for first child
            depth += 1.5
            if fragment.term.is_frag and not fragment.term.is_bg and not fragment.term.is_fg:
                depth += .5
            level += 1
            for c in sorted(children, key=lambda x: (x['StageName'], not x.term.is_null, x.term.is_bg)):
                if c.exchange_value(scenario, observed=True) == 0:
                    continue
                if c.term.is_frag and not c.term.is_bg and not fragment.term.is_fg:
                    level += .25
                coords.extend(self.frag_layout_recurse(c, level=level, depth=depth))
                level = coords[-1][2]
                # setup coords for next child -- background nodes are smaller
                if c.term.is_bg or c.term.is_fg or c.term.is_null:
                    level += 0.7
                else:
                    level += 1

        return coords

    def frag_traversal_entry(self, fragment, scenario=None):
        return self.frag_layout_traverse(fragment, 1.0, scenario=scenario, first=True)

    def frag_layout_traverse(self, fragment, node_weight, scenario=None, first=False, parbox_width=15):
        """
        This actually draws the fragment components
        :param fragment:
        :param node_weight:
        :param scenario:
        :param first: [False] whether to suppress printing exchange weight
        :param parbox_width: [14cm] width of right-hand text box
        :return:
        """
        subfrag_scale = 1.0
        if not first:
            node_weight *= fragment.exchange_value(scenario, observed=True)
        if node_weight == 0:
            return '\n', '\n'
        if fragment.term.is_frag:
            if fragment.term.is_fg:
                # foreground
                boxes = fg_box(fragment.get_uuid())
                frag_name = fragment['Name']
            elif fragment.term.is_bg:
                if fragment.term.term_node.term.is_null:
                    # cutoff
                    boxes = cutoff_box(fragment.get_uuid())
                else:
                    # background
                    boxes = bg_box(fragment.get_uuid())
                frag_name = fragment.term.term_node['Name']
            else:
                # subfragment
                boxes = subfrag_box(fragment.get_uuid(), fragment.term.term_node.get_uuid())
                subfrag_scale = 1.0 / fragment.term.term_node.exchange_value(scenario, observed=True)
                print('subfrag scaling: %g' % subfrag_scale)
                frag_name = fragment.term.term_node['Name']
                '''
                exchs = [x for x in fragment.get_fragment_inventory(scenario=scenario, scale=node_weight)]
                if len(exchs) > 0:
                    frag_name += '\\\\\n{\scriptsize\n'
                    for x in exchs:
                        frag_name += '%6s: %6.3g %s\\\\' % (x.direction, x.value, x.flow['Name'])
                    frag_name += '}'
                '''
        elif fragment.term.is_null:
            # I/O
            boxes = io_box(fragment.get_uuid())
            frag_name = '%s: %s' % (fragment.direction, fragment.flow['Name'])
        else:
            # process
            boxes = process_box(fragment.get_uuid())
            frag_name = fragment.term.term_node['Name']
            subfrag_scale = fragment.term.node_weight_multiplier
        boxes += '\n\\rput[l]([angle=0,nodesep=6pt]nx%.5s){\parbox{%fcm}{\\raggedright %s}}' % (fragment.get_uuid(),
                                                                                                parbox_width, frag_name)

        if fragment.direction == 'Input':
            arrows = '\\ncline{->}{nx%.5s}{px%.5s}' % (fragment.get_uuid(), fragment.get_uuid())
        else:
            arrows = '\\ncline{<-}{nx%.5s}{px%.5s}' % (fragment.get_uuid(), fragment.get_uuid())
        if not first:
            arrows += '\n\\bput(0.78){\\parbox{2cm}{\\centering \\scriptsize %.4g %s}}' % (node_weight,
                                                                                           fragment.flow.unit())

        children = [c for c in fragment.child_flows(fragment)]
        if len(children) > 0:
            parbox_width -= 2

            for c in children:
                arrows += '\n\\ncangle[angleA=-90,angleB=180,framearc=0]{nx%.5s}{px%.5s}' % (fragment.get_uuid(),
                                                                                             c.get_uuid())
                arrows += '\n'
                boxes += '\n'
                bplus, aplus = self.frag_layout_traverse(c, node_weight * subfrag_scale,
                                                         scenario=scenario, parbox_width=parbox_width)

                boxes += bplus
                arrows += aplus

        arrows += '\n'
        boxes += '\n'
        return boxes, arrows

    def contrib_chart(self, frag, results, stages=None, **kwargs):
        """
        Creates a chart and saves it in the image directory. The file name is frag.get_uuid() (the only use of
        the frag argument in the function)
        :param frag: fragment to which the chart pertains
        :param results: a LIST of LciaResult instances (if LciaResults dict, it gets converted to a list)
        :param stages: to be queried
        :return:
        """
        if not isinstance(results, list):
            results = results.to_list()

        if stages is None:
            stages = _grab_stages(results)

        fig = scenario_compare_figure(results, stages, **kwargs)
        save_plot(os.path.join(self.img_folder, self._img_fname(frag)))
        self._write_stage_names(frag, stages)
        return fig

    def frag_chart(self, frag):
        return '''
\\begin{minipage}{\\textwidth}
{\\pnode(12,-1){pLegend}
\\large Contribution Analysis}

\\includegraphics[width=12cm]{%s}
\\rput[tl](pLegend){\\parbox{5cm}{\\raggedright Stages\\\\
{\\scriptsize \\input{%s}}}}
\\end{minipage}
''' % (self.img_rel_path(frag), self.stg_rel_path(frag))

    @staticmethod
    def contrib_table(results, stages=None):
        """
        Tabular version of contrib chart.
        :param results:
        :param stages:
        :return:
        """
        if not isinstance(results, list):
            results = results.to_list()

        if stages is None:
            stages = _grab_stages(results)

        tab_lf = '\\\\'

        chart = '\n\\begin{tabularx}{\\textwidth}{|X|%s}\n\\hline' % ('r|' * len(results))
        chart += '\\rule[-4pt]{0pt}{16pt}\\textbf{Stage} '
        for i, j in enumerate(results):
            chart += ' & \\textbf{%.10s} ' % j.quantity['Indicator']

        chart += '%s \n ' % tab_lf
        for i, j in enumerate(results):
            chart += ' &  %s ' % j.quantity.unit()

        chart += '%s \\hline\n' % tab_lf

        for i, s in enumerate(stages):
            chart += '%s -- %s' % (chr(ord('A') + i), s)

            for r in results:
                d = sum([q for q in r.contrib_query([s])])
                if abs(d) > 0.1 * r.range():
                    chart += ' & \\sffamily \\textbf{%8.3e} ' % d
                elif d == 0:
                    chart += ' & -- '
                else:
                    chart += ' & \\sffamily %8.3e ' % d

            chart += '%s\n' % tab_lf

        chart += '\\hline \\rule[-3pt]{0pt}{12pt} \\hfill \\textbf{TOTAL:} '

        for r in results:
            chart += ' & \sffamily  \\textbf{%6.3e} ' % r.total()

        chart += '%s\n \\hline\\end{tabularx}\n' % tab_lf

        return chart

    def fragment_report(self, frag, scenario=None, stages=None, results=None, table=False, **kwargs):
        """
        Generate a report for the supplied fragment in the TeXAuthor working directory.
        If results arg is present, draw a chart of stage contributions.
        if table is True, draw a table of stage contributions.
        :param frag:
        :param scenario:
        :param stages: list of stages to report (defaults to a collection of all stages found in the results
        :param results:
        :param table:
        :return:
        """

        if results is not None:
            if not isinstance(results, list):
                results = results.to_list()
            if stages is None:
                stages = _grab_stages(results)
            self.contrib_chart(frag, results, stages=stages, **kwargs)

        coords = self.frag_layout_recurse(frag, scenario=scenario)

        filename = os.path.join(self.folder, '%s.tex' % frag.get_uuid())

        tex_dump = fragment_header(frag, scenario=scenario)
        tex_dump += frag_drawing_opener(coords[-1][2])
        tex_dump += frag_pnodes(coords)
        bx, ar = self.frag_traversal_entry(frag, scenario=scenario)
        tex_dump += bx
        tex_dump += ar
        tex_dump += frag_drawing_closer()

        if os.path.exists(self.img_rel_path(frag)):
            tex_dump += self.frag_chart(frag)

        if results is not None:
            if table is True:
                tex_dump += self.contrib_table(results, stages=stages)

        with open(filename, 'w') as fp:
            fp.write(self._tex_sanitize(tex_dump))

        print('Written to %s' % filename)
        if frag.get_uuid() not in self._file_list:
            with open(self._wrapper_fname, 'a') as wp:
                wp.write('\\input{%s}\\clearpage\n' % filename)
            self._file_list.append(frag.get_uuid())
        return filename

    def new_report(self):
        if os.path.exists(self._wrapper_fname):
            os.remove(self._wrapper_fname)
            self._file_list = []
        with open(self._wrapper_fname, 'w') as fp:
            fp.write('% TeX report wrapper for lcatools')

    def new_section(self, section_name):
        with open(self._wrapper_fname, 'a') as fp:
            fp.write('\\clearpage\n\n\\section{%s}\n\n' % section_name)

