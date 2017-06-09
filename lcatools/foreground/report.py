"""
This file is used to cast a fragment to TeX.  The output is a TeX file in an include folder which draws
a fragment as a tree diagram using PSTricks.

I guess for research on this, I should start by actually drawing a fragment by hand
"""
import os
import re
from lcatools.exchanges import comp_dir
from lcatools.charts import scenario_compare_figure, save_plot


class TraversalError(Exception):
    pass


default_tex_folder = 'tex-files'
default_frag_data = 'fragment-data.tex'
default_model_doc = 'model-doc.tex'


def tex_sanitize(tex):
    tex = re.sub('%', '\\%', tex)
    tex = re.sub('_', '\\\\textunderscore{}', tex)  # this doesn't work bc filenames have underscores
    return tex


def grab_stages(results):
    stages = set()
    for r in results:
        stages = stages.union(r.components())
    return list(stages)


"""
drawing 'primitives'
"""
TAB_LF = '\\\\'


def stage_name_table(stages):
    out = '\\scriptsize\n\\begin{tabular}[b]{c@{ -- }l}\n'
    for i, k in enumerate(stages):
        out += '%s & %s\\\\\n' % (chr(ord('A') + i), tex_sanitize(k))
    out += '\\end{tabular}\n'
    return out


def save_stages(fname, stages):
    out = stage_name_table(stages)
    with open(fname + '_stages', 'w') as fp:
        fp.write(out)


def fragment_header(frag):
    s = '{\\hypertarget{%.5s}{\\Large \\texttt{%.5s}}}\\subsection{%s}\n' % (
        frag.get_uuid(), frag.get_uuid(), frag['Name']
    )
    return tex_sanitize(s)


def fragment_fu(frag, scenario=None):
    s = '{\\large %s: %g %s %s}\\\\[8pt]\n%s\n' % (comp_dir(frag.direction), frag.exchange_value(scenario),
        frag.flow.unit(),
        frag.flow['Name'], frag['Comment'])
    return tex_sanitize(s)


def fragment_inventory(fragment, scenario=None):
    exchs = [x for x in fragment.inventory(scenario=scenario, observed=True)]
    inventory = ''
    if len(exchs) > 0:
        inventory += '\n{\\small\n\\begin{tabular}{rccl}\n'
        for x in exchs:
            inventory += tex_sanitize('%6s & %6.3g & %s & %s\\\\\n' % (x.direction,
                                                                       x.value, x.flow.unit(), x.flow['Name']))
        inventory += '\\end{tabular}\n}\n'

    return inventory


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
    return '\\rput(px%.5s){\\rput[l](1,0){\\rnode[style=phantom]{nx%.5s}{\scriptsize CUTOFF}}}' % (uid, uid)


def bg_box(uid):
    return '\\rput(px%.5s){\\pnode(1.1,0){nx%.5s}\\psframe*(1,-0.25)(1.1,0.25)}' % (uid, uid)


def subfrag_box(uid, tgt_top):
    return ('\\rput(px%.5s){\\rput(1.25,0){\\nodebox{nx%.5s}{style=process,style=dashed}'
            '{1.15cm}{0.6cm}{\\hyperlink{%.5s}{\\texttt{%.5s}}}}}' % (uid, uid, tgt_top, tgt_top))


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
    def _img_fname(f_base):
        return '%s_img.eps' % f_base

    @staticmethod
    def _stg_fname(f_base):
        return '%s_stages' % f_base

    @property
    def _wrapper_fname(self):
        return os.path.join(self.folder, self._fragment_data_file)

    @property
    def _documentation_fname(self):
        return os.path.join(self.folder, self._model_doc_file)

    @staticmethod
    def _init_report_file(fname, intro_text):
        if os.path.exists(fname):
            os.remove(fname)

        with open(fname, 'w') as fp:
            fp.write('%% %s\n' % intro_text)

    def _read_wrapper(self):
        with open(self._wrapper_fname) as fp:
            return fp.read().splitlines()

    def img_rel_path(self, f_base):
        return os.path.join(self.img_folder, self._img_fname(f_base))

    def stg_rel_path(self, f_base):
        return os.path.join(self.img_folder, self._stg_fname(f_base))

    def _write_stage_names(self, f_base, stages):
        out = stage_name_table(stages)
        with open(self.stg_rel_path(f_base), 'w') as fp:
            fp.write(out)

    def __init__(self, folder=default_tex_folder, overwrite=True, comments=False,
                 frag_data=default_frag_data, model_doc=default_model_doc):
        """
        This function

        :param folder: ['tex-files'] Use different folders for different reports if the reports represent different
         scenarios or data.
        :param overwrite: [True] whether to overwrite existing files in the folder
        :param comments: [False] whether to write FragmentFlow comments on tree drawings
        :param frag_data: ['fragment-data.tex'] default fragment data file.  Use the same folder but different frag_data
         files for different reports if the reports are based on the same scenario but include different contents.
        :param model_doc: ['model-doc.tex'] model doc file.
        """
        self.folder = folder
        self.overwrite = overwrite
        self.comments = comments

        self._fragment_data_file = frag_data
        self._model_doc_file = model_doc

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

    def _write_output(self, f_base, content, add_to_index=None):
        """
        Writes output to a specified file basename.  The output is written to f_base.tex, and any image and
        stage data should be generated in img_rel_path(f_base) and stg_rel_path(f_base) respectively.
        :param f_base:
        :param content:
        :param add_to_index: can be None, 'tex', 'doc': whether to add the filename to the tex-files or
         model-documentation index
        :return:
        """
        filename = os.path.join(self.folder, '%s.tex' % f_base)

        with open(filename, 'w') as fp:
            fp.write(content)

        print('Written to %s' % filename)
        if add_to_index == 'tex':
            if f_base not in self._file_list:
                with open(self._wrapper_fname, 'a') as wp:
                    wp.write('\\input{%s}\\clearpage\n' % filename)
                self._file_list.append(f_base)
        elif add_to_index == 'doc':
            with open(self._documentation_fname, 'a') as dp:
                dp.write('\\input{%s}\n' % filename)

    def frag_layout_recurse(self, fragment, level=0, depth=0, scenario=None):
        """
        This pass creates a list of coordinates for pnode declarations for the reference points of all fragment flows.
        returns a 3-tuple of (pnode name, x-coord, y-coord)

        At the end of it, coords[-1][2] is the vertical position of the last child

        Each pnode's name is equal to the first 5 characters of the fragment's uuid, same as in the drawing.
        :param fragment:
        :param level: [0] vertical traversal height, used for positioning the pnodes
        :param depth: [0] horizontal traversal depth
        :param scenario:
        :return:
xs        """
        coords = [('%.5s' % fragment.get_uuid(), depth, level)]
        if fragment.exchange_value(scenario, observed=True) == 0:
            return coords

        term = fragment.termination(scenario)

        children = [c for c in fragment.child_flows]
        if len(children) > 0 and not term.is_null:
            # setup coords for first child
            depth += 2.0
            if term.is_subfrag:
                depth += .5
            level += 1
            for c in sorted(children, key=lambda x: (x['StageName'],
                                                     x.direction,
                                                     not x.termination(scenario).is_null,
                                                     x.termination(scenario).is_bg)):
                c_term = c.termination(scenario)
                if c.exchange_value(scenario, observed=True) == 0 and not c.balance_flow:
                    continue
                if c_term.is_subfrag:
                    level += .25
                coords.extend(self.frag_layout_recurse(c, level=level, depth=depth, scenario=scenario))
                level = coords[-1][2]
                # setup coords for next child -- background nodes are smaller
                if c_term.is_bg or c_term.is_fg or c_term.is_null:
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
        :return: boxes, arrows, subfragments
        """
        subfrag_scale = 1.0
        subfrags = []
        term = fragment.termination(scenario)

        if not first:
            node_weight *= fragment.exchange_value(scenario, observed=True)
        if node_weight == 0 and not fragment.balance_flow:
            return '\n', '\n', subfrags
        if term.is_frag:
            if term.is_fg:
                # foreground
                boxes = fg_box(fragment.get_uuid())
                frag_name = fragment['Name']
            elif term.is_bg:
                if term.term_node.term.is_null:
                    # cutoff
                    boxes = cutoff_box(fragment.get_uuid())
                else:
                    # background
                    boxes = bg_box(fragment.get_uuid())
                frag_name = term.term_node['Name']
            else:
                # subfragment
                top_frag = term.term_node.top()
                subfrags.append(top_frag)
                boxes = subfrag_box(fragment.get_uuid(), top_frag.get_uuid())
                if term.term_node is top_frag:
                    subfrag_scale = 1.0 / top_frag.exchange_value(scenario, observed=True)
                else:
                    ffs = [f for f in top_frag.io_flows(scenario, observed=True) if f.fragment is term.term_node]
                    if len(ffs) != 1:
                        raise TraversalError('top:%s\nterm:%s\n%d matching flow found' % (top_frag, term.term_node,
                                                                                          len(ffs)))
                    subfrag_scale = 1.0 / ffs[0].node_weight
                # except ZeroDivisionError:
                    # zero exchange value usually means unobserved-- for reference flow that is not usually important
                    # subfrag_scale = 0.0
                print('subfrag scaling: %g' % subfrag_scale)
                frag_name = term.term_node['Name']
                '''
                exchs = [x for x in fragment.get_fragment_inventory(scenario=scenario, scale=node_weight)]
                if len(exchs) > 0:
                    frag_name += '\\\\\n{\scriptsize\n'
                    for x in exchs:
                        frag_name += '%6s: %6.3g %s\\\\' % (x.direction, x.value, x.flow['Name'])
                    frag_name += '}'
                '''
        elif term.is_null:
            # I/O
            boxes = io_box(fragment.get_uuid())
            frag_name = '%s: %s' % (fragment.direction, fragment.flow['Name'])
        else:
            # process
            boxes = process_box(fragment.get_uuid())
            frag_name = term.term_node['Name']
            subfrag_scale = term.node_weight_multiplier
        mag_mod = ''
        if fragment.balance_flow:
            mag_mod += '(=)'
        if node_weight < 0:
            frag_name = '(AVOIDED) ' + frag_name
            mag_mod += '\\darkred'

        if self.comments:
            # this can get added in once we have an easy way to curate comments
            if not first:
                if len(fragment['Comment']) > 0:
                    frag_name += '~$\cdot$~{\\scriptsize %s}' % fragment['Comment']

        boxes += '\n\\rput[l]([angle=0,nodesep=6pt]nx%.5s)' % fragment.uuid
        boxes += '{\parbox{%fcm}{\\footnotesize\\raggedright %s}}' % (parbox_width,
                                                                      tex_sanitize(frag_name))

        if fragment.direction == 'Input':
            arrows = '\\ncline{->}{nx%.5s}{px%.5s}' % (fragment.get_uuid(), fragment.get_uuid())
        else:
            arrows = '\\ncline{<-}{nx%.5s}{px%.5s}' % (fragment.get_uuid(), fragment.get_uuid())
        if not first:
            arrows += '\n\\bput(1.18){\\parbox{2cm}{\\centering %s \\scriptsize %.3g %s}}' % (mag_mod,
                                                                                              node_weight,
                                                                                              fragment.flow.unit())

        children = [c for c in fragment.child_flows]
        if node_weight != 0:
            if len(children) > 0 and not term.is_null:
                parbox_width -= 2.4

                for c in children:
                    arrows += '\n\\ncangle[angleA=-90,angleB=180,framearc=0]{nx%.5s}{px%.5s}' % (fragment.get_uuid(),
                                                                                                 c.get_uuid())
                    arrows += '\n'
                    boxes += '\n'
                    bplus, aplus, sfplus = self.frag_layout_traverse(c, node_weight * subfrag_scale,
                                                                     scenario=scenario, parbox_width=parbox_width)

                    boxes += bplus
                    arrows += aplus

                    subfrags.extend(sfplus)

        arrows += '\n'
        boxes += '\n'
        return boxes, arrows, subfrags

    def _contrib_chart(self, f_base, results, stages=None, **kwargs):
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
            stages = grab_stages(results)

        fig = scenario_compare_figure(results, stages, **kwargs)
        save_plot(os.path.join(self.img_folder, self._img_fname(f_base)))
        self._write_stage_names(f_base, stages)
        return fig

    def frag_chart_narrow(self, f_base):
        return '''
\\includegraphics[width=\\textwidth]{%s}
\\rput[tl](pLegend){\\parbox{5cm}{\\raggedright Stages\\\\
{\\scriptsize \\input{%s}}}}
''' % (self.img_rel_path(f_base), self.stg_rel_path(f_base))

    @staticmethod
    def _results_start(title='Contribution Analysis'):
        return '''
\\begin{minipage}{\\textwidth}
{\\pnode(12,-1){pLegend}
\\large %s}
''' % title

    @staticmethod
    def _results_end():
        return '\n\\end{minipage}\n'

    def _frag_chart(self, f_base):
        """
        Generates TeX for a contribution analysis chart using the canonical fragment name
        :param f_base:
        :return:
        """
        return '\n\n\\includegraphics[width=\\textwidth]{%s}' % self.img_rel_path(f_base)

    def _scenario_contrib_tables(self, results, stages, scenarios):
        """
        Generates a set of tabular scenario-comparison tables, one for each indicator.
        :param results:
        :param stages:
        :param scenarios:
        :return:
        """
        num_charts = len(results[0])
        chart = '\\\\[1em]\n'

        for i in range(num_charts):
            chart += '\n%s\n\n' % results[0][i].quantity['Name']
            chart += '\n\\begin{tabularx}{\\textwidth}{|X|%s}\n\\hline' % ('r|' * len(results))
            chart += '\\rule[-4pt]{0pt}{16pt}\\textbf{Stage} '
            for j in scenarios:
                chart += ' & \\textbf{%.15s} ' % j

            # chart += '%s \n ' % TAB_LF
            # for j in results:
            #     chart += ' &  %s ' % j.quantity.unit()

            chart += '%s \\hline\n' % TAB_LF

            results_inner = [res[i] for res in results]

            chart += self._contrib_table_body(results_inner, stages=stages)
            chart += '%s\n \\hline\\end{tabularx}\n' % TAB_LF

        return chart

    @staticmethod
    def _contrib_table_body(results_inner, stages=None):
        if stages is None:
            stages = grab_stages(results_inner)

        chart = ''
        for i, s in enumerate(stages):
            chart += '%s -- %s' % (chr(ord('A') + i), tex_sanitize(s))

            for r in results_inner:
                d = sum([q for q in r.contrib_query([s])])
                if abs(d) > 0.1 * r.range():
                    chart += ' & \\sffamily \\textbf{%8.2e} ' % d
                elif d == 0:
                    chart += ' & -- '
                else:
                    chart += ' & \\sffamily %8.2e ' % d

            chart += '%s\n' % TAB_LF

        chart += '\\hline \\rule[-3pt]{0pt}{12pt} \\hfill \\textbf{TOTAL:} '

        for r in results_inner:
            chart += ' & \sffamily  \\textbf{%6.3e} ' % r.total()

        return chart

    def _contrib_table(self, results, stages=None):
        """
        Generates TeX for a Tabular contribution chart.
        :param results:
        :param stages:
        :return:
        """
        if not isinstance(results, list):
            results = results.to_list()

        chart = '\n\\begin{tabularx}{\\textwidth}{|X|%s}\n\\hline' % ('r|' * len(results))
        chart += '\\rule[-4pt]{0pt}{16pt}\\textbf{Stage} '
        for i, j in enumerate(results):
            chart += ' & \\textbf{%.12s} ' % j.quantity['Indicator']

        chart += '%s \n ' % TAB_LF
        for j in results:
            chart += ' &  %s ' % j.quantity.unit()

        chart += '%s \\hline\n' % TAB_LF

        chart += self._contrib_table_body(results, stages=stages)
        chart += '%s\n \\hline\\end{tabularx}\n' % TAB_LF

        return chart

    def _process_tex(self, frag, scenario=None, full=True):
        """
        Returns TeX content for the process tree figure
        :param frag:
        :param scenario:
        :param full: [True] whether to print section heading and inventory
        :return:
        """
        coords = self.frag_layout_recurse(frag, scenario=scenario)

        if full:
            tex_dump = fragment_header(frag)
            tex_dump += fragment_fu(frag, scenario=scenario)
            tex_dump += fragment_inventory(frag, scenario=scenario)
        else:
            tex_dump = fragment_fu(frag, scenario=scenario)

        tex_dump += frag_drawing_opener(coords[-1][2])
        tex_dump += frag_pnodes(coords)
        bx, ar, subfrags = self.frag_traversal_entry(frag, scenario=scenario)  # subfrags go back up to the author
        tex_dump += bx
        tex_dump += ar
        tex_dump += frag_drawing_closer()

        return tex_dump, subfrags

    def results_tex(self, f_base, results, stages=None, table=False, scenarios=None, **kwargs):
        """
        Generates a chart and table from a set of results, and returns the TeX content
        :param f_base:
        :param results:
        :param stages:
        :param table:
        :param scenarios: if results is a double-array, indicate scenario names
        :param kwargs:
        :return:
        """
        if not isinstance(results, list):
            results = results.to_list()
        if stages is None:
            stages = grab_stages(results)

        skip = False
        if scenarios is None:
            tex_dump = self._results_start()
            if all(r.total() == 0 for r in results):
                skip = True
        else:
            tex_dump = self._results_start(title='Scenario Analysis')
            if all(r.total() == 0 for k in results for r in k):
                skip = True

        if skip:
            tex_dump += '\n\nNo Impacts.\n'

        else:
            try:
                self._contrib_chart(f_base, results, stages=stages, scenarios=scenarios, **kwargs)
                tex_dump += self._frag_chart(f_base)
            except AttributeError:
                print('bailing out of chart')
                tex_dump = ''

            if table is True:
                if scenarios is None:
                    tex_dump += self._contrib_table(results, stages=stages)
                else:
                    tex_dump += self._scenario_contrib_tables(results, stages, scenarios)

        tex_dump += self._results_end()
        return tex_dump

    def results_report(self, f_base, results, **kwargs):
        """

        :param f_base:
        :param results:
        :param kwargs: stages, table, other args to scenario_compare_figure
        :return:
        """
        self._write_output(f_base, self.results_tex(f_base, results, **kwargs), add_to_index='doc')

    def fragment_diagram(self, f_base, frag, scenario=None):
        """
        Generate just the fragment diagram, without heading, inventory or results
        :param f_base:
        :param frag:
        :param scenario:
        :return:
        """
        tex_dump, subfrags = self._process_tex(frag, scenario=scenario, full=False)
        self._write_output(f_base, tex_dump, add_to_index='doc')
        return subfrags

    def fragment_report(self, frag, f_base=None, scenario=None, stages=None, results=None, table=False, **kwargs):
        """
        Generate a report for the supplied fragment in the TeXAuthor working directory.  Fragment report is
        created as a subsection with a heading, hyperlink, and full inventory listing.
        If results arg is present, draw a chart of stage contributions.
        if table is True, draw a table of stage contributions.
        :param frag:
        :param f_base: defaults to frag.uuid; if the default is overridden, output is written to doc
        :param scenario:
        :param stages: list of stages to report (defaults to a collection of all stages found in the results
        :param results: list of LciaResult objects
        :param table: [False]
        :return: a list of the fragment's children
        """
        if f_base is None:
            f_base = frag.uuid
            _to_index = 'tex'
        else:
            _to_index = 'doc'

        tex_dump, subfrags = self._process_tex(frag, scenario=scenario)

        if results is not None:
            tex_dump += self.results_tex(f_base, results, stages=stages, table=table, **kwargs)

        self._write_output(f_base, tex_dump, add_to_index=_to_index)

        if frag.has_property('ModelDocumentation'):
            docbase = os.path.join(self.folder, '%s-model-doc' % f_base)
            self._write_output(docbase, frag['ModelDocumentation'], add_to_index='doc')

        return subfrags

    def new_report(self):
        """
        Create new fragment-data and model-doc files; if fragment
        :return:
        """
        self._file_list = []
        self._init_report_file(self._wrapper_fname,
                               'TeX report wrapper for fragment-data, lcatools')

        self._init_report_file(self._documentation_fname,
                               'TeX report wrapper for model documentation, lcatools')

    def new_section(self, section_name):
        with open(self._wrapper_fname, 'a') as fp:
            fp.write('\\cleardoublepage\n\n\\section{%s}\n\n' % section_name)

    def report(self, query, chart=True):
        """
        generate TeX reports for the fragments and results.
        :param query: a ForegroundQuery
        :param chart: [True] whether to compute + plot results
        :return: list of child fragments
        """
        children = []
        for i, f in enumerate(query.fragments):
            if chart:
                ch = self.fragment_report(f, stages=query.agg_stages[i], results=query.agg_results[i], table=True,
                                          scenario=query.scenario)
            else:
                ch = self.fragment_report(f, scenario=query.scenario)
            for k in ch:
                if k not in children:
                    # preserve order encountered
                    children.append(k)

        return children

    def recurse_report(self, cat, fragments, quantities, section_names=None, **kwargs):
        """
        Recursively generate fragment drawings for supplied fragments plus all descendents.
        TODO: figure out a way to organize / group child fragments other than by tier.
        Current plans are to _manually edit_ fragment_data.tex to reorganize sections / hide private fragments
        :param cat: Foreground Catalog
        :param fragments: list of LcFragment objects at the top tier of the report [[ or uuids? ]]
        :param quantities: list of LcQuantity objects or quantity UUIDs
        :param section_names: [None] a list of names for successive tiers
        :param kwargs: passed to foreground query: weightings, scenario, observed, [scale doesn't make sense]
        """
        self.new_report()
        if section_names is None:
            section_names = []

        try:
            section_name = section_names.pop(0)
        except IndexError:
            section_name = 'Top-level Fragments'

        # run out top-level frags, then draw their children in the next section
        qs = []

        for q in quantities:
            if isinstance(q, str):
                qs.append(q)
            else:
                qs.append(q.get_uuid())

        level = 0

        seen = []

        while len(fragments) > 0:
            if len(section_name) > 0:
                self.new_section(section_name)
            seen.extend(fragments)
            query = cat.fg_query(fragments, qs, **kwargs)
            children = self.report(query)
            new_children = [ch for ch in children if ch not in seen]
            fragments = new_children

            level += 1

            try:
                section_name = section_names.pop(0)
            except IndexError:
                section_name = 'Tier %d Fragments' % level
