from antelope import EntityNotFound


def __make_link(fg, parent, models, child):
    if isinstance(child, tuple):
        f = []
        if isinstance(parent, list):
            for i, p in enumerate(parent):
                if p is None or child[i] is None:
                    f.append(None)
                else:
                    term = models.get(child[i])
                    f.append(fg.new_fragment(term.flow, 'Output', parent=p))
                    f[-1].terminate(term)
        else:
            for c in child:
                term = models.get(c)
                f.append(fg.new_fragment(term.flow, 'Output', parent=parent))
                f[-1].terminate(term)
    else:
        term = models.get(child)
        if parent is None:
            f = fg.new_fragment(term.flow, 'Input')
        else:
            f = fg.new_fragment(term.flow, 'Output', parent=parent)
        f.terminate(term)
    return f


def _make_route(fg, models, route):
    """
    Cycle through
    :param fg:
    :param route:
    :return:
    """
    parent = first = None
    for step in route:
        if first is None and isinstance(step, tuple):
            raise ValueError('Model spec cannot begin with a tuple! %s' % step)
        child = __make_link(fg, parent, models, step)
        if first is None:
            first = child
        parent = child
    return first


def make_routes(study, models, routes):
    """
    Disposition routes get built from scratch during LCA runtime in LCA study foreground.  Components are drawn from
    models foreground.

    This is a bit of a Kluge, but here is the idea:

    A ROUTE SPECIFICATION consists of a sequence of STAGES (as a tuple).  Each STAGE is EITHER:
     - a fragment present in the models foreground, OR
     - a BRANCH.  Any given route specification may contain AT MOST ONE BRANCH. The branch is implemented as a tuple
       of fragments.  Each subsequent stage must be a tuple with the same length.  The first stage may not be a branch.

    For each stage, the route builder will retrieve the target and create a fragment whose flow matches the target's.
    It will then terminate the fragment to the target.  That stage then becomes the parent of the subsequent stage.

    When a branch is encountered, it will create multiple child flows from the parent, one for each entry in the branch.
    EVERY SUBSEQUENT STAGE must have the same number of entries as the branch, and these entries are appended as
    child nodes to the branched parents.  Use None to end a branch (None must be supplied as a placeholder throughout
    the route).

    :param study: LcForeground to contain the dynamically constructed routes
    :param models: LcForeground that has the modeling content used in the routes.  Both foregrounds may be the same.
    :param routes: a dict of name : route specification, where the name is assigned as the external_ref
    :return: a list of names (external_refs) of the created fragments, should be identical to the keys of routes.
    """
    refs = []
    for k, v in routes.items():
        refs.append(k)
        if study[k] is None:
            route = _make_route(study, models, v)
            study.observe(route, name=k)
    return refs


def _check_p_map(fg, models, p_map):
    nc = 0
    for k, v in p_map.items():
        try:
            fg.get(k)
        except EntityNotFound:
            try:
                models.get(k)
            except EntityNotFound:
                print('Product map entry %s not found' % k)
                return False
        if v is None:
            nc += 1
        else:
            try:
                float(v)
            except (ValueError, AttributeError):
                print('Key %s has non-floatable value %s' % (k, v))
                return False
    if nc != 1:
        print('Wrong number of balance flows (%d)' % nc)
        return False
    return True


def build_market_mix(fg, parent, models, p_map, freight=False):
    if freight:
        sense = 'Input'
    else:
        sense = 'Output'
    if _check_p_map(fg, models, p_map):
        for k, v in p_map.items():
            try:
                term = fg.get(k)
            except EntityNotFound:
                term = models.get(k)
            if freight:
                flow = term
            else:
                flow = parent.flow
            if v is None:
                c = fg.new_fragment(flow, sense, parent=parent, balance=True)
            else:
                c = fg.new_fragment(flow, sense, parent=parent, value=v)
                mix_name = 'mix-%s-%s' % (parent.flow.external_ref, k)
                fg.observe(c, name=mix_name, exchange_value=v)
            if freight:
                # don't terminate freight flows
                continue
            c.terminate(term, term_flow = term.flow)
            if term.external_ref == 'frag-t_waste':
                c.term.descend = False
                c['StageName'] = 'Waste to Landfill'

        return True
    return False


def market_mixers(study):
    return {k.external_ref: k for k in study.fragments(show_all=True) if k.external_ref.startswith('mix')}
