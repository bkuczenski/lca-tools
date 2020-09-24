"""
This is the dynamically-generated study used for running results

CONCEPT: This module / package provides tools to "automagically" build a multi-tiered LCA model that has at its core
an MFA model.  The basic structure of the model is as follows:

#-0-...      Study container (X). Terminated to logistics fragment. Mapping MFA flows to life cycle routes that generate impacts.
  #-0-...    Logistics container (Y). Terminated to activity fragment container. Mapping logistics flows to terminal fragments
    #-0-...  Foreground Activity container (A or a_c). Terminates observed LCA model to various foreground activities, and drive
      |      logistics and market routes in the outer containers
      #- LCA models generated from observational data, generating no impacts and producing balance flows and logistics flows
         accessed by scenarios supplied to the activity container

The study container, logistics container, and activity container are all generic.  The MFA models are generated from
fragment queries, and other MFA-like models can be generated on a study-specific basis.

There are a total of three LcForegrounds involved:

The study and logistics containers are generated on-the-fly / at query time.  The activity container is part of a
semi-persistent observational foreground.  The impact drivers are all stored in a separate / curated modeling foreground.

"""

from antelope import EntityNotFound
from .markets import make_routes, build_market_mix


def build_study_foreground(study, lca, study_container='Study Container', logistics_container='Logistics Container',
                           route_map=None, product_flow_map=None, logistics_map=None, strict=True):
    """
    The study foreground consists of three nested container fragments, each of which implements one stage of LCA
    computation.

    The innermost fragment handles processing activities and is stored semi-persistently in the MFA foreground, along
    with the mfa-derived facility models.  This step is handled in generate_mfa_fragment() below.

    The next-outer fragment handles logistics-- maps MFA logistical flows to LCA transport activities. For this,
    every transporter has to be loaded in mfa_config and mapped here to an lca logistics flow.

    The outermost fragment handles the market disposition of the product flows, and requires the market machinery in
    build_market_mix which, janky though it is, should be fire and forget at this point

    The next-level fragment
    processor activity models
    :param study: a foreground to contain the dynamic study
    :param lca: an LcaModel, having both an observational foreground (fg) and a persistent modeling foreground (models)
    :param study_container: Name (external_ref) of the study container fragment X
    :param logistics_container: Name (external_ref) of the logistics container fragment Y
    :param route_map: see .markets.make_routes()
    :param product_flow_map: maps (fg) flows to market mixes of routes
    :param logistics_map: maps (fg) flows to transport processes
    :return: X and Y, the study overall and the logistics map
    """
    if product_flow_map is None:
        product_flow_map = dict()

    if logistics_map is None:
        logistics_map = dict()

    if route_map is None:
        route_map = dict()

    make_routes(study, lca.models, route_map)

    # build node balance flows
    for tup in lca.lca_entities:
        lca.fg.add_or_retrieve(*tup, strict=strict)

    # build functional unit
    f_u = lca.functional_unit

    # build logistics container
    try:
        log = study.get(logistics_container)
    except EntityNotFound:
        log = study.new_fragment(f_u, 'Output', Name=logistics_container)
        for k, v in logistics_map.items():
            try:
                flow = lca.fg.get(k)
            except EntityNotFound:
                continue
            try:
                term = lca.models.get(v)
            except EntityNotFound:
                continue
            study.new_fragment(flow, 'Input', parent=log, StageName='Reverse Logistics').terminate(term, descend=False)
        study.observe(log, name=logistics_container)
        a_c = lca.activity_container
        log.terminate(a_c)

    # build study container
    try:
        frag = study.get(study_container)
    except EntityNotFound:
        frag = study.new_fragment(f_u, 'Output', Name=study_container)
        for k, p_map in product_flow_map.items():
            flow = lca.fg.get(k)
            c = study.new_fragment(flow, 'Output', parent=frag)
            if build_market_mix(study, c, lca.models, p_map):
                print('Built model for %s' % k)
            else:
                print('Skipped badly formed specification for %s' % k)
        study.observe(frag, name=study_container)
        frag.terminate(log)
    return frag, log


def scenario_knobs(knobs, scenarios):
    """
    Apply parameter values to a set of "knobs" (fragment names) to define scenarios.
    :param knobs: mapping of fragment names (external_ref) to fragments
    :param scenarios: mapping of scenario names to knob: value mappings (dict of dicts)
    :return: None
    """
    for k, vd in scenarios.items():

        for i, v in vd.items():
            if i in knobs:
                knobs[i].observe(v, scenario=k)
            elif v is True:
                # valid setting at runtime; nothing to do here
                continue
            else:
                print('%s: Skipping unknown scenario key %s=%g' % (k, i, v))
