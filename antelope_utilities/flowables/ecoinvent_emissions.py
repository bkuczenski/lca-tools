from antelope_catalog.data_sources.local import make_config


def add_ecoinvent_synonyms_to_flowables(cat, ecoinvent_ref, save_local=False):
    ec = make_config('ecoinvent')
    res = next(ec.make_resources(ecoinvent_ref))
    res.check(cat)
    if not res.archive._master._fs.OK:  # fix this!
        raise AttributeError('Ecoinvent resource does not have MasterData')
    res.archive.load_flows()
    for f in res.archive.entities_by_type('flow'):
        if f.context is None:
            continue
        if 'Produktion von Getreide, Hackfrüchten, Gemüse, etc.' in f.synonyms:
            print('Dealing with problematic ecoinvent synonym')
            cat.lcia_engine.add_flow(f, merge_strategy='distinct')
        else:
            cat.lcia_engine.add_flow(f)

    if save_local:
        cat.lcia_engine.save_flowables(cat._flowables)  # fix this too
