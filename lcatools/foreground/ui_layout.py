inspections = {
    'characterization': {
        'show locations': []
    },
    'exchange': {
        'terminate': lambda x: x.terminate,
        'originate': lambda x: x.originate
    },
    'flow': {
        'source': lambda x: x.source,
        'sink': lambda x: x.sink,
        'show characterizations': lambda x: x.profile,
    },
    'process': {
        'intermediate exchanges': lambda x: x.intermediate,
        'elementary exchanges': lambda x: x.elementary,
        'foreground lcia': lambda x: x.lcia,
        'lcia detailed results': lambda x: x.q_lcia,
        'background lcia': [],  # lambda x: x.bg_lcia,
        'select exchange to follow': lambda x: x.select_exchange
    },
    'quantity': {
        'flowables': [],  #lambda x: x.flowables,
        'by compartment': lambda x: x.factors,
        'all factors': []  #lambda x: x.all_factors
    }
}

comparisons = {
    'flow': {

    }
}

choices = {
    # 33 handlers at first count for v0.1 - what, 20 minutes each? 2016-08-04 22:30
    # 13 written (plus a lot of background work), 25 to go... 2016-08-05 13:42
    # 26 written; 13 or so to go ... 2016-08-08 12:39
    'Catalog': {
        'show catalog': lambda x: x._show,
        'add archive': lambda x: x.add_archive,
        'load archive': lambda x: x.load_archive,
        'choose archive': lambda x: x.set_current_archive,
        'search entities': {
            'processes': lambda x: x.isearch('process'),
            'flows': lambda x: x.isearch('flow'),
            'quantities': lambda x: x.isearch('quantity')
        },
        'browse entities': lambda x: x.ibrowse,
        'selection': {
            'add to foreground': lambda x: x.add_selection,
            'inspect': lambda x: x.inspect,
            'compare': lambda x: x.compare,
            'unselect': []
        },
    },
    'FlowDB': {
        'flowables': {
            'search': lambda x: x.search_flowables,
            'add synonym': [],
            'lookup characterizations': []
        },
        'compartments': {
            'browse': lambda x: x.browse_compartments,
            'add synonym': [],
            'add subcompartment': []
        }
    },
    'Foreground': {
        'view foreground entities': lambda x: x.view_foreground,
        'work on foreground': lambda x: x.specify_foreground,
        'add selection to foreground': lambda x: x.add_selection,
        'create flow': [],
        'edit flow': [],
        'add background': [],
    },
    'Fragments': {
        'list fragments': [],
        'create fragment': [],
        'edit fragment': [],
        'fragment flows': [],
        'fragment LCIA': []
    },
    'X - save and exit': 1
}
