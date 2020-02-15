def transport_model(modeled_flow):
    return 'Transport Mix - %s' % modeled_flow.name


def activity_model_ref(modeled_fragment):
    return 'Activity model - %s' % modeled_fragment['Name']


def logistics_summary_ref(sc_frag):
    return '%s Logistics' % sc_frag.name
