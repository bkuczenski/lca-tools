"""
This file contains scripts to create the test archives used to support unit tests.  It requires an active
catalog at the location specified

"""
from antelope_catalog import LcCatalog
from lcatools.archives import LcArchive

from lcatools.entities import LcQuantity
from lcatools.entities.tests import refinery_archive as DEST_FILE

# CUSTOMIZE THESE ENTRIES AS NEEDED
CAT_FOLDER = '/data/LCI/cat-food/'

NS_UUID = '81ac4b0f-3301-4e80-b76a-bf1d0288e993'


# used internally
petro_id = '0aaf1e13-5d80-37f9-b7bb-81a6b8965c71'
grid_id = '96bffbb9-b875-36cf-8a11-5723c9d239d9'

# emissive coolness
cool = {'2300c22d-fa07-34a3-81a2-6fb72e63b09c': 15.2,  # Lead
        'b4cec7e5-b107-3e94-8152-1d30a754387f': 1.3,  # Selenium
        '6505c252-8e98-3e73-8e2b-e49deaf2598b': 4.7,  # Arsenic
        'c699151a-d569-3534-8ac8-36d6c0615820': 42,  # Antimony
        '984bef7c-3a39-337f-8383-93457f65d597': 138,  # Mercury
        'c643d09b-c28b-3703-b6a3-c90bfe087475': 16,  # Chromium
        'b8d7c779-e771-335b-9443-1ce11dab8237': 12.6,  # Chromium
        }

def shrink_petro(_petro, max_count=3):
    """
    Remove several exchanges so that only a few remain. This is done to keep the archive file size down. Except there's
    really no good reason for that.
    :param _petro:
    :param max_count:
    :return:
    """
    c_in = 0
    c_out = 0
    ditch = []
    for i in _petro.inventory():
        if i.is_reference:
            continue
        if i.direction == 'Input':
            if c_in > max_count:
                ditch.append(i)
                continue
            c_in += 1
        else:
            if c_out > max_count:
                ditch.append(i)
                continue
            c_out += 1
    # assert len(ditch) + (2 * max_count) == 40, len(ditch)
    for i in ditch:
        _petro._exchanges.pop(i.key)
    _petro._d.pop('allocationFactors')

    # ensure the named exchanges required by tests are included
    next(x for x in _petro.exchanges() if x.flow['Name'].startswith('Nitrogen oxides'))
    next(x for x in _petro.exchanges() if x.flow['Name'].startswith('Transport, ocean freighter, r'))


if __name__ == '__main__':
    cat = LcCatalog(CAT_FOLDER)
    petro = cat.get_archive('local.uslci.olca', 'inventory').get(petro_id)  # use get_archive to get entity and not ref
    grid = cat.get_archive('local.uslci.olca', 'inventory').get(grid_id)

    # shrink_petro(petro)  # let's not bother shrinking this

    for p in (petro, grid):
        p._d.pop('processDocumentation')

    A = LcArchive(None, ref='test.entities', ns_uuid=NS_UUID)
    A.add_entity_and_children(petro)
    A.add_entity_and_children(grid)

    # create a dummy LCIA method
    q = LcQuantity.new('Emissive Coolness', 'Cool', Indicator='Coolombs')

    A.add(q)
    for k, v in cool.items():
        A[k].add_characterization(q, value=v)

    A.check_counter()
    # assert len([e for e in A.entities()]) + (2 * max_count) == 54
    A.write_to_file(DEST_FILE, complete=True)
