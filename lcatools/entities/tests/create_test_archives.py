"""
This file contains scripts to create the test archives used to support unit tests.  It requires an active
catalog at the location specified

"""

from lcatools import LcCatalog
from lcatools.providers.lc_archive import LcArchive


CAT_FOLDER = '/data/LCI/cat-food/'
DEST_FILE = '/data/GitHub/lca-tools/lcatools/entities/tests/test_archive.json'
NS_UUID = '81ac4b0f-3301-4e80-b76a-bf1d0288e993'
petro_id = '0aaf1e13-5d80-37f9-b7bb-81a6b8965c71'
grid_id = '96bffbb9-b875-36cf-8a11-5723c9d239d9'


def shrink_petro(_petro, max_count=3):
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
    assert len(ditch) == 34
    for i in ditch:
        _petro._exchanges.pop(i.key)
    _petro._d.pop('allocationFactors')

    # ensure the named exchanges are included
    next(x for x in _petro.exchanges() if x.flow['Name'].startswith('Nitrogen oxides'))
    next(x for x in _petro.exchanges() if x.flow['Name'].startswith('Transport, ocean freighter, r'))


if __name__ == '__main__':
    cat = LcCatalog(CAT_FOLDER)
    petro = cat.get_archive('local.uslci.olca').get(petro_id)
    grid = cat.get_archive('local.uslci.olca').get(grid_id)

    shrink_petro(petro)

    for p in (petro, grid):
        p._d.pop('processDocumentation')

    A = LcArchive(None, ref='test.entities', ns_uuid=NS_UUID)
    A.add_entity_and_children(petro)
    A.add_entity_and_children(grid)
    assert len([e for e in A.entities()]) == 48
    A.write_to_file(DEST_FILE, complete=True)
