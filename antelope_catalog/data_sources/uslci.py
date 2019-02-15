from .data_source import DataSource, ResourceInfo

VALID_FORMATS = ('olca', 'ecospold')  # 'ecospold')


INFO = {
    'olca': ResourceInfo(None,
                        'OpenLcaJsonLdArchive',
                         'https://www.dropbox.com/s/upuo0yc7hwevmvs/NREL_USLCI_JSON__20180109.zip?dl=1',
                         '3e8471b1a73b38013c3ab8e8b2df0ef4',
{
    "unset_reference": [
          [
            "dc72e285-719b-318b-9c9c-c838846a9cf4",
            "d939590b-a0d7-310c-8952-9921ed64a078",
            "Output"
          ],
          [
              '155839ec-d6ab-3ed6-b7c8-45586e676f6d',  # ambiguous termination
              '9dfc9e7c-3c20-3f4d-9d79-d54ec2d8d08b',
              'Output'
          ]
    ],
    "set_reference": [
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "d939590b-a0d7-310c-8952-9921ed64a078",
            "Output"
          ],
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "c4069217-dfd4-324a-9243-2ee8058809d6",
            "Output"
          ],
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "0e44e579-abb0-3c77-af64-c774d65be529",
            "Output"
          ],
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "c5f94bb1-b39e-39fa-b616-376a30531c2d",
            "Output"
          ],
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "4f1bfabb-f0d3-3f1f-a905-2fd9b36bbfa0",
            "Output"
          ],
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "562918fe-3ff5-33f4-abaa-d8c615380d25",
            "Output"
          ],
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "5f96ad43-abab-3b62-bcf8-c71f9d6c7881",
            "Output"
          ],
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "30a64e63-3583-3f83-8d8d-0b6ea003b888",
            "Output"
          ],
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "588384cf-851f-391e-bf49-c6e5309d76db",
            "Output"
          ]
    ],
    "characterize_flow": [
          [
            "0e44e579-abb0-3c77-af64-c774d65be529",
            "93a60a56-a3c8-11da-a746-0800200b9a66",
            750
          ],
          [
            "562918fe-3ff5-33f4-abaa-d8c615380d25",
            "93a60a56-a3c8-11da-a746-0800200b9a66",
            975
          ],
          [
            "588384cf-851f-391e-bf49-c6e5309d76db",
            "93a60a56-a3c8-11da-a746-0800200b9a66",
            500
          ],
          [
            "c4069217-dfd4-324a-9243-2ee8058809d6",
            "93a60a56-a3c8-11da-a746-0800200b9a66",
            0.737
          ],
          [
            "c5f94bb1-b39e-39fa-b616-376a30531c2d",
            "93a60a56-a3c8-11da-a746-0800200b9a66",
            809
          ],
          [
            "d939590b-a0d7-310c-8952-9921ed64a078",
            "93a60a56-a3c8-11da-a746-0800200b9a66",
            850
          ]
    ],
    "allocate_by_quantity": [
          [
            "0aaf1e13-5d80-37f9-b7bb-81a6b8965c71",
            "93a60a56-a3c8-11da-a746-0800200b9a66"
          ]
    ]
},
                         {}),
    'ecospold': ResourceInfo(None,
                             'EcospoldV1Archive',
                             'https://www.dropbox.com/s/xcsxgj5pipgwe43/USLCI_Processes_ecospold1.zip?dl=1',
                             'f022051ee87c0976e126ebcc2bced017',
{
    "set_reference": [
        ['Natural gas, combusted in industrial equipment', 2176, 'Output']  # hack to force load of flow 2176
    ],
    "unset_reference": [
        ['Crude oil, in refinery', None, 'Output'],  # remove all Output references
#        [None, 9218, 'Output'],  # recovered energy, generic  # can't sort 'None' :(
        ['Petroleum refining, at refinery', 32123, 'Output'],  # documentary activity flow -- not allocatable
        ['Bucked and debarked log, hardwood, green, at veneer mill, E', 5339, 'Output'],  # ambiguous term error
        ['Harvesting, fresh fruit bunch, at farm (2)', 23293, 'Output']  # terrible naming practice
    ],
    "characterize_flow": [
        [775, 'kg', 0.867],  # 1.153 L/kg from ecospold doc
        [869, 'kg', 0.739],  # 1.353 L/kg from ecospold doc
        [16694, 'kg', 0.543],  # 1.84 L/kg from ecospold doc
        [16696, 'kg', 0.944],  # 1.059 L/kg from ecospold doc
        [16700, 'kg', 0.809],  # 1.236 L/kg from ecospold doc
        [16702, 'kg', 0.737],  # 1.356 m3/kg from ecospold doc
        [2176, 'kWh', 11.111]   # see ecospold documentation
    ],
    "allocate_by_quantity": [
        [
            'Petroleum refining, at refinery', 'kg'
        ]
    ]
},
                             {
                                 'prefix': 'USLCI_Processes_ecospold1/USLCI_Processes_ecospold1',
                                 'ns_uuid': '96386cae-b651-47ca-8fcd-d3a1aebd6034'
                             })}


class UsLciConfig(DataSource):

    prefix = 'local.uslci'
    _ifaces = ('inventory', 'quantity')

    def _ref(self, fmt):
        return '.'.join([self.prefix, fmt])

    @property
    def references(self):
        for f in VALID_FORMATS:
            yield self._ref(f)

    def interfaces(self, ref):
        for k in self._ifaces:
            yield k

    def make_resources(self, ref):
        if ref not in self.references:
            raise ValueError('Unknown reference %s' % ref)
        fmt = ref.split('.')[-1]
        info = INFO[fmt]
        yield self._make_resource(ref, info=info, interfaces=self._ifaces)
