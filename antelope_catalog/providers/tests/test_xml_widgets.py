from ..xml_widgets import render_text_block, find_tag
from lxml import objectify
import unittest


ecospold2_sample = '''
<ecoSpold xmlns="http://www.EcoInvent.org/EcoSpold02">
  <childActivityDataset>
    <activityDescription>
      <activity id="aed7f674-fc6b-5c7e-9e09-0b89adb92259" activityNameId="202fa2d7-8c4e-4e6a-8870-d8db89b890fa" activityNameContextId="de659012-50c4-4e96-b54a-fc781bf987ab" parentActivityId="aed7f674-fc6b-5c7e-9e09-0b89adb92259" inheritanceDepth="-1" type="1" specialActivityType="1">
        <activityName xml:lang="en">market for electricity, medium voltage</activityName>
        <includedActivitiesStart xml:lang="en">This activity starts from 1kWh of electricity fed into the medium voltage transmission network.</includedActivitiesStart>
        <includedActivitiesEnd xml:lang="en">This activity ends with the transport of 1 kWh of medium voltage electricity in the transmission network over aerial lines and cables.

This dataset includes: 
  - electricity inputs produced in this country and from imports and transformed to medium voltage
 - the transmission network
 - direct emissions to air (SF6) 
 - electricity losses during transmission 

This dataset doesn't include
 - electricity losses during transformation from high to medium voltage or medium to low, as these are included in the dataset for transformation
  - leakage of insulation oil from cables and electro technical equipment (transformers, switchgear, circuit breakers) because this only happens in case of accidental release
 - SF6 emissions during production and deconstruction of the switchgear, as these are accounted for in the transmission network dataset.
- Emissions to soil from leakage of treating substances from poles (included in transmission network).</includedActivitiesEnd>
        <generalComment>
          <text xml:lang="en" index="10000">The shares of electricity technologies on this market are valid for the year 2014. They have been implemented by the software layer and don't represent the production volumes in the unlinked datasets valid for the year 2012. These shares have been calculated based on statistics from 2014. Basic source: Statistical tables by StatCan on electricity production in Canada. Further information will be available in a report available on ecoQuery. </text>
          <variable xml:lang="en" name="year">2014</variable>
          <text xml:lang="en" index="1">This dataset describes the electricity available on the medium voltage level in {{location}} for year {{year}}. This is done by showing the transmission of 1kWh electricity at medium voltage.</text>
          <variable xml:lang="en" name="location">Canada, Québec</variable>
        </generalComment>
      </activity>
    </activityDescription>
  </childActivityDataset>
</ecoSpold>
'''


converted_string_ecospold = '''This dataset describes the electricity available on the medium voltage level in Canada, Québec for year 2014. This is done by showing the transmission of 1kWh electricity at medium voltage.
The shares of electricity technologies on this market are valid for the year 2014. They have been implemented by the software layer and don't represent the production volumes in the unlinked datasets valid for the year 2012. These shares have been calculated based on statistics from 2014. Basic source: Statistical tables by StatCan on electricity production in Canada. Further information will be available in a report available on ecoQuery. '''


class XmlWidgetTestCase(unittest.TestCase):
    def test_objectify(self):
        o = objectify.fromstring(ecospold2_sample)
        self.assertEqual(len(o), 1)
        self.assertEqual(len(o['childActivityDataset']), 1)

    def test_find_tags_ecospold(self):
        o = objectify.fromstring(ecospold2_sample)
        self.assertEqual(len(find_tag(o, 'generalComment')), 1)

    def test_string_substitution_ecospold(self):
        o = objectify.fromstring(ecospold2_sample)
        self.assertEqual(render_text_block(find_tag(o, 'generalComment')), converted_string_ecospold)


if __name__ == '__main__':
    unittest.main()
