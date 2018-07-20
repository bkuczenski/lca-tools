from lcatools.data_sources.archive_tools import ConfigFlowCharacterization, ConfigAllocation, ConfigBadReference

"""
Note: USLCI is an Ecospold1 archive, which uses a Namespace UUID (uuid3) to generate keys. This allows
process names, flow numeric identifiers, and quantity units to be used as entity references.
"""

'''
Still open:

'''

uslci_flow_characterizations = [
    # Pulp, kraft market, bleached, average production, at mill
    ConfigFlowCharacterization(37086, 'USD', 0.0),  # Electricity, at bleached kraft market pulp mill
    ConfigFlowCharacterization(37088, 'USD', 0.0),  # Steam, at bleached kraft market pulp mill
    ConfigFlowCharacterization(37084, 'USD', 0.0),  # Tall oil, at bleached kraft market pulp mill
    ConfigFlowCharacterization(31355, 'USD', 1.0),  # Pulp, kraft market, bleached, average production, at mill
    ConfigFlowCharacterization(37082, 'USD', 0.0),  # Turpentine, at bleached kraft market pulp mill
    # Rapeseed, whole plant, at field
    ConfigFlowCharacterization(37383, 'USD', 1.0),  # Rapeseed, at field
    ConfigFlowCharacterization(37381, 'USD', 0.0),  # Rapeseed residues, at field
    # Cotton, whole plant, at field
    ConfigFlowCharacterization(16188, 'USD', 1.0),  # Cotton, at field
    ConfigFlowCharacterization(16186, 'USD', 0.0),  # Cotton straw, at field
    # Roundwood, softwood, national forest, steep slope, at forest road, INW
    ConfigFlowCharacterization(13865, 'm3', 0.00278),  # Bark, softwood, national forest, steep slope, at forest road, INW (360 kg/m3)
    # Dry veneer processing, at plywood  plant, US PNW
    ConfigFlowCharacterization(17239, 'USD', 1.0),  # Dry veneer, sold, at plywood plant, US PNW
    ConfigFlowCharacterization(17237, 'USD', 1.0),  # Dry veneer, at plywood plant, US PNW
    # Roundwood, softwood, national forest, gentle slope, at forest road, INW
    ConfigFlowCharacterization(13863, 'm3', 0.00278),  # Bark, softwood, national forest, gentle slope, at forest road, INW
    # Soy oil, refined, at plant
    ConfigFlowCharacterization(40717, 'USD', 1.0),  # Soy oil, refined, at plant
    ConfigFlowCharacterization(40719, 'USD', 1.0),  # Soap stock, at plant
    # Soybean oil, crude, degummed, at plant
    ConfigFlowCharacterization(40655, 'USD', 1.0),  # Soybean oil, crude, degummed, at plant
    ConfigFlowCharacterization(19657, 'USD', 1.0),  # Soy meal, at plant
    # Wheat, at field
    ConfigFlowCharacterization(5528, 'USD', 1.0),  # Wheat grains, at field
    ConfigFlowCharacterization(5526, 'USD', 1.0),  # Wheat straw, at field
    # Roundwood, hardwood, average, High Intensity Management, NE-NC
    ConfigFlowCharacterization(13806, 'm3', 0.00182),  # Bark, hardwood, average, High Intensity Management, NE-NC 550 kg/m3
    # Soybean grains, at field
    ConfigFlowCharacterization(41095, 'USD', 1.0),  # Soybean grains, at field
    ConfigFlowCharacterization(41093, 'USD', 1.0),  # Soybean residues, at field
    # Roundwood, softwood, state-private moist cold forest, gentle slope, at frst rd, INW
    ConfigFlowCharacterization(13854, 'm3', .00278),  # Bark, softwood, state-private moist cold forest, gentle slope, at frst rd, INW [kg]
    # Petroleum refining, at refinery
    ConfigFlowCharacterization(16700, 'kg', 0.809),  # Kerosene, at refinery [l] - 1.236 l/kg or 6.75 lb/gal
    ConfigFlowCharacterization(16696, 'kg', 0.975),  # Residual fuel oil, at refinery - density assumed
    ConfigFlowCharacterization(869, 'kg', 0.750),  # Gasoline, at refinery [l] - density assumed
    ConfigFlowCharacterization(16694, 'kg', 0.5),  # Liquefied petroleum gas, at refinery [l] - density assumed
    ConfigFlowCharacterization(16702, 'kg', 0.737),  # Refinery gas, at refinery [m3] - .046 'lg'/ft3 or 1.357 m3/kg
    ConfigFlowCharacterization(775, 'kg', 0.850),  # Diesel, at refinery [l] - density assumed
    # Roundwood, softwd, state or private dry forest, steep slope, at forest rd, INW
    ConfigFlowCharacterization(13847, 'm3', .00278),  # Bark, softwd, state or private dry forest, steep slope, at forest rd, INW [kg]
    # Soybeans, at field, 1998-2001
    ConfigFlowCharacterization(41521, 'USD', 1.0),  # Soybean grains, at field, 1998-2001 [kg]
    ConfigFlowCharacterization(41519, 'USD', 1.0),  # Soybean residues, at field, 1998-2001 [kg]
    ConfigFlowCharacterization(41523, 'USD', 1.0),  # Soybeans, at field, 1998-2001 [kg]
    # Roundwood, hardwood, average, Med Intensity Management, NE-NC
    ConfigFlowCharacterization(13804, 'm3', .00182),  # Bark, hardwood, average, Med Intensity Management, NE-NC [kg]
    # Roundwood, softwood, average, Med Intensity Management, NE-NC
    ConfigFlowCharacterization(13833, 'm3', .00278),  # Bark, softwood, average, Med Intensity Management, NE-NC [kg]
    # Paper, mechanical, coated, average production, at mill
    ConfigFlowCharacterization(31644, 'USD', 0.0),  # Steam, at coated mechanical paper mill
    ConfigFlowCharacterization(31638, 'USD', 0.0),  # Turpentine, at coated mechanical paper mill
    ConfigFlowCharacterization(31642, 'USD', 0.0),  # Electricity, at coated mechanical paper mill
    ConfigFlowCharacterization(31640, 'USD', 0.0),  # Tall oil, at coated mechanical paper mill
    ConfigFlowCharacterization(31636, 'USD', 1.0),  # Paper, mechanical, coated, average production, at mill
    # Paper, freesheet, uncoated, average production, at mill, 2006
    ConfigFlowCharacterization(31513, 'USD', 0.0),  # Turpentine, at uncoated freesheet mill [kg]
    ConfigFlowCharacterization(31515, 'USD', 0.0),  # Tall oil, at uncoated freesheet mill [kg]
    ConfigFlowCharacterization(31519, 'USD', 0.0),  # Steam, at uncoated freesheet mill [MJ]
    ConfigFlowCharacterization(31505, 'USD', 1.0),  # Paper, freesheet, uncoated, average production, at mill [kg]
    ConfigFlowCharacterization(31517, 'USD', 0.0),  # Electricity, at uncoated freesheet mill [kWh]
    # Roundwood, sftwd, state-private moist cold forest, steep slope, at forest rd,INW
    ConfigFlowCharacterization(13856, 'm3', .00278),  # Bark, sftwd, state-private moist cold forest, steep slope, at forest rd,INW [kg]
    # Roundwood, softwood, average, Low Intensity Management, NE-NC
    ConfigFlowCharacterization(13835, 'm3', .00278),  # Bark, softwood, average, Low Intensity Management, NE-NC [kg]
    # Roundwood, softwd, state or private dry forest, gentle slope, at forest rd, INW
    ConfigFlowCharacterization(13845, 'm3', .00278),  # Bark, softwd, state or private dry forest, gentle slope, at forest rd, INW [kg]
    # Rice, at field
    ConfigFlowCharacterization(37747, 'USD', 1.0),  # Rice grain, at field [kg]
    ConfigFlowCharacterization(37745, 'USD', 1.0),  # Rice straw, at field
    # Roundwood, hardwood, average, Low Intensity Management, NE-NC
    ConfigFlowCharacterization(13802, 'm3', .00182),  # Bark, hardwood, average, Low Intensity Management, NE-NC [kg]
    # Roundwood, softwood, average, High Intensity Management, NE-NC
    ConfigFlowCharacterization(13831, 'm3', .00278),  # Bark, softwood, average, High Intensity Management, NE-NC [kg]
    # Solid strip and plank flooring, hardwood, E
    ConfigFlowCharacterization(40602, 'USD', 1000.0),  # Solid strip and plank flooring, hardwood, E [m3]
    ConfigFlowCharacterization(5831, 'USD', 1.0),  # Wood fuel, hardwood, from flooring production, E [kg]
    # Heat, onsite boiler, softwood mill, average, NE-NC
    ConfigFlowCharacterization(26227, 'MJ', 3.6),  # Electricity, onsite boiler, softwood mill, average, NE-NC [kWh]
    # Paper, mechanical, uncoated, average production, at mill
    ConfigFlowCharacterization(31771, 'USD', 0.0),  # Tall oil, at uncoated mechanical paper mill [kg]
    ConfigFlowCharacterization(31775, 'USD', 0.0),  # Steam, at uncoated mechanical paper mill [MJ]
    ConfigFlowCharacterization(31773, 'USD', 0.0),  # Electricity, at uncoated mechanical paper mill [kWh]
    ConfigFlowCharacterization(31767, 'USD', 1.0),  # Paper, mechanical, uncoated, average production, at mill [kg]
    ConfigFlowCharacterization(31769, 'USD', 0.0),  # Turpentine, at uncoated mechanical paper mill [kg]
    # Potato, whole plant, at field
    ConfigFlowCharacterization(36465, 'USD', 1.0),  # Potato, at field [kg]
    ConfigFlowCharacterization(36463, 'USD', 1.0),  # Potato leaves, at field [kg]
    # Corn, whole plant, at field
    ConfigFlowCharacterization(15776, 'USD', 1.0),  # Corn stover, at field
    ConfigFlowCharacterization(15778, 'USD', 1.0),  # Corn, at field
    # Heat, onsite boiler, hardwood mill, average, SE
    ConfigFlowCharacterization(26199, 'MJ', 3.6),  # Electricity, onsite boiler, hardwood mill, average, SE [kWh]
    # Paper, bag and sack, unbleached kraft, average production, at mill
    ConfigFlowCharacterization(31240, 'USD', 0.0),  # Electricity, at unbleached kraft bag and sack paper mill [kWh]
    ConfigFlowCharacterization(31236, 'USD', 0.0),  # Turpentine, at unbleached kraft bag and sack paper mill [kg]
    ConfigFlowCharacterization(31234, 'USD', 1.0),  # Paper, bag and sack, unbleached kraft, average production, at mill [kg]
    ConfigFlowCharacterization(31238, 'USD', 0.0),  # Tall oil, at unbleached kraft bag and sack paper mill [kg]
    # Heat, onsite boiler, hardwood mill average, NE-NC
    ConfigFlowCharacterization(38905, 'MJ', 3.6),  # Electricity, onsite boiler, hardwood mill, average, NE-NC [kWh]
    # Paper, freesheet, coated, average production, at mill
    ConfigFlowCharacterization(31387, 'USD', 0.0),  # Electricity, at coated freesheet mill [kWh]
    ConfigFlowCharacterization(31389, 'USD', 0.0),  # Steam, at coated freesheet mill [MJ],
    ConfigFlowCharacterization(31381, 'USD', 1.0),  # Paper, freesheet, coated, average production, at mill [kg],
    ConfigFlowCharacterization(31383, 'USD', 0.0),  # Turpentine, at coated freesheet mill [kg]
    ConfigFlowCharacterization(31385, 'USD', 0.0)   # Tall oil, at coated freesheet mill [kg]
]

uslci_allocations = [
    ConfigAllocation('Crude oil, in refinery', 'kg'),
    ConfigAllocation('Petroleum refining, at refinery', 'kg')
]

uslci_all_allocations = [
    ConfigAllocation('Ethanol, denatured, forest residues, thermochem', 'kg'),
    ConfigAllocation('Sawn lumber, softwood, rough, green, at sawmill, NE-NC', 'kg'),
    ConfigAllocation('Sawn lumber, hardwood, rough, green, at sawmill, SE', 'kg'),
    ConfigAllocation('Pulp, kraft market, bleached, average production, at mill', 'USD'),
    ConfigAllocation('Rapeseed, whole plant, at field', 'USD'),
    ConfigAllocation('Cotton, whole plant, at field', 'USD'),
    ConfigAllocation('Composite wood I-joist processing, at plant, US PNW', 'kg'),
    ConfigAllocation('Roundwood, softwood, national forest, steep slope, at forest road, INW', 'm3'),
    ConfigAllocation('Dry veneer processing, at plywood  plant, US PNW', 'USD'),
    ConfigAllocation('Trim and saw process, at plywood plant, US PNW', 'kg'),
    ConfigAllocation('Roundwood, softwood, national forest, gentle slope, at forest road, INW', 'm3'),
    ConfigAllocation('Veneer, hardwood, green, at veneer mill, E', 'kg'),
    ConfigAllocation('Soy oil, refined, at plant', 'USD'),
    ConfigAllocation('Soybean oil, crude, degummed, at plant', 'USD'),
    ConfigAllocation('Wheat, at field', 'USD'),
    ConfigAllocation('Roundwood, hardwood, average, High Intensity Management, NE-NC', 'm3'),
    ConfigAllocation('Soy biodiesel, production, at plant', 'kg'),
    ConfigAllocation('Chlorine, production mix, at plant', 'kg'),
    ConfigAllocation('Soybean grains, at field', 'USD'),
    ConfigAllocation('Roundwood, softwood, state-private moist cold forest, gentle slope, at frst rd, INW', 'm3'),
    ConfigAllocation('Sawn lumber, hardwood, rough, green, at sawmill, NE-NC', 'kg'),
    ConfigAllocation('Planed dried lumber processing, at planer mill, US PNW', 'kg'),
    ConfigAllocation('Petroleum refining, at refinery', 'kg'),
    ConfigAllocation('Roundwood, softwd, state or private dry forest, steep slope, at forest rd, INW', 'm3'),
    ConfigAllocation('Soybeans, at field, 1998-2001', 'USD'),
    ConfigAllocation('Planed dried lumber processing, at planer mill, US SE', 'kg'),
    ConfigAllocation('Roundwood, hardwood, average, Med Intensity Management, NE-NC', 'm3'),
    ConfigAllocation('Roundwood, softwood, average, Med Intensity Management, NE-NC', 'm3'),
    ConfigAllocation('Paper, mechanical, coated, average production, at mill', 'USD'),
    ConfigAllocation('Paper, freesheet, uncoated, average production, at mill, 2006', 'USD'),
    ConfigAllocation('Roundwood, sftwd, state-private moist cold forest, steep slope, at forest rd,INW', 'm3'),
    ConfigAllocation('Roundwood, softwood, average, Low Intensity Management, NE-NC', 'm3'),
    ConfigAllocation('Roundwood, softwd, state or private dry forest, gentle slope, at forest rd, INW', 'm3'),
    ConfigAllocation('Rice, at field', 'USD'),
    ConfigAllocation('Roundwood, hardwood, average, Low Intensity Management, NE-NC', 'm3'),
    ConfigAllocation('Roundwood, softwood, average, High Intensity Management, NE-NC', 'm3'),
    ConfigAllocation('Crude oil, in refinery', 'kg'),
    ConfigAllocation('Oriented strand board processsing, at plant, US SE', 'kg'),
    ConfigAllocation('Solid strip and plank flooring, hardwood, E', 'USD'),
    ConfigAllocation('Heat, onsite boiler, softwood mill, average, NE-NC', 'MJ'),
    ConfigAllocation('Paper, mechanical, uncoated, average production, at mill', 'USD'),
    ConfigAllocation('Potato, whole plant, at field', 'USD'),
    ConfigAllocation('Ethanol, denatured, corn dry mill', 'kg'),
    ConfigAllocation('Corn, whole plant, at field', 'USD'),
    ConfigAllocation('Planed green lumber processing, at planer mill, US PNW', 'kg'),
    ConfigAllocation('Heat, onsite boiler, hardwood mill, average, SE', 'MJ'),
    ConfigAllocation('Rough green lumber processing, at sawmill, US PNW', 'kg'),
    ConfigAllocation('Paper, bag and sack, unbleached kraft, average production, at mill', 'USD'),
    ConfigAllocation('Heat, drying veneer, hardwood, at veneer mill, E', 'MJ'),
    ConfigAllocation('Heat, onsite boiler, hardwood mill average, NE-NC', 'MJ'),
    ConfigAllocation('Rough green lumber processing, at sawmill, US SE', 'kg'),
    ConfigAllocation('Paper, freesheet, coated, average production, at mill', 'USD')
]


uslci_bad_references = [
    ConfigBadReference('Laminated veneer lumber processing, at plant, US SE', 27265, 'Output'),
    ConfigBadReference(None, 9218, 'Output'),  # recovered energy, petrochemical manufacturing

]
"""
The following USLCI processes had their co-products removed from the reference entity, treating them as cut-offs:

Laminated veneer lumber processing, at plant, US SE (coproducts, unspecified)
Sawn lumber, softwood, rough, green, at sawmill, NE-NC (4 different wastes)
Sawn lumber, hardwood, rough, green, at sawmill, SE (5 different wastes)
Vacuum infusion, rigid composites part, at plant (composite scrap)
Ethylene, at plant (recovered energy)
Polyethylene, low density, resin, at plant, CTR (recovered energy)
Thermoforming, rigid polypropylene part, at plant (OCC packaging, PP scrap)
Lost foam casting, aluminum (Aluminum scrap)
Methylene diphenyl diisocyanate, resin, at plant (recovered energy)
Propylene, at plant (recovered energy)
Trim and saw process, at plywood plant, US PNW (hogfuel, sawdust)
Open mold casting, rigid composites part, at plant (composite scrap)
Methylene diphenyl diisocyanate resin, at plant, CTR (recovered energy)
Medium density fiberboard (MDF), at MDF mill (wood waste, bark, wood fuel burned on site)
Sawn lumber, softwood, rough, green, at sawmill, INW (5 different wastes)
Acrylonitrile-butadiene-styrene copolymer resin, at plant, CTR (recovered energy)
Engineered flooring, hardwood, unfinished, E (shavings, sawdust, wood fuel
Debarking, at plywood plant, US SE (bark)
Polystyrene, general purpose, at plant, CTR (recovered energy)
Acrylonitrile-butadiene-styrene copolymer, resin, at plant (recovered energy)
Glue laminated beam processing, at plant, US SE (coproducts, unspecified)
Polyethylene, high density, resin, at plant, CTR (recovered energy)
Trim and saw process, at plywood plant, US SE (wood wastes)
Sawn Lumber, softwood, planed, kiln dried, at planer mill, INW (wood chips, shavings)
Roundwood, hardwood, green, at mill, NE-NC (bark -- closed coproduct)
Glue laminated beam processing, at plant, US PNW (coproducts, unspecified)
Sawn lumber, hardwood, planed, kiln dried, at planer mill, NE-NC (4 wood wastes)
Veneer, hardwood, dry, at veneer mill, E (fuel, clippings)
Sulfur, at plant (recovered energy)
Unsaturated polyester, resin, at plant (polyester scrap)
Polyethylene terephthalate, resin, at plant, CTR (recovered energy)
Debarking, at plywood plant, US PNW (bark)
Precision sand casting, aluminum (aluminum scrap, unspecified coproduct)
Sawn lumber, hardwood, planed, kiln dried, at planer mill, SE (3 wood wastes)
Particleboard, average, softwood, particleboard mill (3 wastes, incl. 2 closed coproducts)
Sawn lumber, hardwood, rough, green, at sawmill, NE-NC (3 wastes incl 1 closed; leaving 2 used for MDF)
Planed dried lumber processing, at planer mill, US PNW (sawdust, leaving 2 wastes)
Polyol ether, for flexible foam polyurethane production, at plant, CTR (recovered energy)
Sulfuric acid, at plant (recovered energy)
Petroleum refining, at refinery (removed Petroleum refining, at refinery semantic flow)
Steel, stainless 304, quarto plate (Secondary fuel renewable)
Toluene diisocyanate, at plant, CTR (recovered energy)
Toluene diisocyanate, at plant (recovered energy)
Corrugated Product (board trimmings and rejects for recyc)
Acetic acid, at plant (recovered energy)
Planed dried lumber processing, at planer mill, US SE (sawdust, leaving shavings)
Polyethylene, low density, resin, at plant (recovered energy)
Roundwood, softwood, green, at mill, NE-NC (bark - closed coproduct)
Injection molding, rigid polypropylene part, at plant (OCC packaging, scrap)
Ethylene glycol, at plant (recovered energy)
Polyethylene terephthalate, resin, at plant (recovered energy)
Roundwood, hardwood, green, at logyard, SE (bark - closed coproduct)
Compression molding, rigid composites part, at plant (OCC packaging, scrap)
Sawn lumber, softwood, planed, kiln dried, at planer, NE-NC (2 wastes)
Green veneer processing, at plywood plant, US PNW (3 wastes)
Acrylonitrile, at plant (recovered energy)
Steel, cold-formed studs and track, at plant (3 cutoffs)
Composite wood I-joist processing, at plant, US SE (sawdust)
Semi-permanent mold (SPM) casting, aluminum (aluminum scrap, byproduct)
Roundwood, hardwood, green, at mill, SE (closed coproduct)
Green veneer processing, at plywood plant, US SE (3 wastes)
Aniline, at plant (recovered energy)
Roundwood, hardwood, green, at logyard, NE-NC (bark - closed coproduct)
Polystyrene, high impact, resin, at plant, CTR (recovered energy)
Oriented strand board processsing, at plant, US SE (2 wastes, leaving fines used by particleboard)
Laminated veneer lumber processing, at plant, US PNW (coproduct, unspecified)
Roundwood, softwood, green, at logyard, NE-NC (bark - closed coproduct)
Polyol ether, for flexible foam polyurethane production, at plant (recovered energy)
Roundwood, softwood, green, at logyard, INW (bark - closed coproduct)
Polyethylene, linear low density, resin, at plant, CTR (recovered energy)
Polyvinyl chloride, resin, at plant, CTR (recovered energy)
Polyol ether, for rigid foam polyurethane production, at plant
Polypropylene, resin, at plant, CTR (recovered energy)
Planed green lumber processing, at planer mill, US PNW (two wastes; kept one)
Rough green lumber processing, at sawmill, US PNW (bark; kept 2 MDF/particleboard wastes)
Open molding, rigid composites part, at plant (4 wastes)
Rough green lumber processing, at sawmill, US SE (Bark; kept 2 MDF/particleboard wastes)
"""

