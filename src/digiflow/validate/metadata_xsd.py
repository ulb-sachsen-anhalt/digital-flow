"""module for metadata schema doc validation"""

import os
import pathlib

import lxml.etree as ET

import digiflow.common as dfc


_XDS_RES = pathlib.Path(__file__).parent.parent / 'resources' / 'xsd'
METS_1_12 = os.path.join(_XDS_RES, 'mets-1-12.xsd')
MODS_3_8 = os.path.join(_XDS_RES, 'mods-3-8.xsd')
MODS_3_7 = os.path.join(_XDS_RES, 'mods-3-7.xsd')
MIX_2_0 = os.path.join(_XDS_RES, 'mix-2-0.xsd')
ALTO_3_1 = os.path.join(_XDS_RES, 'alto-3-1.xsd')
ALTO_4_2 = os.path.join(_XDS_RES, 'alto-4-2.xsd')

METS_MODS_XSD = {'mets:mets': [METS_1_12],
                 'mods:mods': [MODS_3_8]}
DEFAULT_XSD_MAPPINGS = {
    'mets:mets': [METS_1_12],
    'mods:mods': [MODS_3_8], #MODS_3_7],
    'mix:mix': [MIX_2_0],
    'alto': [ALTO_4_2],
}

# please linter for lxml
# pylint: disable=c-extension-no-member

class InvalidXMLException(Exception):
    """Mark invalid Validation outcome"""


def _is_schema_root(xml_tree, schema) -> bool:
    """
    Rooot namespace *might* be prefixed, therefore go for tree's QName
    """
    qualified_name = ET.QName(xml_tree)
    local_name = qualified_name.localname
    return local_name in schema


def _is_contained(xml_tree, schema):
    if _is_schema_root(xml_tree, schema):
        return True
    return len(xml_tree.findall('.//' + schema, dfc.XMLNS)) > 0


def _validate(xml_tree, schema, xsd_file):
    if _is_schema_root(xml_tree, schema):
        return _validate_with_xsd(xml_tree, xsd_file)
    _invalids = []
    sections = xml_tree.findall('.//' + schema, dfc.XMLNS)
    for section in sections:
        _invalids.extend(_validate_with_xsd(section, xsd_file))
    return _invalids


def _validate_with_xsd(xml_tree, xsd_file):
    the_tree = ET.parse(xsd_file)
    schema_tree = ET.XMLSchema(the_tree)
    invalids = []
    try:
        schema_tree.assertValid(xml_tree)
    except ET.DocumentInvalid:
        for error in schema_tree.error_log:
            entry = (error.level_name, error.domain_name, error.message)
            invalids.append(entry)
    return invalids


def validate_xml(xml_data, xsd_mappings=None) -> bool:
    """
    Validate XML data with a set of given schema definitions (XSDs)

    :param xml_data: string|PosixPath|ET.etree.root
    """

    if isinstance(xml_data, pathlib.Path):
        xml_data = str(xml_data)
    if isinstance(xml_data, str):
        xml_data = ET.parse(xml_data).getroot()
    if xsd_mappings is None:
        xsd_mappings = DEFAULT_XSD_MAPPINGS
    invalids = []
    for schema, xsd_files in xsd_mappings.items():
        if _is_contained(xml_data, schema):
            for xsd_file in xsd_files:
                invalids.extend(_validate(xml_data, schema, xsd_file))
    if len(invalids) > 0:
        raise InvalidXMLException(invalids)
    return True
