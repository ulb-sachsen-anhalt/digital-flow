"""Helper to trigger DDB-Validation for digitized prints and newspaper issues/additionals
cf. https://github.com/Deutsche-Digitale-Bibliothek/ddb-metadata-schematron-validation
"""

import os
import typing

from pathlib import Path

# trigger saxon API
SAXON_ENABLED = True
try:
    from saxonche import (
        PySaxonProcessor,
    )
except ModuleNotFoundError:
    SAXON_ENABLED = False


_DEFAULT_RESULT_FILE = 'result_xslt.xml'


class DigiflowTransformException(Exception):
    """Mark Missing Transformation Result"""


def transform(path_input, path_template, path_result=None, post_process=None) -> Path:
    """Low-level API to apply XSLT-Transformation
    like Schematron Validation Report Language XSLT
    on given input file and store outcome in local
    result file"""

    if not isinstance(path_input, str):
        path_input = str(path_input)
    if not isinstance(path_template, str):
        path_template = str(path_template)
    if path_result is None:
        _the_dir = os.path.dirname(path_input)
        path_result = os.path.join(_the_dir, _DEFAULT_RESULT_FILE)
    if not isinstance(path_result, str):
        path_result = str(path_result)
    try:
        with PySaxonProcessor() as proc:
            xsltproc = proc.new_xslt30_processor()
            xslt_exec = xsltproc.compile_stylesheet(stylesheet_file=path_template)
            xslt_exec.transform_to_file(source_file=path_input,
                                        output_file=path_result)
    except Exception as any_exc:
        raise DigiflowTransformException(any_exc) from any_exc
    if not os.path.isfile(path_result):
        raise DigiflowTransformException(f"missing result {path_result}")
    if post_process is not None:
        return post_process(path_input, proc, path_result)
    return Path(path_result).resolve()


def evaluate(path_input, xpr):
    """Use extended XPath 2+ evaluation (like from DDB)
    to evaluate XPath-Expression against given input
    """

    if not isinstance(path_input, str):
        path_input = str(path_input)
    try:
        with PySaxonProcessor() as proc:
            the_doc = proc.parse_xml(xml_file_name=path_input)
            xpath_proc = proc.new_xpath_processor()
            xpath_proc.set_context(xdm_item=the_doc)
            return xpath_proc.evaluate(xpr)
    except Exception as any_exc:
        raise DigiflowTransformException(any_exc) from any_exc
