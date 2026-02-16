"""Test specification for Record class"""

import json

import pytest

import digiflow.record as df_r

# pylint: disable=c-extension-no-member, line-too-long


@pytest.mark.parametrize(
    ["urn", "local_identifier"],
    [
        ("oai:digital.bibliothek.uni-halle.de/hd:10595", "10595"),
        ("oai:digitale.bibliothek.uni-halle.de/vd18:9427342", "9427342"),
        ("oai:opendata.uni-halle.de:1981185920/34265", "1981185920_34265"),
        ("oai:dev.opendata.uni-halle.de:123456789/27949", "123456789_27949"),
    ],
)
def test_record_local_identifiers(urn, local_identifier):
    """Ensure local identifier for different URN inputs"""

    # act
    record = df_r.Record(urn)
    assert record.local_identifier == local_identifier
    assert record.info == {}


def test_record_update_info_set_input():
    """Prevent TypeError for input info alike
    {'vd17': '3:607751D', 'mps': [(3.5, 304), (3.6, 455), (3.7, 184)],
      'ocr_loss': {'n.a.', '00000838'}}
    """

    the_urn = "oai:opendata2.uni-halle.de:1516514412012/27399"
    record = df_r.Record(the_urn)
    the_info = {
        "vd17": "3:607751D",
        "mps": [(3.5, 304), (3.6, 455), (3.7, 184)],
        "ocr_loss": {"n.a.", "00000838"},
    }
    record.info = the_info

    # act
    with pytest.raises(df_r.RecordDataException) as data_exc:
        json.dumps(record.dict())

    # assert
    assert record.info["ocr_loss"] == {"n.a.", "00000838"}
    assert "Object of type set is not JSON serializable" in data_exc.value.args[0]


def test_record_update_info_valid_input():
    """Prevent TypeError for info string alike
    {'vd17': '3:607751D', 'urn': 'urn:nbn:de:gbv:3:1-42926',
      'mps': [(3.5, 304), (3.6, 455), (3.7, 184)],
      'ocr_loss': {'n.a.', '00000838'}, 'n_execs': '8'}
    """

    the_urn = "oai:opendata2.uni-halle.de:1516514412012/27399"
    record = df_r.Record(the_urn)
    the_info = {
        "vd17": "3:607751D",
        "urn": "urn:nbn:de:gbv:3:1-42926",
        "n_images_ocrable": 943,
        "mps": [(3.5, 304), (3.6, 455), (3.7, 184)],
        "ocr_loss": ["n.a.", "00000838"],
        "n_execs": "8",
    }
    record.info = the_info

    # act
    serialized_info = json.dumps(record.dict())

    # assert
    assert '"ocr_loss": ["n.a.", "00000838"]' in serialized_info
