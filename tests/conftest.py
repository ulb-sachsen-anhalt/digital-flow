import os

from pathlib import Path

import pytest

import digiflow as df
import digiflow.record as df_r


__FILE_ABS_PATH__ = os.path.abspath(__file__)
TEST_ROOT = Path(os.path.dirname(__FILE_ABS_PATH__))
TEST_RES = TEST_ROOT / 'resources'
LIB_RES = Path(__FILE_ABS_PATH__).parent.parent / 'src' / 'digiflow' / 'resources'

LEGACY_HEADER_STR = '\t'.join(df_r.LEGACY_HEADER) + '\n'
RECORD_HEADER_STR = '\t'.join(df_r.RECORD_HEADER) + '\n'



def write_datalist(path_data_list, data, headers=LEGACY_HEADER_STR):
    with open(str(path_data_list), 'w', encoding='utf8') as handle:
        if headers:
            handle.write(headers+"\n")
        handle.writelines(data)


@pytest.fixture(name="oai_record_list")
def fixture_oai_record_list(tmp_path):
    path_state_list = tmp_path / 'ocr_list'
    data = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853011\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tocr_skip\t2021-08-03_15:03:56\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:17320046\tn.a.\t2021-09-01T15:25:43Z\t17320046,issue,ger,20\tupload_done\t2021-09-09_22:57:45\n"
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tocr_skip\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tocr_skip\t2021-08-03_15:20:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\tn.a.\t2015-08-25T20:00:35Z\t9510507,issue,[('9510507', 'issue'), ('n.a.', 'section'), ('8849883', 'year'), ('9059307', 'newspaper')],,None\tocr_done\t2021-08-03_16:44:54\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510508\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    write_datalist(path_state_list, data, LEGACY_HEADER_STR)
    return path_state_list
