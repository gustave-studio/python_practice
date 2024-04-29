import sys
import unittest
import pytest
from unittest.mock import patch
sys.path.append("/Users/fujigaki/python_app/python_practice")
from batch.batch1 import Batch1

def test_send_request1():
    batch = Batch1()
    assert batch.send_request('ダミー') == '返り値ダミー'

@patch.object(Batch1, 'send_request', return_value='ダミーエラー')
def test_send_request2(self):
    batch = Batch1()
    assert batch.send_request('ダミー') == 'ダミーエラー'

@patch.object(Batch1, 'send_request', side_effect=Exception('raise ダミーエラー'))
def test_send_request3(self):
    batch = Batch1()
    with pytest.raises(Exception) as exc_info:
            batch.send_request('ダミー')
    assert str(exc_info.value) == 'raise ダミーエラー'

def mock_side_effect(arg):
    print(f"arg: {arg}")
    if arg == "エラー対象の引数":
        raise Exception('raise ダミーエラー')
    else:
        return "ok"

@patch.object(Batch1, 'send_request', side_effect=mock_side_effect)
def test_send_request4(self):
    batch = Batch1()
    with pytest.raises(Exception) as exc_info:
        batch.send_request("エラー対象の引数")
    assert str(exc_info.value) == 'raise ダミーエラー'

    assert batch.send_request("正常系の引数") == 'ok'
