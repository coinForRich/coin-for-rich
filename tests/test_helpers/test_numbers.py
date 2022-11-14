import pytest
from decimal import Decimal
from common.helpers.numbers import round_decimal


@pytest.mark.beforepop
def test_round_decimal():
    assert round_decimal(Decimal(16.0/7)) == round_decimal(2.29)
    assert round_decimal(0) == Decimal(0)
    assert round_decimal(None) is None
    assert round_decimal("1.5") == 1.5
