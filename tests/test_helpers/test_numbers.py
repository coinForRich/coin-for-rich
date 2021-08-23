from decimal import Decimal
from common.helpers.numbers import round_decimal


def test_round_decimal():
    assert round_decimal(Decimal(16.0/7)) == round_decimal(2.29)
