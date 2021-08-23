# This module contains common numbers helpers

from decimal import Decimal
from typing import Union


def round_decimal(
        number: Union[float, int, Decimal],
        n_decimals: int=2
    ) -> Decimal:
    '''
    Rounds a `number` to `n_decimals` decimals
    '''

    return round(Decimal(number), n_decimals)
