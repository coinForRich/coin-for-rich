# This module contains common number helpers

from decimal import Decimal
from typing import Union


def round_decimal(
        number: Union[float, int, Decimal, str],
        n_decimals: int=2
    ) -> Union[Decimal, None]:
    '''
    Rounds a `number` to `n_decimals` decimals

    If number is None, returns None

    :params:
        `number`: float, int or Decimal type or str representing float
        `n_decimals`: number of decimals
    '''

    if number is None:
        return None
    return round(Decimal(number), n_decimals)
