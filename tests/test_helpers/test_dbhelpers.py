import logging
import psycopg2
from psycopg2 import sql, extras
from typing import Any
from common.config.constants import DBCONNECTION
from common.helpers.numbers import round_decimal
from fetchers.config.queries import PSQL_INSERT_UPDATE_QUERY, PSQL_INSERT_IGNOREDUP_QUERY
from fetchers.helpers.dbhelpers import psql_bulk_insert


def test_dbhelper():
    def run_query(cur: Any, query: str):
        try:
            cur.execute(query)
            results = cur.fetchall()
        except Exception as exc:
            logging.warning(exc)
            raise exc
        return results

    conn = psycopg2.connect(DBCONNECTION)
    cur = conn.cursor()
    table = "test"
    rows = (
        (1, 'a', 'b', round_decimal(351235), round_decimal(3.14)),
        (2, 'b', 'c', round_decimal(1001), round_decimal(9001)),
        (5, 'a', 'q', round_decimal(54), round_decimal(97))
    )
    unique_cols = ("id", "b", "q")
    update_cols = ("c", "o") # Order does not matter

    query = "select * from test where id=1;"
    
    # Insert update
    assert psql_bulk_insert(
        conn,
        rows,
        table,
        insert_update_query = PSQL_INSERT_UPDATE_QUERY,
        unique_cols = unique_cols,
        update_cols = update_cols,
    ) == True

    # Query from
    results = run_query(cur, query)
    assert results[0] == rows[0]
    
    # Insert ignore dups
    assert psql_bulk_insert(
        conn,
        rows,
        table,
        insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
    ) == True

    # Query from
    results = run_query(cur, query)
    assert results[0] == rows[0]
