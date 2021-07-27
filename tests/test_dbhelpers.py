import psycopg2
from psycopg2 import sql, extras
from common.config.constants import DBCONNECTION
from fetchers.config.queries import PSQL_INSERT_UPDATE_QUERY, PSQL_INSERT_IGNOREDUP_QUERY
from fetchers.helpers.dbhelpers import psql_bulk_insert


def test_dbhelper():
    conn = psycopg2.connect(DBCONNECTION)
    cur = conn.cursor()
    table = "test"
    rows = (
        (1, 'a', 'b', 351235, 3.14),
        (2, 'b', 'c', 1001, 9001),
        (5, 'a', 'q', 54, 97)
    )
    unique_cols = ("id", "b", "q")
    update_cols = ("o", "c")
    assert psql_bulk_insert(
        conn,
        rows,
        table,
        insert_ignoredup_query = PSQL_INSERT_IGNOREDUP_QUERY
        # unique_cols = unique_cols,
        # update_cols = update_cols,
    ) == True
