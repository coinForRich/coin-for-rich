# This module contains db helpers

import csv
import psycopg2
from psycopg2 import sql, extras
from io import StringIO


def psql_bulk_insert(conn, rows, table, insert_query, cursor=None):
    '''
    Bulk inserts rows to `table` using StringIO and CSV
    Also uses `page_size` of 1000 for inserting
    :params:
        `conn`: psycopg2 conn obj
        `rows`: iterable of tuples
        `table`: string - table name
        `insert_query`: string - insert query to `table`,
            in case the copy method fails; this query
            must have a `{table}` placeholder
        `cursor`: psycopg2 cursor obj (optional)
    '''

    if not cursor:
        cursor = conn.cursor()
    try:
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(rows)
        buffer.seek(0)
        cursor.copy_from(buffer, table, sep=",", null="")
        conn.commit()
        print(f'PSQL Bulk Insert: Successfully copied rows to table {table}')
    except psycopg2.IntegrityError:
        conn.rollback()
        insert_query = sql.SQL(insert_query).format(
                table = sql.Identifier(table)
        )
        extras.execute_values(cursor, insert_query, rows, page_size=1000)
        conn.commit()
        print(f'PSQL Bulk Insert: Successfully inserted rows to table {table}')
    except Exception as exc:
        print(f'PSQL Bulk Insert: EXCEPTION: {exc}')
        conn.rollback()
    if cursor:
        cursor.close()

def psql_query_format(query, *args):
    '''
    Returns a formatted SQL query in
        psycopg2 format
    :params:
        `query`: string - a query with **only**
            positional placeholders (i.e., `{}`)
        `*args`: any - position arguments to
            put into `query`
    '''
    
    return sql.SQL(query).format(
        *(sql.Identifier(arg) for arg in args)
    )

