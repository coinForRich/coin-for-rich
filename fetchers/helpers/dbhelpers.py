# This module contains db helpers

import csv
import psycopg2
from io import StringIO


def psql_copy_from_csv(conn, rows, table, cursor=None):
    '''
    Psycopg2 copy from rows to table using StringIO and CSV
    Do nothing when error occurs
    params:
        `conn`: psycopg2 conn obj (required)
        `rows`: iterable of tuples
        `table`: string - table name
        `cursor`: psycopg2 cursor obj
    '''

    own_cursor = False
    if not cursor:
        own_cursor = True
        cursor = conn.cursor()
    try:
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(rows)
        buffer.seek(0)
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
        print(f'PSQL Copy: Successfully copied rows to table {table}')
    except psycopg2.IntegrityError:
        conn.rollback()
    if own_cursor:
        cursor.close()
