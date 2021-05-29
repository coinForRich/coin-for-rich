# This module contains db helpers

import csv
from io import StringIO


def psql_copy_from_csv(cursor, rows, table):
    '''
    Psycopg2 copy from rows to table using StringIO and CSV
    Do nothing when error occurs
    params:
        `cursor`: psycopg2 cursor obj
        `rows`: tuples of tuples
        `table`: string - table name
    '''

    try:
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(rows)
        buffer.seek(0)
        cursor.copy_from(buffer, table, sep=",")
    except Exception as exc:
        print(f'Exception {exc} occurred when copying rows to {table}')
        pass
