# This module contains db helpers

import csv
import psycopg2
from psycopg2 import sql, extras
from io import StringIO


def psql_bulk_insert(
        conn,
        rows: tuple,
        table: str,
        insert_update_query: str = None,
        insert_ignoredup_query: str = None,
        unique_cols: tuple = None,
        update_cols: tuple = None,
        cursor = None
    ):
    '''
    Bulk inserts `rows` to `table` using StringIO and CSV,
        updates with new values on conflict;
        
    Also uses `page_size` of 1000 for inserting;
        
    Returns a boolean value indicating whether insert
        is successful

    :params:
        `conn`: psycopg2 conn obj
        `rows`: iterable of tuples
        `table`: string - table name
        `insert_update_query`: string - insert-update query to `table`,
            in case the copy method fails; this query must have a
            `{table}` placeholder, 3 placeholders for unique columns,
            update columns, and "excluded" columns
        `insert_ignoredup_query`: string - insert-ignoredup query to `table`,
            in case the copy method fails; this query must have a `{table}` placeholder
        `unique_cols`: tuple of strings with column names where unique constraint exists
        `update_cols`: tuple of strings with column names to update data on
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
        if cursor:
            cursor.close()
        return True
    except psycopg2.IntegrityError:
        conn.rollback()
        if insert_update_query is not None:
            print("PSQL Bulk Insert: Performing insert with update")
            ex_cols = ("excluded" for _ in update_cols)
            insert_query = sql.SQL(insert_update_query).format(
                sql.SQL(", ").join(map(sql.Identifier, unique_cols)),
                sql.SQL(", ").join(map(sql.Identifier, update_cols)),
                sql.SQL(", ").join(map(sql.Identifier, ex_cols, update_cols)),
                table = sql.Identifier(table)
            )
        elif insert_ignoredup_query is not None:
            print("PSQL Bulk Insert: Performing insert without update")
            insert_query = sql.SQL(insert_ignoredup_query).format(
                table = sql.Identifier(table)
            )
        else:
            if cursor:
                cursor.close()
            raise ValueError(
                "PSQL Bulk Insert: Either insert-update query or insert-ignoredup query must be provided"
            )
        extras.execute_values(cursor, insert_query, rows, page_size=1000)
        conn.commit()
        print(f'PSQL Bulk Insert: Successfully inserted rows to table {table}')
        if cursor:
            cursor.close()
        return True
    except Exception as exc:
        print(f'PSQL Bulk Insert: EXCEPTION: {exc}')
        conn.rollback()
        if cursor:
            cursor.close()
        raise exc

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

