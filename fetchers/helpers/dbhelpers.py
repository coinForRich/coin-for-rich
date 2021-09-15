# This module contains db helpers

import sys
import csv
import logging
import psycopg2
from psycopg2 import sql, extras
from typing import Iterable
from io import StringIO


def log_psycopg2_exc(err: Exception) -> None:
    '''
    Logs details about a psycopg2 Exception
    '''
    
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno

    # print the connect() error
    logging.error("\npsycopg2 ERROR:", err, "on line number:", line_num)
    logging.error("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    # print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    logging.error("pgerror:", err.pgerror)
    logging.error("pgcode:", err.pgcode, "\n")

def psql_bulk_insert(
        conn,
        rows: Iterable,
        table: str,
        insert_update_query: str = None,
        insert_ignoredup_query: str = None,
        unique_cols: tuple = None,
        update_cols: tuple = None,
        cursor = None
    ) -> bool:
    '''
    Bulk inserts `rows` to `table` using StringIO and CSV;
        
    On conflict, either ignores or updates new values;
        
    Also uses `page_size` of 1000 for inserting;
        
    Returns a boolean value indicating whether insert is successful

    :params:
        `conn`: psycopg2 conn obj
        `rows`: iterable of tuples
        `table`: string - table name
        `insert_update_query`: string - insert-update query to `table`,\n
                in case the copy method fails; this query must have a\n
                `{table}` placeholder, 3 placeholders for unique columns,\n
                update columns, and "excluded" columns
        `insert_ignoredup_query`: string - insert-ignoredup query to `table`,\n
                in case the copy method fails; this query must have a `{table}` placeholder
        `unique_cols`: tuple of strings with column names where unique constraint exists
        `update_cols`: tuple of strings with column names to update data on
        `cursor`: psycopg2 cursor obj (optional)

    Note: `insert_update_query` is prioritized over `insert_ignoredup_query` if both are entered
    '''

    if insert_update_query is None and insert_ignoredup_query is None:
        # Raise exception immediately
        raise ValueError(
            "PSQL Bulk Insert: Either insert-update query or insert-ignoredup query must be provided"
        )
    else:
        if not cursor:
            cursor = conn.cursor()
        try:
            buffer = StringIO()
            writer = csv.writer(buffer)
            writer.writerows(rows)
            buffer.seek(0)
            cursor.copy_from(buffer, table, sep=",", null="")
            conn.commit()
            logging.info(f'PSQL Bulk Insert: Successfully copied rows to table {table}')
            return True
        except psycopg2.IntegrityError:
            conn.rollback()
            if insert_update_query is not None:
                logging.info(
                    f"PSQL Bulk Insert: Performing insert with update to table {table}"
                )
                ex_cols = ("excluded" for _ in update_cols)

                if len(update_cols) > 1:
                    insert_query = sql.SQL(insert_update_query).format(
                        sql.SQL(", ").join(map(sql.Identifier, unique_cols)),
                        sql.SQL("(") + sql.SQL(", ").join(map(sql.Identifier, update_cols)) + sql.SQL(")"),
                        sql.SQL("(") + sql.SQL(", ").join(map(sql.Identifier, ex_cols, update_cols)) + sql.SQL(")"),
                        table = sql.Identifier(table)
                    )
                else:
                    # This should enable single-column updates
                    insert_query = sql.SQL(insert_update_query).format(
                        sql.SQL(", ").join(map(sql.Identifier, unique_cols)),
                        sql.SQL(", ").join(map(sql.Identifier, update_cols)),
                        sql.SQL(", ").join(map(sql.Identifier, ex_cols, update_cols)),
                        table = sql.Identifier(table)
                    )

            elif insert_ignoredup_query is not None:
                logging.info(
                    f"PSQL Bulk Insert: Performing insert without update to table {table}"
                )
                insert_query = sql.SQL(insert_ignoredup_query).format(
                    table = sql.Identifier(table)
                )
            extras.execute_values(cursor, insert_query, rows, page_size=1000)
            conn.commit()
            logging.info(f'PSQL Bulk Insert: Successfully inserted rows to table {table}')
            return True
        # Is it fine to catch ANY exception?
        except Exception as exc:
            conn.rollback()
            logging.warning(f'PSQL Bulk Insert: EXCEPTION: \n')
            log_psycopg2_exc(exc)
            return False
            # Not want to raise exception here
            #   because this function is used in mass-fetching
            # raise exc
        finally:
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

