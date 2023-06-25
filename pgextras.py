#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
from typing import Any

from tabulate import tabulate

from scripts import PgExtras

METHODS = [
    ("bloat", "Table and index bloat in your database ordered by most " "wasteful."),
    ("blocking", "Queries holding locks other queryes are qaiting to be " "releases"),
    (
        "cache_hit",
        "Calculates your cache hit rate (effective databases are at " "99% and up).",
    ),
    (
        "calls",
        "Show 10 most frequently called queries. Requires the " "pg_stat_statements.",
    ),
    (
        "index_usage",
        "Calculates your index hit rate (effective databases are " "at 99% and up).",
    ),
    ("locks", "Display queries with active locks."),
    (
        "long_running_queries",
        "Show all queries longer than five minutes by " "descending duration.",
    ),
    (
        "outliers",
        "Show 10 queries that have longest execution time in "
        "aggregate. Requires the pg_stat_statments.",
    ),
    ("ps", "View active queries with execution time."),
    (
        "seq_scans",
        "Show the count of sequential scans by table descending by " "order.",
    ),
    ("total_index_size", "Show the total size of all indexes."),
    (
        "total_indexes_size",
        "Show the total size of all the indexes on each" "table, descending by size.",
    ),
    (
        "table_size",
        "Show the size of the tables (excluding indexes)," "descending by size.",
    ),
    (
        "total_table_size",
        "Show the size of the tables (including indexes), " "descending by size.",
    ),
    (
        "unused_indexes",
        "Show unused and almost unused indexes, ordered by "
        "their size relative to the number of index scans.",
    ),
    (
        "vacuum_stats",
        "Show dead rows and whether an automatic vacuum is "
        "expected to be triggered.",
    ),
    ("version", "Get the Postgres server version."),
]


def main(args: Any):
    """Main function."""
    with PgExtras(dsn=args.dsn) as pg:
        for method in args.methods:
            try:
                func = getattr(pg, method)
            except AttributeError as error:
                raise SystemExit(1, str(error))

            results = func()

            if not results:
                print(method)
                print("No results found.")
                continue

            column_names = results[0]._fields

            # Create a list of rows with incremental 'id' values
            rows = [[i + 1] + list(row) for i, row in enumerate(results)]

            # Print the table using tabulate
            print(
                tabulate(rows, headers=["id"] + list(column_names), tablefmt="presto")
            )


if __name__ == "__main__":
    left_column_length = 20

    parser = argparse.ArgumentParser(
        description="CLI for PgExtras",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(
            "{}: {} {}".format(k, " " * (left_column_length - len(k)), v)
            for k, v in METHODS
        ),
    )

    parser.add_argument("-dsn", required=True)
    parser.add_argument("-methods", nargs="+", default=["version"])
    main(parser.parse_args())
