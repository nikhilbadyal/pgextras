"""Entrypoint."""
import re
from collections import namedtuple
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

import psycopg2
import psycopg2.extras
from loguru import logger
from packaging.version import parse as parse_version

from . import sql_constants as sql

if TYPE_CHECKING:
    from psycopg2._psycopg import connection, cursor

postgres_9 = "9.2.0"
postgres_13 = "13"


class PgExtras:
    """Base Class for Utils."""

    def __init__(self: Self, dsn: str, logquery: bool = False, truncate: bool = True) -> None:  # noqa: FBT001,FBT002
        self.dsn = dsn
        self._pg_stat_statement: None | bool = None
        self._cursor: cursor | None = None
        self._conn: connection | None = None
        self._is_pg_at_least_nine_two: None | bool = None
        self._is_pg_at_least_thirteen: None | bool = None
        self.log_query = logquery
        self.truncate = truncate

    def __enter__(self: Self) -> Self:
        """To use with clause."""
        return self

    def __exit__(
        self: Self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Exit from with clause."""
        self.close_db_connection()

    @property
    def cursor(self: Self) -> Any:
        """Return the cursor."""
        if self._cursor is None:
            self._conn = psycopg2.connect(self.dsn, cursor_factory=psycopg2.extras.NamedTupleCursor)

            self._cursor = self._conn.cursor()

        return self._cursor

    @property
    def query_column(self: Self) -> str:
        """PG9.2 changed column names.

        :returns: str
        """
        if self.is_pg_at_least_nine_two():
            return "query"
        return "current_query"

    @property
    def time_column(self: Self) -> str:
        """PG9.2 changed column names.

        :returns: str
        """
        if self.is_pg_at_least_thirteen():
            return "total_exec_time"
        return "total_time"

    @property
    def pid_column(self: Self) -> str:
        """PG9.2 changed column names.

        :returns: str
        """
        if self.is_pg_at_least_nine_two():
            return "pid"
        return "procpid"

    def truncate_query(self: Self, column_name: str) -> str:
        """Truncate long query."""
        if self.truncate:
            return f"""
                CASE WHEN length({column_name}) < 120
                    THEN {column_name}
                    ELSE substr({column_name}, 0, 120) || '..'
                END
            """
        return column_name

    def pg_stat_statement(self: Self) -> bool:
        """Some queries require the pg_stat_statement module to be installed.

        :returns: boolean
        """
        if self._pg_stat_statement is None:
            results = self.execute(sql.PG_STAT_STATEMENT)
            is_available = results[0].available  # type: ignore[attr-defined]

            if is_available:
                self._pg_stat_statement = True
            else:
                self._pg_stat_statement = False

        return self._pg_stat_statement

    def get_missing_pg_stat_statement_error(self: Self) -> Any:
        """Missing pg stats statement."""
        Record = namedtuple("Record", "error")  # noqa: PYI024
        error = """
            pg_stat_statements extension needs to be installed in the
            public schema first. This extension is only available on
            Postgres versions 9.2 or greater. You can install it by
            adding pg_stat_statements to shared_preload_libraries in
            postgresql.conf, restarting postgres and then running the
            following sql statement in your database:
            CREATE EXTENSION pg_stat_statements;
        """

        return Record(error)

    def is_pg_at_least_nine_two(self: Self) -> bool:
        """Some queries have different syntax depending what version of postgres we are querying against.

        :returns: boolean
        """
        if self._is_pg_at_least_nine_two is None:
            results = self.version()
            regex = re.compile(r"PostgreSQL (\d+(\.\d+)+) on")
            matches = regex.match(results[0].version)  # type: ignore[attr-defined]
            version = matches.groups()[0]  # type: ignore[union-attr]

            if parse_version(version) > parse_version(postgres_9):
                self._is_pg_at_least_nine_two = True
            else:
                self._is_pg_at_least_nine_two = False

        return self._is_pg_at_least_nine_two

    def is_pg_at_least_thirteen(self: Self) -> bool:
        """Some queries have different syntax depending what version of postgres we are querying against.

        :returns: boolean
        """
        if self._is_pg_at_least_thirteen is None:
            results = self.version()
            regex = re.compile(r"PostgreSQL (\d+(\.\d+)+) on")
            matches = regex.match(results[0].version)  # type: ignore[attr-defined]
            version = matches.groups()[0]  # type: ignore[union-attr]

            if parse_version(version) >= parse_version("13"):
                self._is_pg_at_least_thirteen = True
            else:
                self._is_pg_at_least_thirteen = False

        return self._is_pg_at_least_thirteen

    def close_db_connection(self: Self) -> None:
        """Close database connection."""
        if self._cursor is not None:
            self._cursor.close()  # type: ignore[no-untyped-call]

        if self._conn is not None:
            self._conn.close()

    def execute(self: Self, statement: str) -> list[tuple[Any, ...]]:
        """Execute the given sql statement.

        :param statement: sql statement to run
        :returns: list
        """
        # Make the sql statement easier to read in case some of the queries we
        # run end up in the output
        sql = statement.replace("\n", "")
        sql = " ".join(sql.split())
        if self.log_query:
            logger.debug(sql)
        self.cursor.execute(sql)
        return self.cursor.fetchall()  # type: ignore[no-any-return]

    def cache_hit(self: Self) -> list[tuple[Any, ...]]:
        """Calculates your cache hit rate (effective databases are at 99% and up).

        Record(     name='index hit rate', ratio=Decimal('0.99994503346970922117') )

        :returns: list of Records
        """
        return self.execute(sql.CACHE_HIT)

    def index_usage(self: Self) -> list[tuple[Any, ...]]:
        """Calculates your index hit rate (effective databases are at 99% and up).

        Record(     relname='pgbench_history', percent_of_times_index_used=None,     rows_in_table=249976 )

        :returns: list of Records
        """
        return self.execute(sql.INDEX_USAGE)

    def calls(self: Self) -> list[tuple[Any, ...]]:
        """Show 10 most frequently called queries. Requires the pg_stat_statements Postgres module to be installed.

        Record(     query='BEGIN;',     exec_time=datetime.timedelta(0, 0, 288174),     prop_exec_time='0.0%',
        ncalls='845590', sync_io_time=datetime.timedelta(0) )

        :param truncate: trim the Record.query output if greater than 40 chars
        :returns: list of Records
        """
        if self.pg_stat_statement():
            query = self.truncate_query(column_name=self.query_column)
            return self.execute(sql.CALLS.format(query=query, total_time=self.time_column))
        return [self.get_missing_pg_stat_statement_error()]

    def blocking(self: Self) -> list[tuple[Any, ...]]:
        """Display queries holding locks other queries are waiting to be released.

        Record(     pid=40821,     source='', running_for=datetime.timedelta(0, 0, 2857),     waiting=False,
        query='SELECT pg_sleep(10);' )

        :returns: list of Records
        """
        return self.execute(sql.BLOCKING.format(query_column=self.query_column, pid_column=self.pid_column))

    def outliers(self: Self) -> list[tuple[Any, ...]]:
        """Show 10 queries that have longest execution time in aggregate. Requires the pg_stat_statments.

        Record(     qry='UPDATE pgbench_tellers SET tbalance = tbalance
        + ?;',     exec_time=datetime.timedelta(0, 19944, 993099),
        prop_exec_time='67.1%',     ncalls='845589',
        sync_io_time=datetime.timedelta(0) )

        :param truncate: trim the Record.qry output if greater than 40
            chars
        :returns: list of Records
        """
        if self.pg_stat_statement():
            query = self.truncate_query(column_name=self.query_column)
            return self.execute(sql.OUTLIERS.format(query=query, total_time=self.time_column))
        return [self.get_missing_pg_stat_statement_error()]

    def vacuum_stats(self: Self) -> list[tuple[Any, ...]]:
        """Show dead rows and whether an automatic vacuum is expected to be triggered.

        Record(     schema='public',     table='pgbench_tellers', last_vacuum='2014-04-29 14:45',
        last_autovacuum='2014-04-29 14:45',     rowcount='10',     dead_rowcount='0', autovacuum_threshold='52',
        expect_autovacuum=None )

        :returns: list of Records
        """
        return self.execute(sql.VACUUM_STATS)

    def bloat(self: Self) -> list[tuple[Any, ...]]:
        """Table and index bloat in your database ordered by most wasteful.

        Record(
            type='index',
            schemaname='public',
            object_name='pgbench_accounts::pgbench_accounts_pkey',
            bloat=Decimal('0.2'),
            waste='0 bytes'
        )

        :returns: list of Records
        """
        return self.execute(sql.BLOAT)

    def long_running_queries(self: Self) -> list[tuple[Any, ...]]:
        """Show all queries longer than five minutes by descending duration.

        Record(     pid=19578,     duration=datetime.timedelta(0, 19944, 993099),     query='SELECT * FROM students' )

        :returns: list of Records
        """
        idle = "AND state <> 'idle'" if self.is_pg_at_least_nine_two() else "AND current_query <> '<IDLE>'"

        return self.execute(
            sql.LONG_RUNNING_QUERIES.format(pid_column=self.pid_column, query_column=self.query_column, idle=idle),
        )

    def seq_scans(self: Self) -> list[tuple[Any, ...]]:
        """Show the count of sequential scans by table descending by order.

        Record(     name='pgbench_branches',     count=237 )

        :returns: list of Records
        """
        return self.execute(sql.SEQ_SCANS)

    def unused_indexes(self: Self) -> list[tuple[Any, ...]]:
        """Show unused and almost unused indexes, ordered by their size.

        relative to the number of index scans. Exclude
        indexes of very small tables (less than 5 pages), where the planner will almost invariably select a sequential
        scan, but may not in the future as the table grows.

        Record(
            table='public.grade_levels',
            index='index_placement_attempts_on_grade_level_id',
            index_size='97 MB',
            index_scans=0
        )

        :returns: list of Records
        """
        return self.execute(sql.UNUSED_INDEXES)

    def total_table_size(self: Self) -> list[tuple[Any, ...]]:
        """Show the size of the tables (including indexes), descending by size.

        Record(     name='pgbench_accounts',     size='15 MB' )

        :returns: list of Records
        """
        return self.execute(sql.TOTAL_TABLE_SIZE)

    def total_indexes_size(self: Self) -> list[tuple[Any, ...]]:
        """Show the total size of all the indexes on each table, descending by size.

        Record(     table='pgbench_accounts',     index_size='2208 kB' )

        :returns: list of Records
        """
        return self.execute(sql.TOTAL_INDEXES_SIZE)

    def table_size(self: Self) -> list[tuple[Any, ...]]:
        """Show the size of the tables (excluding indexes), descending by size.

        :returns: list
        """
        return self.execute(sql.TABLE_SIZE)

    def index_size(self: Self) -> list[tuple[Any, ...]]:
        """Show the size of indexes, descending by size.

        :returns: list
        """
        return self.execute(sql.INDEX_SIZE)

    def total_index_size(self: Self) -> list[tuple[Any, ...]]:
        """Show the total size of all indexes.

        Record(     size='2240 kB' )

        :returns: list of Records
        """
        return self.execute(sql.TOTAL_INDEX_SIZE)

    def locks(self: Self) -> list[tuple[Any, ...]]:
        """Display queries with active locks.

        Record(     procpid=31776,     relname=None,
        transactionid=None,     granted=True,     query_snippet='select
        * from hello;',     age=datetime.timedelta(0, 0, 288174), )

        :returns: list of Records
        """
        return self.execute(sql.LOCKS.format(pid_column=self.pid_column, query_column=self.query_column))

    def table_indexes_size(self: Self) -> list[tuple[Any, ...]]:
        """Show the total size of all the indexes on each table, descending by size.

        Record(     table='pgbench_accounts',     index_size='2208 kB' )

        :returns: list of Records
        """
        return self.execute(sql.TABLE_INDEXES_SIZE)

    def ps(self: Self) -> list[tuple[Any, ...]]:
        """View active queries with execution time.

        Record(     pid=28023,     source='pgbench', running_for=datetime.timedelta(0, 0, 288174),     waiting=0,
        query='UPDATE pgbench_accounts SET abalance = abalance + 423;' )

        :returns: list of Records
        """
        idle = "AND state <> 'idle'" if self.is_pg_at_least_nine_two() else "AND current_query <> '<IDLE>'"

        query_column = self.truncate_query(column_name=self.query_column)
        return self.execute(sql.PS.format(pid_column=self.pid_column, query_column=query_column, idle=idle))

    def version(self: Self) -> list[tuple[Any, ...]]:
        """Get the Postgres server version.

        Record(     version='PostgreSQL 9.3.3 on x86_64-apple- darwin13.0.0' )

        :returns: list of Records
        """
        return self.execute(sql.VERSION)
