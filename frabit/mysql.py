# (c) 2020 Frabit Project maintained and limited by Blylei < blylei918@gmail.com >
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
#
# This file is part of Frabit
#
"""
基于mysql_connector封装对MySQL的操作接口,Frabit对MySQL的操作均通过此模块实现
"""
import os
import sys
import re
import datetime
import time
import logging

import mysql
from mysql import connector
from mysql.connector import errorcode

import frabit
from frabit import utils
from frabit import exceptions
from frabit import output

from frabit.exceptions import (ConninfoException,
                               MysqlException,
                               MysqlDataError,
                               MysqlIntegrityError,
                               MysqlInternalError,
                               MysqlOperationalError,
                               MysqlNotSupportedError,
                               MysqlProgrammingError,
                               MysqlConnectError,
                               MysqlInterfaceError
                               )
from frabit.infofile import Tablespace
from frabit.remote_status import RemoteStatusMixin
from frabit.utils import force_str, simplify_version, with_metaclass
from frabit.binlog import DEFAULT_XLOG_SEG_SIZE

_logger = logging.getLogger(__name__)

_live_connections = []
"""
List of connections to be closed at the interpreter shutdown
"""


@atexit.register
def _atexit():
    """
    Ensure that all the connections are correctly closed
    at interpreter shutdown
    """
    # Take a copy of the list because the conn.close() method modify it
    for conn in list(_live_connections):
        _logger.warning(
            "Forcing {} cleanup during process shut down.".format(conn.__class__.__name__))
        conn.close()


class MySQL:
    """
    This  class represents a generic interface to a MySQL server.
    """

    CHECK_QUERY = 'SELECT 1;'

    def __init__(self, conninfo):
        """
        Abstract base class constructor for MySQL interface.

        :param str conninfo: Connection information (aka DSN)
        """
        super(MySQL, self).__init__()
        self.conninfo = conninfo
        self._conn = None
        self.allow_reconnect = True
        # Build a dictionary with connection info parameters,This is mainly used to speed up search in conninfo
        try:
            self.conn_parameters = self.parse_dsn(conninfo)
        except (ValueError, TypeError) as e:
            _logger.debug(e)
            raise ConninfoException('Cannot connect to mysql: "{}" is not a valid connection string'.format(conninfo))

    @staticmethod
    def parse_dsn(dsn):
        """
        Parse connection parameters from 'conninfo'

        :param str dsn: Connection information (aka DSN)
        :rtype: dict[str,str]
        """
        return dict(x.split('=', 1) for x in dsn.split())

    @staticmethod
    def encode_dsn(parameters):
        """
        Build a connection string from a dictionary of connection
        parameters

        :param dict[str,str] parameters: Connection parameters
        :rtype: str
        """
        return ' '.join(["%s=%s" % (k, v) for k, v in sorted(parameters.items())])

    def get_connection_string(self):
        """
        Return the connection string, adding the application_name parameter
        if requested, unless already defined by user in the connection string

        :return str: the connection string
        """
        conn_string = self.conninfo
        return conn_string

    def connect(self):
        """
        Generic function for MySQL connection (using mysql-connector-python)
        """

        if not self._check_connection():
            try:
                self._conn = mysql.connector.connect(self.conninfo)
            # If mysql-connector-python fails to connect to the host, raise the appropriate exception
            except connector.PoolError as e:
                raise MysqlConnectError(force_str(e).strip())
            # Register the connection to the list of live connections
            _live_connections.append(self)
        return self._conn

    def _check_connection(self):
        """
        Return false if the connection is broken

        :rtype: bool
        """
        # If the connection is not present return False
        if not self._conn:
            return False

        # Check if the connection works by running 'SELECT 1'
        cursor = None
        initial_status = None
        try:
            initial_status = self._conn.status
            cursor = self._conn.cursor()
            cursor.execute(self.CHECK_QUERY)
            # Rollback if initial status was IDLE because the CHECK QUERY
            # has started a new transaction.
            if initial_status == STATUS_READY:
                self._conn.rollback()
        except connector.DatabaseError:
            # Connection is broken, so we need to reconnect
            self.close()
            # Raise an error if reconnect is not allowed
            if not self.allow_reconnect:
                raise MysqlConnectError("Connection lost, reconnection not allowed")
            return False
        finally:
            if cursor:
                cursor.close()
        return True

    def close(self):
        """
        Close the connection to MySQL
        """
        if self._conn:
            # If the connection is still alive, rollback and close it
            if not self._conn.closed:
                if self._conn.status == STATUS_IN_TRANSACTION:
                    self._conn.rollback()
                self._conn.close()
            # Remove the connection from the live connections list
            self._conn = None
            _live_connections.remove(self)

    def _cursor(self, *args, **kwargs):
        """
        Return a cursor
        """
        conn = self.connect()
        return conn.cursor(*args, **kwargs)

    @property
    def server_version(self):
        """
        Version of MySQL (returned by mysql-connector-python)
        """
        conn = self.connect()
        return conn.server_version

    @property
    def server_txt_version(self):
        """
        Human readable version of MySQL (calculated from server_version)

        :rtype: str|None
        """
        try:
            conn = self.connect()
            major = int(conn.server_version / 10000)
            minor = int(conn.server_version / 100 % 100)
            patch = int(conn.server_version % 100)
            return "{major}.{minor}.{patch}".format(major=major, minor=minor, patch=patch)
        except MysqlConnectError as e:
            _logger.debug("Error retrieving MySQL version: {}".format(force_str(e).strip()))
            return None

    @property
    def server_major_version(self):
        """
        MySQL major version (calculated from server_txt_version)

        :rtype: str|None
        """
        result = self.server_txt_version
        if result is not None:
            return simplify_version(result)
        return None


class MySQLConnection(MySQL):
    """
    This class represents a standard client connection to a MySQL server.
    """

    def __init__(self, conninfo):
        """
        MySQl connection constructor.

        :param str conninfo: Connection information (aka DSN)
        """
        super(MySQLConnection, self).__init__(conninfo)
        self.configuration_files = None

    def connect(self):
        """
        Connect to the MySQL server. It reuses an existing connection.
        """
        if self._check_connection():
            return self._conn
        # create a new connection if not an existing connection
        self._conn = super(MySQLConnection, self).connect()
        return self._conn

    @property
    def server_txt_version(self):
        """
        Human readable version of MySQL (returned by the server)
        """
        try:
            cur = self._cursor()
            cur.execute("SELECT version()")
            return cur.fetchone()[0].split()[1]
        except (MysqlInterfaceError, MysqlException) as e:
            _logger.debug("Error retrieving MySQL version: {}".format(force_str(e).strip()))
            return None

    @property
    def is_superuser(self):
        """
        Returns true if current user has superuser privileges
        """
        try:
            cur = self._cursor()
            cur.execute('SELECT usesuper FROM pg_user '
                        'WHERE usename = CURRENT_USER')
            return cur.fetchone()[0]
        except (MysqlInterfaceError, MysqlException) as e:
            _logger.debug("Error calling is_superuser() function: %s".format(force_str(e).strip()))
            return None

    @property
    def has_backup_privileges(self):
        """
        Returns true if current user has efficient privileges,include below:
        super :
        processlist:
        replicate client:
        replicate slave :
        """
        privileges_info = """
        SELECT
        concat(Select_priv
              ,Insert_priv
              ,Update_priv
              ,Delete_priv
              ,Create_priv
              ,Drop_priv
              ,Reload_priv
              ,Shutdown_priv
              ,Process_priv
              ,File_priv
              ,Grant_priv
              ,References_priv
              ,Index_priv
              ,Alter_priv
              ,Show_db_priv
              ,Super_priv
              ,Lock_tables_priv
              ,Execute_priv
              ,Repl_slave_priv
              ,Repl_client_priv
              ,Create_view_priv
              ,Show_view_priv
              ,Create_routine_priv
              ,Alter_routine_priv
              ,Create_user_priv
              ,Event_priv
              ,Trigger_priv
              )
        FROM mysql.user
        WHERE user = CURRENT_USER
          AND host = CURRENT_HOST 
        """
        try:
            cur = self._cursor()
            cur.execute(privileges_info)
            return cur.fetchone()[0]
        except (MysqlInterfaceError, MysqlException) as e:
            _logger.debug("Error checking privileges for functions needed for backups: {}".format(force_str(e).strip()))
            return None

    @property
    def current_binlog_info(self):
        """
        Get detailed information about the current Binlog position in MySQL.

        This method returns a dictionary containing the following data:

         * file_name
         * position
         * gtid

        :rtype: dict
        """
        try:
            cur = self._cursor()
            cur.execute("SHOW MASTER STATUS;")
            return cur.fetchone()
        except (MysqlInterfaceError, MysqlException) as e:
            _logger.debug("Error retrieving current binlog detailed information: {}".format(force_str(e).strip()))
        return None

    @property
    def current_binlog_file_name(self):
        """
        Get current WAL file from MySQL

        :return str: current WAL file in MySQL
        """
        current_binlog_info = self.current_binlog_info
        if current_binlog_info is not None:
            return current_binlog_info['file']
        return None

    @property
    def current_binlog_position(self):
        """
        Get current WAL location from MySQL

        :return str: current WAL location in MySQL
        """
        current_binlog_info = self.current_binlog_info
        if current_binlog_info is not None:
            return current_binlog_info['position']
        return None

    @property
    def current_gtid(self):
        """
        Get current WAL location from MySQL

        :return str: current WAL location in MySQL
        """
        current_binlog_info = self.current_binlog_info
        if current_binlog_info is not None:
            return current_binlog_info['gtid']
        return None

    @property
    def binlog_file_size(self):
        """
        Retrieve the size of one Binlog file.

        :return: The wal size (In bytes)
        """
        try:
            cur = self._cursor()
            # We can't use the `get_setting` method here, because it
            # use `SHOW`, returning an human readable value such as "16MB",
            # while we prefer a raw value such as 16777216.
            cur.execute("SELECT setting "
                        "FROM pg_settings "
                        "WHERE name='wal_segment_size'")
            result = cur.fetchone()
            wal_segment_size = int(result[0])

            # Prior to MySQL 11, the wal segment size is returned in
            # blocks
            if self.server_version < 110000:
                cur.execute("SELECT setting "
                            "FROM pg_settings "
                            "WHERE name='wal_block_size'")
                result = cur.fetchone()
                wal_block_size = int(result[0])

                wal_segment_size *= wal_block_size

            return wal_segment_size
        except ValueError as e:
            _logger.error("Error retrieving current xlog "
                          "segment size: %s",
                          force_str(e).strip())
            return None

    @property
    def current_size(self):
        """
        Returns the total size of the MySQL server
        (requires superuser or pg_read_all_stats)
        """
        if not self.has_backup_privileges:
            return None

        try:
            cur = self._cursor()
            cur.execute(
                "SELECT sum(pg_tablespace_size(oid)) "
                "FROM pg_tablespace")
            return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving MySQL total size: %s",
                          force_str(e).strip())
            return None

    @property
    def archive_timeout(self):
        """
        Retrieve the archive_timeout setting in MySQL

        :return: The archive timeout (in seconds)
        """
        try:
            cur = self._cursor(cursor_factory=DictCursor)
            # We can't use the `get_setting` method here, because it
            # uses `SHOW`, returning an human readable value such as "5min",
            # while we prefer a raw value such as 300.
            cur.execute("SELECT setting "
                        "FROM pg_settings "
                        "WHERE name='archive_timeout'")
            result = cur.fetchone()
            archive_timeout = int(result[0])

            return archive_timeout
        except ValueError as e:
            _logger.error("Error retrieving archive_timeout: %s",
                          force_str(e).strip())
            return None

    @property
    def checkpoint_timeout(self):
        """
        Retrieve the checkpoint_timeout setting in MySQL

        :return: The checkpoint timeout (in seconds)
        """
        try:
            cur = self._cursor(cursor_factory=DictCursor)
            # We can't use the `get_setting` method here, because it
            # uses `SHOW`, returning an human readable value such as "5min",
            # while we prefer a raw value such as 300.
            cur.execute("SELECT setting "
                        "FROM pg_settings "
                        "WHERE name='checkpoint_timeout'")
            result = cur.fetchone()
            checkpoint_timeout = int(result[0])

            return checkpoint_timeout
        except ValueError as e:
            _logger.error("Error retrieving checkpoint_timeout: %s",
                          force_str(e).strip())
            return None

    def get_archiver_stats(self):
        """
        This method gathers statistics from pg_stat_archiver.
        Only for Postgres 9.4+ or greater. If not available, returns None.

        :return dict|None: a dictionary containing Postgres statistics from
            pg_stat_archiver or None
        """
        try:
            # pg_stat_archiver is only available from Postgres 9.4+
            if self.server_version < 90400:
                return None
            cur = self._cursor(cursor_factory=DictCursor)
            # Select from pg_stat_archiver statistics view,
            # retrieving statistics about WAL archiver process activity,
            # also evaluating if the server is archiving without issues
            # and the archived WALs per second rate.
            #
            # We are using current_settings to check for archive_mode=always.
            # current_setting does normalise its output so we can just
            # check for 'always' settings using a direct string
            # comparison
            cur.execute(
                "SELECT *, "
                "current_setting('archive_mode') IN ('on', 'always') "
                "AND (last_failed_wal IS NULL "
                "OR last_failed_wal LIKE '%.history' "
                "AND substring(last_failed_wal from 1 for 8) "
                "<= substring(last_archived_wal from 1 for 8) "
                "OR last_failed_time <= last_archived_time) "
                "AS is_archiving, "
                "CAST (archived_count AS NUMERIC) "
                "/ EXTRACT (EPOCH FROM age(now(), stats_reset)) "
                "AS current_archived_wals_per_second "
                "FROM pg_stat_archiver")
            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving pg_stat_archive data: %s",
                          force_str(e).strip())
            return None

    def fetch_remote_status(self):
        """
        Get the status of the MySQL server

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        # MySQL settings to get from the server (requiring superuser)
        pg_superuser_settings = [
            'data_directory']
        # MySQL settings to get from the server
        pg_settings = []
        pg_query_keys = [
            'server_txt_version',
            'is_superuser',
            'is_in_recovery',
            'current_xlog',
            'pgespresso_installed',
            'replication_slot_support',
            'replication_slot',
            'synchronous_standby_names',
            'postgres_systemid'
        ]
        # Initialise the result dictionary setting all the values to None
        result = dict.fromkeys(
            pg_superuser_settings + pg_settings + pg_query_keys,
            None)
        try:
            # Retrieve wal_level, hot_standby and max_wal_senders
            # only if version is >= 9.0
            if self.server_version >= 90000:
                pg_settings.append('wal_level')
                pg_settings.append('hot_standby')
                pg_settings.append('max_wal_senders')
                pg_settings.append('wal_keep_segments')

            if self.server_version >= 90300:
                pg_settings.append('data_checksums')

            if self.server_version >= 90400:
                pg_settings.append('max_replication_slots')

            if self.server_version >= 90500:
                pg_settings.append('wal_compression')

            # retrieves superuser settings
            if self.has_backup_privileges:
                for name in pg_superuser_settings:
                    result[name] = self.get_setting(name)

            # retrieves standard settings
            for name in pg_settings:
                result[name] = self.get_setting(name)

            result['is_superuser'] = self.is_superuser
            result['has_backup_privileges'] = self.has_backup_privileges
            result['is_in_recovery'] = self.is_in_recovery
            result['server_txt_version'] = self.server_txt_version
            result['pgespresso_installed'] = self.has_pgespresso
            current_xlog_info = self.current_xlog_info
            if current_xlog_info:
                result['current_lsn'] = current_xlog_info['location']
                result['current_xlog'] = current_xlog_info['file_name']
            else:
                result['current_lsn'] = None
                result['current_xlog'] = None
            result['current_size'] = self.current_size
            result['archive_timeout'] = self.archive_timeout
            result['checkpoint_timeout'] = self.checkpoint_timeout
            result['xlog_segment_size'] = self.xlog_segment_size

            result.update(self.get_configuration_files())

            # Retrieve the replication_slot status
            result["replication_slot_support"] = False
            if self.server_version >= 90400:
                result["replication_slot_support"] = True
                if self.slot_name is not None:
                    result["replication_slot"] = (
                        self.get_replication_slot(self.slot_name))

            # Retrieve the list of synchronous standby names
            result["synchronous_standby_names"] = []
            if self.server_version >= 90100:
                result["synchronous_standby_names"] = (
                    self.get_synchronous_standby_names())

            if self.server_version >= 90600:
                result["postgres_systemid"] = self.get_systemid()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.warning("Error retrieving MySQL status: %s",
                            force_str(e).strip())
        return result

    def get_setting(self, name):
        """
        Get a MySQL setting with a given name

        :param name: a parameter name
        """
        try:
            cur = self._cursor()
            cur.execute('SHOW "%s"' % name.replace('"', '""'))
            return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving MySQL setting '%s': %s",
                          name.replace('"', '""'), force_str(e).strip())
            return None

    def get_configuration_files(self):
        """
        Get postgres configuration files or an empty dictionary
        in case of error

        :rtype: dict
        """
        if self.configuration_files:
            return self.configuration_files
        try:
            self.configuration_files = {}
            cur = self._cursor()
            cur.execute(
                "SELECT name, setting FROM pg_settings "
                "WHERE name IN ('config_file', 'hba_file', 'ident_file')")
            for cname, cpath in cur.fetchall():
                self.configuration_files[cname] = cpath

            # Retrieve additional configuration files
            # If MySQL is older than 8.4 disable this check
            if self.server_version >= 80400:
                cur.execute(
                    "SELECT DISTINCT sourcefile AS included_file "
                    "FROM pg_settings "
                    "WHERE sourcefile IS NOT NULL "
                    "AND sourcefile NOT IN "
                    "(SELECT setting FROM pg_settings "
                    "WHERE name = 'config_file') "
                    "ORDER BY 1")
                # Extract the values from the containing single element tuples
                included_files = [included_file
                                  for included_file, in cur.fetchall()]
                if len(included_files) > 0:
                    self.configuration_files['included_files'] = included_files

        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving MySQL configuration files "
                          "location: %s", force_str(e).strip())
            self.configuration_files = {}

        return self.configuration_files

    def start_exclusive_backup(self, label):
        """
        Calls pg_start_backup() on the MySQL server

        This method returns a dictionary containing the following data:

         * location
         * file_name
         * file_offset
         * timestamp

        :param str label: descriptive string to identify the backup
        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction, as the pg_start_backup
            # invocation can last up to MySQL's checkpoint_timeout
            conn.rollback()

            # Start an exclusive backup
            cur = conn.cursor(cursor_factory=DictCursor)
            if self.server_version < 80400:
                cur.execute(
                    "SELECT location, "
                    "({pg_walfile_name_offset}(location)).*, "
                    "now() AS timestamp "
                    "FROM pg_start_backup(%s) AS location"
                        .format(**self.name_map),
                    (label,))
            else:
                cur.execute(
                    "SELECT location, "
                    "({pg_walfile_name_offset}(location)).*, "
                    "now() AS timestamp "
                    "FROM pg_start_backup(%s,%s) AS location"
                        .format(**self.name_map),
                    (label, self.immediate_checkpoint))

            start_row = cur.fetchone()

            # Rollback to release the transaction, as the connection
            # is to be retained until the end of backup
            conn.rollback()

            return start_row
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "pg_start_backup(): %s" % force_str(e).strip()
            _logger.debug(msg)
            raise PostgresException(msg)

    def start_concurrent_backup(self, label):
        """
        Calls pg_start_backup on the MySQL server using the
        API introduced with version 9.6

        This method returns a dictionary containing the following data:

         * location
         * timeline
         * timestamp

        :param str label: descriptive string to identify the backup
        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction, as the pg_start_backup
            # invocation can last up to MySQL's checkpoint_timeout
            conn.rollback()

            # Start the backup using the api introduced in postgres 9.6
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(
                "SELECT location, "
                "(SELECT timeline_id "
                "FROM pg_control_checkpoint()) AS timeline, "
                "now() AS timestamp "
                "FROM pg_start_backup(%s, %s, FALSE) AS location",
                (label, self.immediate_checkpoint))
            start_row = cur.fetchone()

            # Rollback to release the transaction, as the connection
            # is to be retained until the end of backup
            conn.rollback()

            return start_row
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "pg_start_backup command: %s" % (force_str(e).strip(),)
            _logger.debug(msg)
            raise PostgresException(msg)

    def stop_exclusive_backup(self):
        """
        Calls pg_stop_backup() on the MySQL server

        This method returns a dictionary containing the following data:

         * location
         * file_name
         * file_offset
         * timestamp

        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction, as the pg_stop_backup
            # invocation could will wait until the current WAL file is shipped
            conn.rollback()

            # Stop the backup
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(
                "SELECT location, "
                "({pg_walfile_name_offset}(location)).*, "
                "now() AS timestamp "
                "FROM pg_stop_backup() AS location"
                    .format(**self.name_map)
            )

            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = ("Error issuing pg_stop_backup command: %s" %
                   force_str(e).strip())
            _logger.debug(msg)
            raise PostgresException(
                'Cannot terminate exclusive backup. '
                'You might have to manually execute pg_stop_backup '
                'on your MySQL server')

    def stop_concurrent_backup(self):
        """
        Calls pg_stop_backup on the MySQL server using the
        API introduced with version 9.6

        This method returns a dictionary containing the following data:

         * location
         * timeline
         * backup_label
         * timestamp

        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction, as the pg_stop_backup
            # invocation could will wait until the current WAL file is shipped
            conn.rollback()

            # Stop the backup  using the api introduced with version 9.6
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(
                'SELECT end_row.lsn AS location, '
                '(SELECT CASE WHEN pg_is_in_recovery() '
                'THEN min_recovery_end_timeline ELSE timeline_id END '
                'FROM pg_control_checkpoint(), pg_control_recovery()'
                ') AS timeline, '
                'end_row.labelfile AS backup_label, '
                'now() AS timestamp FROM pg_stop_backup(FALSE) AS end_row')

            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = ("Error issuing pg_stop_backup command: %s" %
                   force_str(e).strip())
            _logger.debug(msg)
            raise PostgresException(msg)

    def pgespresso_start_backup(self, label):
        """
        Execute a pgespresso_start_backup

        This method returns a dictionary containing the following data:

         * backup_label
         * timestamp

        :param str label: descriptive string to identify the backup
        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction,
            # as the pgespresso_start_backup invocation can last
            # up to MySQL's checkpoint_timeout
            conn.rollback()

            # Start the concurrent backup using pgespresso
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(
                'SELECT pgespresso_start_backup(%s,%s) AS backup_label, '
                'now() AS timestamp',
                (label, self.immediate_checkpoint))

            start_row = cur.fetchone()

            # Rollback to release the transaction, as the connection
            # is to be retained until the end of backup
            conn.rollback()

            return start_row
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "pgespresso_start_backup(): %s" % force_str(e).strip()
            _logger.debug(msg)
            raise PostgresException(msg)

    def pgespresso_stop_backup(self, backup_label):
        """
        Execute a pgespresso_stop_backup

        This method returns a dictionary containing the following data:

         * end_wal
         * timestamp

        :param str backup_label: backup label as returned
            by pgespress_start_backup
        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()
            # Issue a rollback to release any unneeded lock
            conn.rollback()
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute("SELECT pgespresso_stop_backup(%s) AS end_wal, "
                        "now() AS timestamp",
                        (backup_label,))
            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "Error issuing pgespresso_stop_backup() command: %s" % (
                force_str(e).strip())
            _logger.debug(msg)
            raise PostgresException(
                '%s\n'
                'HINT: You might have to manually execute '
                'pgespresso_abort_backup() on your MySQL '
                'server' % msg)

    def flush_binlog(self):
        """
        Execute a FLUSH BINARY LOGS ;

        To be SURE of the switch of a xlog, we collect the xlogfile name
        before and after the switch.
        The method returns the just closed xlog file name if the current xlog
        file has changed, it returns an empty string otherwise.

        The method returns None if something went wrong during the execution
        of the pg_switch_wal command.

        :rtype: str|None
        """
        try:
            conn = self.connect()
            # Requires superuser privilege
            if not self.has_backup_privileges:
                raise BackupFunctionsAccessRequired()
            cur = conn.cursor()
            cur.execute('FLUSH BINARY LOGS')
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error issuing {pg_switch_wal}() command: %s"
                    .format(**self.name_map),
                force_str(e).strip())
            return None

    def get_replication_stats(self, client_type=STANDBY):
        """
        Returns streaming replication information
        """
        try:
            cur = self._cursor(cursor_factory=NamedTupleCursor)

            if not self.has_backup_privileges:
                raise BackupFunctionsAccessRequired()

            # pg_stat_replication is a system view that contains one
            # row per WAL sender process with information about the
            # replication status of a standby server. It has been
            # introduced in MySQL 9.1. Current fields are:
            #
            # - pid (procpid in 9.1)
            # - usesysid
            # - usename
            # - application_name
            # - client_addr
            # - client_hostname
            # - client_port
            # - backend_start
            # - backend_xmin (9.4+)
            # - state
            # - sent_lsn (sent_location before 10)
            # - write_lsn (write_location before 10)
            # - flush_lsn (flush_location before 10)
            # - replay_lsn (replay_location before 10)
            # - sync_priority
            # - sync_state
            #

            if self.server_version < 90100:
                raise PostgresUnsupportedFeature('9.1')

            from_repslot = ""
            where_clauses = []
            if self.server_version >= 100000:
                # Current implementation (10+)
                what = "r.*, rs.slot_name"
                # Look for replication slot name
                from_repslot = "LEFT JOIN pg_replication_slots rs " \
                               "ON (r.pid = rs.active_pid) "
                where_clauses += ["(rs.slot_type IS NULL OR "
                                  "rs.slot_type = 'physical')"]
            elif self.server_version >= 90500:
                # MySQL 9.5/9.6
                what = "pid, " \
                       "usesysid, " \
                       "usename, " \
                       "application_name, " \
                       "client_addr, " \
                       "client_hostname, " \
                       "client_port, " \
                       "backend_start, " \
                       "backend_xmin, " \
                       "state, " \
                       "sent_location AS sent_lsn, " \
                       "write_location AS write_lsn, " \
                       "flush_location AS flush_lsn, " \
                       "replay_location AS replay_lsn, " \
                       "sync_priority, " \
                       "sync_state, " \
                       "rs.slot_name"
                # Look for replication slot name
                from_repslot = "LEFT JOIN pg_replication_slots rs " \
                               "ON (r.pid = rs.active_pid) "
                where_clauses += ["(rs.slot_type IS NULL OR "
                                  "rs.slot_type = 'physical')"]
            elif self.server_version >= 90400:
                # MySQL 9.4
                what = "pid, " \
                       "usesysid, " \
                       "usename, " \
                       "application_name, " \
                       "client_addr, " \
                       "client_hostname, " \
                       "client_port, " \
                       "backend_start, " \
                       "backend_xmin, " \
                       "state, " \
                       "sent_location AS sent_lsn, " \
                       "write_location AS write_lsn, " \
                       "flush_location AS flush_lsn, " \
                       "replay_location AS replay_lsn, " \
                       "sync_priority, " \
                       "sync_state"
            elif self.server_version >= 90200:
                # MySQL 9.2/9.3
                what = "pid, " \
                       "usesysid, " \
                       "usename, " \
                       "application_name, " \
                       "client_addr, " \
                       "client_hostname, " \
                       "client_port, " \
                       "backend_start, " \
                       "CAST (NULL AS xid) AS backend_xmin, " \
                       "state, " \
                       "sent_location AS sent_lsn, " \
                       "write_location AS write_lsn, " \
                       "flush_location AS flush_lsn, " \
                       "replay_location AS replay_lsn, " \
                       "sync_priority, " \
                       "sync_state"
            else:
                # MySQL 9.1
                what = "procpid AS pid, " \
                       "usesysid, " \
                       "usename, " \
                       "application_name, " \
                       "client_addr, " \
                       "client_hostname, " \
                       "client_port, " \
                       "backend_start, " \
                       "CAST (NULL AS xid) AS backend_xmin, " \
                       "state, " \
                       "sent_location AS sent_lsn, " \
                       "write_location AS write_lsn, " \
                       "flush_location AS flush_lsn, " \
                       "replay_location AS replay_lsn, " \
                       "sync_priority, " \
                       "sync_state"

            # Streaming client
            if client_type == self.STANDBY:
                # Standby server
                where_clauses += ['{replay_lsn} IS NOT NULL'.format(
                    **self.name_map)]
            elif client_type == self.WALSTREAMER:
                # WAL streamer
                where_clauses += ['{replay_lsn} IS NULL'.format(
                    **self.name_map)]

            if where_clauses:
                where = 'WHERE %s ' % ' AND '.join(where_clauses)
            else:
                where = ''

            # Execute the query
            cur.execute(
                "SELECT %s, "
                "pg_is_in_recovery() AS is_in_recovery, "
                "CASE WHEN pg_is_in_recovery() "
                "  THEN {pg_last_wal_receive_lsn}() "
                "  ELSE {pg_current_wal_lsn}() "
                "END AS current_lsn "
                "FROM pg_stat_replication r "
                "%s"
                "%s"
                "ORDER BY sync_state DESC, sync_priority"
                .format(**self.name_map)
                % (what, from_repslot, where))

            # Generate a list of standby objects
            return cur.fetchall()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving status of standby servers: %s",
                          force_str(e).strip())
            return None

    def get_replication_slot(self, slot_name):
        """
        Retrieve from the MySQL server a physical replication slot
        with a specific slot_name.

        This method returns a dictionary containing the following data:

         * slot_name
         * active
         * restart_lsn

        :param str slot_name: the replication slot name
        :rtype: psycopg2.extras.DictRow
        """
        if self.server_version < 90400:
            # Raise exception if replication slot are not supported
            # by MySQL version
            raise PostgresUnsupportedFeature('9.4')
        else:
            cur = self._cursor(cursor_factory=NamedTupleCursor)
            try:
                cur.execute("SELECT slot_name, "
                            "active, "
                            "restart_lsn "
                            "FROM pg_replication_slots "
                            "WHERE slot_type = 'physical' "
                            "AND slot_name = '%s'" % slot_name)
                # Retrieve the replication slot information
                return cur.fetchone()
            except (PostgresConnectionError, psycopg2.Error) as e:
                _logger.debug("Error retrieving replication_slots: %s",
                              force_str(e).strip())
                raise
