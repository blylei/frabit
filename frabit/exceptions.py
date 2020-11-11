# Copyright (C) 2020-2020 blylei Limited
#
# This file is part of Flyrabbit.
#
# Flyrabbit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Flyrabbit.  If not, see <http://www.gnu.org/licenses/>.


class FlyrabbitException(Exception):
    """
    The base class of all other Flyrabbit exceptions
    """


class ConfigurationException(FlyrabbitException):
    """
    Base exception for all the Configuration errors
    """


class CommandException(FlyrabbitException):
    """
    Base exception for all the errors related to
    the execution of a Command.
    """


class CompressionException(FlyrabbitException):
    """
    Base exception for all the errors related to
    the execution of a compression action.
    """


class MysqlException(FlyrabbitException):
    """
    Base exception for all the errors related to PostgreSQL.
    """


class BackupException(FlyrabbitException):
    """
    Base exception for all the errors related to the execution of a backup.
    """


class HookScriptException(FlyrabbitException):
    """
    Base exception for all the errors related to Hook Script execution.
    """


class LockFileException(FlyrabbitException):
    """
    Base exception for lock related errors
    """


class SyncException(FlyrabbitException):
    """
    Base Exception for synchronisation functions
    """


class SshCommandException(CommandException):
    """
    Error parsing ssh_command parameter
    """


class TimeoutError(CommandException):
    """
    Error parsing command execute timeout parameter
    """


class UnknownBackupIdException(BackupException):
    """
    The searched backup_id doesn't exists
    """


class BackupInfoBadInitialisation(BackupException):
    """
    Exception for a bad initialization error
    """


class SyncError(SyncException):
    """
    Synchronisation error
    """


class SyncNothingToDo(SyncException):
    """
    Nothing to do during sync operations
    """


class SyncToBeDeleted(SyncException):
    """
    An incomplete backup is to be deleted
    """


class CommandFailedException(CommandException):
    """
    Exception representing a failed command
    """


class CommandMaxRetryExceeded(CommandFailedException):
    """
    A command with retry_times > 0 has exceeded the number of available retry
    """


class RsyncListFilesFailure(CommandException):
    """
    Failure parsing the output of a "rsync --list-only" command
    """


class DataTransferFailure(CommandException):
    """
    Used to pass failure details from a data transfer Command
    """

    @classmethod
    def from_command_error(cls, cmd, e, msg):
        """
        This method build a DataTransferFailure exception and report the
        provided message to the user (both console and log file) along with
        the output of the failed command.

        :param str cmd: The command that failed the transfer
        :param CommandFailedException e: The exception we are handling
        :param str msg: a descriptive message on what we are trying to do
        :return DataTransferFailure: will contain the message provided in msg
        """
        try:
            details = msg
            details += "\n%s error:\n" % cmd
            details += e.args[0]['out']
            details += e.args[0]['err']
            return cls(details)
        except (TypeError, NameError):
            # If it is not a dictionary just convert it to a string
            from frabit.common import force_str
            return cls(force_str(e.args))


class CompressionIncompatibility(CompressionException):
    """
    Exception for compression incompatibility
    """


class FsOperationFailed(CommandException):
    """
    Exception which represents a failed execution of a command on FS
    """


class LockFileBusy(LockFileException):
    """
    Raised when a lock file is not free
    """


class LockFilePermissionDenied(LockFileException):
    """
    Raised when a lock file is not accessible
    """


class LockFileParsingError(LockFileException):
    """
    Raised when the content of the lockfile is unexpected
    """


class ConninfoException(ConfigurationException):
    """
    Error for missing or failed parsing of the conninfo parameter (DSN)
    """


class BinlogHasPurged(MysqlException):
    """
    Error connecting to the MySQL server
    """
    def __str__(self):
        # Returns the first line
        if self.args and self.args[0]:
            from frabit.common import force_str
            return force_str(self.args[0]).splitlines()[0].strip()
        else:
            return ''


class BackupFunctionsAccessRequired(MysqlException):
    """
    Superuser or access to backup functions is required
    """


class AbortedRetryHookScript(HookScriptException):
    """
    Exception for handling abort of retry hook scripts
    """
    def __init__(self, hook):
        """
        Initialise the exception with hook script info
        """
        self.hook = hook

    def __str__(self):
        """
        String representation
        """
        return ("Abort '%s_%s' retry hook script (%s, exit code: %d)" % (
                self.hook.phase, self.hook.name,
                self.hook.script, self.hook.exit_status))


class RecoveryException(FlyrabbitException):
    """
    Exception for a recovery error
    """


class RecoveryTargetActionException(RecoveryException):
    """
    Exception for a wrong recovery target action
    """


class RecoveryStandbyModeException(RecoveryException):
    """
    Exception for a wrong recovery standby mode
    """


class RecoveryInvalidTargetException(RecoveryException):
    """
    Exception for a wrong recovery target
    """
