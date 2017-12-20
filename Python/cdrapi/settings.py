"""
Collect tier-specific CDR settings.

The CDR current has four tiers:
  DEV - where active development is done
  QA - where testing of new/modified software happens
  STAGE - where CBIIT practices release deployments (a.k.a. "TEST")
  PROD - the system used for actual production work
"""

import datetime
import logging
import os
import re
import subprocess

class Tier:
    """
    Collection of tier-specific CDR values

    Class values:
      Pattern strings for the paths to files where the values are stored.
      LOG_FORMAT - pattern for entries in our log files

    Attribute:
      name - string containing the name of the tier represented by the values

    Properties:
      drive - letter where the CDR is installed (D on the CDR servers)
      passwords - dictionary of passwords keyed by a tuple of a lowercase
                  database name and user name for database accounts, or
                  keyed by the lowercase user name string for all other
                  passwords
      hosts - dictionary of this tier's fully qualified DNS names, keyed
              by the uppercase role name
      ports - dictionary of TCP/IP port integers, keyed by database name
      logdir - location where we record what happens
      sql_server - FQDN for the database server
    """

    # Patterns for finding settings files once we know which drive
    # the CDR lives on.
    APPHOSTS = "{}:/etc/cdrapphosts.rc"
    TIER = "{}:/etc/cdrtier.rc"
    PASSWORDS = "{}:/etc/cdrpw"
    DBPW = "{}:/etc/cdrdbpw"
    PORTS = "{}:/etc/cdrdbports"

    # Custom logging format.
    LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

    def __init__(self, tier=None):
        """
        Save or look up the name of this tier

        Pass:
          tier - optional string naming a specific tier; if not provided
                 the value in /etc/cdrtier.rc will be used
        """

        self.name = self.__get_tier_name(tier)

    @property
    def drive(self):
        """
        Letter for the drive where the CDR lives
        """

        if not hasattr(self, "_drive"):
            self._drive = Tier.find_cdr()
        return self._drive

    @property
    def hosts(self):
        """
        Parse the /etc/cdrapphosts.rc file to get the host names for this tier

        Return:
          dictionary of fully-qualified DNS names indexed by uppercase role
          names
        """

        if hasattr(self, "_hosts"):
            return self._hosts
        hosts = {}
        prefix = "CBIIT:" + self.name
        with open(self.APPHOSTS.format(self.drive)) as fp:
            for line in fp:
                line = line.strip()
                if line.startswith(prefix):
                    fields = line.split(":", 4)
                    if len(fields) == 5:
                        hosting, tier, role, local, domain = fields
                        hosts[role.upper()] = ".".join((local, domain))
        self._hosts = hosts
        return hosts

    @property
    def logdir(self):
        """
        Where should we write our logging information?
        """

        return self.drive + ":/cdr/Log"

    @property
    def passwords(self):
        """
        Load the database and CDR user account passwords

        Return:
          dictionary keyed by (database, username) tuple for
          database passwords or keyed by username strings for
          all other passwords
        """

        if hasattr(self, "_passwords"):
            return self._passwords
        passwords = {}
        with open(self.PASSWORDS.format(self.drive)) as fp:
            for line in fp:
                name, password = line.strip().split(":", 1)
                passwords[name.lower()] = password
        prefix = "CBIIT:" + self.name
        with open(self.DBPW.format(self.drive)) as fp:
            for line in fp:
                line = line.strip()
                if line.startswith(prefix):
                    fields = line.split(":", 4)
                    if len(fields) == 5:
                        hosting, tier, database, user, password = fields
                        passwords[(database.lower(), user.lower())] = password
        self._passwords = passwords
        return passwords

    @property
    def ports(self):
        """
        Load the TCP/IP ports for database connections from /etc/cdrdbports

        Return:
          dictionary of port integers keyed by lowercase database names
        """

        if hasattr(self, "_ports"):
            return self._ports
        ports = {}
        prefix = self.name + ":"
        with open(self.PORTS.format(self.drive)) as fp:
            for line in fp:
                line = line.strip()
                if line.startswith(prefix):
                    fields = line.split(":", 2)
                    if len(fields) == 3:
                        tier, database, port = fields
                        ports[database.lower()] = int(port)
        self._ports = ports
        return ports

    @property
    def sql_server(self):
        """
        Look up the FQDN for the SQL Server database for this tier
        """

        return self.hosts.get("DBWIN")

    def get_logger(self, name, **opts):
        """
        Create an object for recording what we do in a disk file

        The logger leaves the log file closed between writes.

        Pass:
          name - required name for the logger
          path - optional path for the log file (default self.logdir/name.log)
          format - optional override for the default log format pattern
          level - optional verboxity for logging (default INFO)
          propagate - if True, the base handler also writes our entries
          multiplex - if True, add new handler even if there already is one
          console - if True, add stram handler to write to stderr
          rolling - if True, roll over to a new log each day at midnight;
                    won't work if `path` is also passed, unless the
                    `path` value ends in a YYYY-MM-DD.log pattern.

        Return:
          logging object

        Raise:
          Exception if the logger has no handlers. This can only happen
          if the caller explicitly passes `path` as None or an empty
          string and does not set the `console` option to True.
        """

        logger = logging.getLogger(name)
        env_level = os.environ.get("CDR_LOGGING_LEVEL")
        logger.setLevel((env_level or opts.get("level") or "INFO").upper())
        logger.propagate = True if opts.get("propagate") else False
        if not logger.handlers or opts.get("multiplex"):
            formatter = self.Formatter(opts.get("format") or self.LOG_FORMAT)
            if "path" not in opts or opts.get("path"):
                path = opts.get("path")
                if not path:
                    if opts.get("rolling"):
                        now = datetime.datetime.now()
                        month = now.strftime("%Y-%m-%d")
                        path = "{}/{}-{}.log".format(self.logdir, name, month)
                    else:
                        path = "{}/{}.log".format(self.logdir, name)
                if opts.get("rolling"):
                    handler = self.RollingLogHandler(path, delay=True)
                else:
                    handler = self.ReleasingLogHandler(path, delay=True)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            if opts.get("console"):
                stream_handler = logging.StreamHandler()
                stream_handler.setFormatter(formatter)
                logger.addHandler(stream_handler)
        if not logger.handlers:
            raise Exception("logger has no handlers")
        return logger

    def password(self, user, database=None):
        """
        Look up the password for a database or CDR user account

        Pass:
          user - required string for the account name
          database - name of the database if this is a DB account

        Return:
          string for the matching password value
        """

        if database is not None:
            return self.passwords.get((database.lower(), user.lower()))
        return self.passwords.get(user.lower())

    def port(self, database):
        """
        Look up the TCP/IP port for connecting to a database

        Pass:
          string for the name of the database

        Return:
          integer for the port used for connecting to the database
        """

        return self.ports.get(database.lower())

    def __get_tier_name(self, name=None):
        """
        Determine which tier this object will be constructed for

        Pass:
          name - optional tier name

        Return:
          name if passed; otherwise the contents of the /etc/cdrtier.rc file
          (falling back on the DEV tier if that file does not exist)
        """

        if name:
            return name.upper()
        try:
            with open(self.TIER.format(self.drive)) as fp:
                return fp.read().strip()
        except:
            return "DEV"

    @staticmethod
    def set_control_value(session, group, name, value, **opts):
        """
        Add or update a row in the `ctl` table

        Called by:
          cdr.updateCtl()
          client XML wrapper command CdrSetCtl

        Required positional arguments
          session - object representing current login
          group - string naming group for which value is being installed
          name - string for value's key within the group
          value - non-empty string for the value to be stored

        Optional keyword argument:
          comment - string describing the new value

        Return:
          None
        """

        message = "Session.set_control_value({!r}, {!r})".format(group, name)
        session.log(message)

        # Make sure the user is allowed to create rows in the ctl table.
        if not session.can_do("SET_SYS_VALUE"):
            raise Exception("set_control() not authorized for this user")

        # Collect and validate the parameters.
        comment = opts.get("comment")
        now = datetime.datetime.now().replace(microsecond=0)
        if not group:
            raise Exception("Missing required group parameter")
        if not name:
            raise Exception("Missing name parameter")
        if not value:
            raise Exception("Missing value parameter")

        # Inactivate any existing rows for this group/name combination.
        assignments = "inactivated = ?"
        conditions = "grp = ? AND name = ? AND inactivated IS NULL"
        update = "UPDATE ctl SET {} WHERE {}".format(assignments, conditions)
        session.cursor.execute(update, (now, group, name))

        # Add the new value's row.
        fields = dict(
            grp=group,
            name=name,
            val=value,
            comment=comment,
            created=now
        )
        names = sorted(fields)
        values = [fields[name] for name in names]
        args = ", ".join(names), ", ".join(["?"] * len(fields))
        insert = "INSERT INTO ctl ({}) VALUES ({})".format(*args)
        try:
            session.cursor.execute(insert, values)
            session.conn.commit()
        except:
            session.logger.exception("settings.set_control() failure")
            session.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if session.cursor.fetchone().tc:
                session.cursor.execute("ROLLBACK TRANSACTION")
            raise

    @staticmethod
    def inactivate_control_value(session, group, name):
        """
        Mark a row in the `clt` table as no longer valid

        Called by:
          cdr.updateCtl()
          client XML wrapper command CdrSetCtl

        Required positional arguments
          session - object representing current login
          group - string naming group for which value is being installed
          name - string for value's key within the group

        Return:
          None
        """

        args = group, name
        message = "Session.inactivate_control_value({!r}, {!r})".format(*args)
        session.log(message)

        # Make sure the user is allowed to update rows in the ctl table.
        if not session.can_do("SET_SYS_VALUE"):
            raise Exception("set_control() not authorized for this user")

        # Validate the parameters.
        if not group:
            raise Exception("Missing required group parameter")
        if not name:
            raise Exception("Missing name parameter")

        # Inactivate all existing rows for this group/name combination.
        now = datetime.datetime.now().replace(microsecond=0)
        conditions = "grp = ? AND name = ? AND inactivated IS NULL"
        assignments = "inactivated = ?"
        update = "UPDATE ctl SET {} WHERE {}".format(assignments, conditions)
        session.cursor.execute(update, (now, group, name))
        session.conn.commit()

    @classmethod
    def find_cdr(cls):
        """
        Figure out which drive volume the CDR is installed on

        Return:
          single character representing the CDR volume's drive letter

        Raise:
          exception if the CDR is not found
        """

        for letter in "DCEFGHIJKLMNOPQRSTUVWXYZ":
            if os.path.exists(cls.APPHOSTS.format(letter)):
                return letter
        raise Exception("CDR host file not found")


    class Formatter(logging.Formatter):
        """
        Make our own logging formatter to get the time stamps right.
        """

        DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"

        converter = datetime.datetime.fromtimestamp
        def formatTime(self, record, datefmt=None):
            ct = self.converter(record.created)
            return ct.strftime(datefmt or self.DATEFORMAT)


    class ReleasingLogHandler(logging.FileHandler):
        """
        Logging file handler which leaves the file closed between writes
        """

        def emit(self, record):
            """
            Emit a record and close the file

            Pass:
              record - assembled string to be written to the log
            """

            logging.FileHandler.emit(self, record)
            self.close()


    class RollingLogHandler(ReleasingLogHandler):
        """
        Logging file handler which rolls over at midnight

        Also leaves the file closed between writes. Decided to do
        our own customization rather than use the standard library's
        `TimedRotatingFileHandler` class, but it wasn't clear how
        our customization to close the file after each write would
        interact with that class.
        """

        # The part of the log file name we change at midnight.
        PATTERN = re.compile(r"-\d\d\d\d-\d\d\-\d\d.log")

        def emit(self, record):
            """
            Emit a record and close the file

            Also, rename the file if we've moved into the next month,
            and make sure other processes can write to the file.

            Pass:
              record - assembled string to be written to the log
            """

            now = datetime.datetime.now()
            suffix = now.strftime("-%Y-%m-%d.log")
            path = self.PATTERN.sub(suffix, self.baseFilename)
            self.baseFilename = path

            # If we've rolled over to a new file, make it world-writable.
            if not os.path.exists(path):
                fp = open(path, "w")
                banner = " Rolling over to new daily session log {} "
                fp.write("{}\n".format(banner.format(path).center(120, "=")))
                fp.close()
                opts = dict(
                    stderr=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stdin=subprocess.PIPE,
                    shell=True
                )
                path = path.replace("/", "\\")
                command = 'icacls "{}" /grant Everyone:(M)'.format(path)
                try:
                    stream = subprocess.Popen(command, **opts)
                    output, error = stream.communicate()
                    code = stream.returncode
                    if code:
                        name = now.strftime("logger-%Y%m%d%H%M%S.err")
                        errpath = "d:/cdr/Log/{}".format(name)
                        with open(errpath, "a") as fp:
                            args = command, code
                            fp.write("{!r} returned code {}\n".format(*args))
                            fp.write("command output: {!r}\n".format(output))
                            fp.write("error output: {!r}\n".format(error))
                except Exception as e:
                    try:
                        import traceback
                        message = "rolling logfile to {!r}".format(path)
                        name = now.strftime("logger-%Y%m%d%H%M%S.err")
                        errpath = "d:/cdr/Log/{}".format(name)
                        with open(errpath, "a") as fp:
                            fp.write("{}\n".format(message))
                            fp.write("command: {!r}\n".format(command))
                            fp.write("{}\n".format(e))
                            traceback.print_exc(None, fp)
                    except:
                        pass

            # Proceed with writing to the log file.
            Tier.ReleasingLogHandler.emit(self, record)
