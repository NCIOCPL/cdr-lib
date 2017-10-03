"""
Collect tier-specific CDR settings.
"""

import datetime
import logging
import os

class Tier:
    """
    Collection of tier-specific CDR values

    Class values:
      Pattern strings for the paths to files where the values are stored.
      LOG_FORMAT - pattern for entries in our log files

    Attributes:
      drive - letter where the CDR is installed (D on the CDR servers)
      name - string containing the name of the tier represented by the values
      passwords - dictionary of passwords keyed by a tuple of a lowercase
                  database name and user name for database accounts, or
                  keyed by the lowercase user name string for all other
                  passwords
      hosts - dictionary of this tier's fully qualified DNS names, keyed
              by the uppercase role name
      ports - dictionary of TCP/IP port integers, keyed by database name
    """

    APPHOSTS = "{}:/etc/cdrapphosts.rc"
    TIER = "{}:/etc/cdrtier.rc"
    PASSWORDS = "{}:/etc/cdrpw"
    DBPW = "{}:/etc/cdrdbpw"
    PORTS = "{}:/etc/cdrdbports"
    LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

    def __init__(self, tier=None):
        """
        Load the values for this tier

        Pass:
          tier - optional string naming a specific tier; if not provided
                 the value in /etc/cdrtier.rc will be used
        """

        self.drive = self.find_cdr()
        self.name = self.get_tier_name(tier)
        self.passwords = self.load_passwords()
        self.hosts = self.load_hosts()
        self.ports = self.load_ports()

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

    def sql_server(self):
        """
        Look up the FQDN for the SQL Server database for this tier
        """

        return self.hosts.get("DBWIN")

    def get_tier_name(self, name=None):
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

    def load_passwords(self):
        """
        Load the database and CDR user account passwords

        Return:
          dictionary keyed by (database, username) tuple for
          database passwords or keyed by username strings for
          all other passwords
        """

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
        return passwords

    def load_hosts(self):
        """
        Parse the /etc/cdrapphosts.rc file to get the host names for this tier

        Return:
          dictionary of fully-qualified DNS names indexed by uppercase role
          names
        """

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
        return hosts

    def load_ports(self):
        """
        Load the TCP/IP ports for database connections from /etc/cdrdbports

        Return:
          dictionary of port integers keyed by lowercase database names
        """

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
        return ports

    def get_logger(self, name, **opts):
        """
        Create an object for recording what we do in a disk file

        Pass:
          name - required name for the logger
          path - optional path for the log file (default self.logdir/name.log)
          format - optional override for the default log format pattern
          level - optional verboxity for logging (default INFO)
          propagate - if True, the base handler also writes our entries
          multiplex - if True, add new handler even if there already is one
          console - if True, add stram handler to write to stderr

        Return:
          logging object

        Raise:
          Exception if the logger has no handlers. This can only happen
          if the caller explicitly passes `path` as None or an empty
          string and does not set the `console` option to True.
        """

        logger = logging.getLogger(name)
        logger.setLevel((opts.get("level") or "INFO").upper())
        logger.propagate = True if opts.get("propagate") else False
        if not logger.handlers or opts.get("multiplex"):
            formatter = self.Formatter(opts.get("format") or self.LOG_FORMAT)
            if "path" not in opts or opts.get("path"):
                path = opts.get("path")
                if not path:
                    path = "{}/{}.log".format(self.get_logdir(), name)
                handler = logging.FileHandler(path)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            if opts.get("console"):
                stream_handler = logging.StreamHandler()
                stream_handler.setFormatter(formatter)
                logger.addHandler(stream_handler)
        if not logger.handlers:
            raise Exception("logger has no handlers")
        return logger

    def get_logdir(self):
        return self.drive + ":/cdr/Log"

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
