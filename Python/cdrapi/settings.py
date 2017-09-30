"""
Collection of tier-specific CDR settings.
"""

import os

class Tier:
    APPHOSTS = "{}:/etc/cdrapphosts.rc"
    TIER = "{}:/etc/cdrtier.rc"
    PASSWORDS = "{}:/etc/cdrpw"
    DBPW = "{}:/etc/cdrdbpw"
    PORTS = "{}:/etc/cdrdbports"
    def __init__(self, tier=None):
        self.drive = self.find_cdr()
        self.name = self.get_tier_name(tier)
        self.passwords = self.load_passwords()
        self.hosts = self.load_hosts()
        self.ports = self.load_ports()
        #print(self.drive)
        #print(self.name)
        #print(self.passwords)
        #print(self.hosts)
        #print(self.ports)
    def password(self, user, database=None):
        if database is not None:
            return self.passwords.get((database.lower(), user.lower()))
        return self.passwords.get(user.lower())
    def port(self, database):
        return self.ports.get(database.lower())
    def sql_server(self):
        return self.hosts.get("DBWIN")
    def get_tier_name(self, name=None):
        if name:
            return name.upper()
        return open(self.TIER.format(self.drive)).read().strip()
    def load_passwords(self):
        passwords = {}
        for line in open(self.PASSWORDS.format(self.drive)):
            name, password = line.strip().split(":", 1)
            passwords[name.lower()] = password
        prefix = "CBIIT:" + self.name
        for line in open(self.DBPW.format(self.drive)):
            line = line.strip()
            if line.startswith(prefix):
                fields = line.split(":", 4)
                if len(fields) == 5:
                    hosting, tier, database, user, password = fields
                    passwords[(database.lower(), user.lower())] = password
        return passwords
    def load_hosts(self):
        hosts = {}
        prefix = "CBIIT:" + self.name
        for line in open(self.APPHOSTS.format(self.drive)):
            line = line.strip()
            if line.startswith(prefix):
                fields = line.split(":", 4)
                if len(fields) == 5:
                    hosting, tier, role, local, domain = fields
                    hosts[role.upper()] = ".".join((local, domain))
        return hosts
    def load_ports(self):
        ports = {}
        prefix = self.name + ":"
        for line in open(self.PORTS.format(self.drive)):
            line = line.strip()
            if line.startswith(prefix):
                fields = line.split(":", 2)
                if len(fields) == 3:
                    tier, database, port = fields
                    ports[database.lower()] = int(port)
        return ports
    @classmethod
    def find_cdr(cls):
        for letter in "DCEFGHIJKLMNOPQRSTUVWXYZ":
            if os.path.exists(cls.APPHOSTS.format(letter)):
                return letter
        raise Exception("CDR host file not found")
