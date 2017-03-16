#----------------------------------------------------------------------
#
# Password lookup for CDR MySQL accounts.  This information is
# separated out so that CBIIT can patch in different upper-tier
# passwords without risking that updates to larger modules would
# lose the upper-tier password patches.
#
# Modified to store the passwords in /etc/cdrdbpw, so they can
# be parsed more easily by the CDR server's C++ code, now that
# CBIIT has decided they are going to used different passwords
# for the SQL Server accounts across tiers.  So this lookup is
# now for both MySQL and SQL Server passwords.
#
#----------------------------------------------------------------------

# Don't populate this until we need it.
passwords = {}

#----------------------------------------------------------------------
# Look up the password for a specific environment, tier, database,
# and account.  If the caller does not specify an account, assume
# the account name is the same as the database.  Lookups are case
# insensitive.  Password information is loaded and parsed from the
# file /etc/cdrdbpw, but only once, and that's deferred until we
# know we need it.
#----------------------------------------------------------------------
def password(env, tier, db, account=None):
    if not passwords:
        try:
            fp = open("/etc/cdrdbpw")
        except:
            raise Exception("Unable to load database passwords")
        for line in fp:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            line = line.split(":", 4)
            if len(line) != 5:
                raise Exception("Malformed database password file")
            key = "|".join(line[:4]).upper()
            passwords[key.upper()] = line[-1]
    if not account:
        account = db
    key = "%s|%s|%s|%s" % (env, tier, db, account)
    key = key.upper()
    if key not in passwords:
        raise Exception("DB account information not found")
    return passwords[key]
