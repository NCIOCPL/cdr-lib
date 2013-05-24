#----------------------------------------------------------------------
#
# $Id$
#
# Password lookup for CDR MySQL accounts.  This information is
# separated out so that CBIIT can patch in different upper-tier
# passwords without risking that updates to larger modules would
# lose the upper-tier password patches.
#
#----------------------------------------------------------------------
passwords = {
    "CBIIT": {
        "PROD": {
            "glossifier": "***REMOVED***",
            "emailers"  : "***REMOVED***",
            "dropbox"   : "***REMOVED***"
        },
        "TEST": {
            "glossifier": "***REMOVED***",
            "emailers"  : "***REMOVED***",
            "dropbox"   : "***REMOVED***"
        },
        "QA": {
            "glossifier": "***REMOVED***",
            "emailers"  : "***REMOVED***",
            "dropbox"   : "***REMOVED***"
        },
        "DEV": {
            "glossifier": "***REMOVED***",
            "emailers"  : "***REMOVED***",
            "dropbox"   : "***REMOVED***"
        },
    },
    "OCE": {
        "PROD": {
            "glossifier": "***REMOVED***",
            "emailers"  : "***REMOVED***",
            "dropbox"   : "***REMOVED***"
        },
        "TEST": {
            "glossifier": "***REMOVED***",
            "emailers"  : "***REMOVED***",
            "dropbox"   : "***REMOVED***"
        },
        "DEV": {
            "glossifier": "***REMOVED***",
            "emailers"  : "***REMOVED***",
            "dropbox"   : "***REMOVED***"
        },
    }
}

def password(env, tier, db):
    try:
        return passwords[env.upper()][tier.upper()][db.lower()]
    except KeyError:
        raise Exception("db %s not found on %s tier in the %s environment" %
                        (repr(db), repr(tier), repr(env)))
