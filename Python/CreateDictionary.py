#!/usr/bin/env python

"""
  Create Spanish dictionary files to be used for spell check in XMetal.
  We're creating the files from the glossary of cancer terms for HP 
  and patients.
  The files are created in the directory /tmp with the
  name: dict_[hp|patient]_[tier].dic
"""

import sys
import cdr
import datetime
from cdrapi import db
from cdrcgi import Controller
from pathlib import Path
from cdrapi.settings import Tier


class CreateDictionary(Controller):
    """Creating a Spanish dictionary file to be used within XMetaL's
    spellchecker
    Required argument:
        audience  One of 'hp' or 'patient'

    Optional arguments:
        tier        specify the tier to connect to
        log-level   defaults to 'info'
    """
    TIER = Tier()
    LOGNAME = "SpanishSpellcheckerFiles"

    def __init__(self, options):
        self.log_level = options.get("log_level") or "info"
        logger = cdr.Logging.get_logger(self.LOGNAME, level=self.log_level)
        self.log = logger

        self.options = options

        self.max_docs = None
        self.tier = self.TIER.name

        if "max_docs" in self.options:
            self.max_docs = self.options["max_docs"]
        if "tier" in self.options:
            self.tier = self.options["tier"]

        self.audience = 'Patient' \
                        if self.options["audience"].upper() == 'PATIENT' \
                        else 'Health professional'

        if not self.options["audience"].upper() in ['HP', 'PATIENT']:
            self.log.error("    Invalid audience (HP, Patient): ")
            sys.exit("Invalid audience (HP, Patient): "
                     f"{self.options['audience']}")


    def run(self):
        """
        Building SQL query for HP and Patient dictionaries

        Return pathname of file created
        """
        self.log.info("    " + 40*"=")
        dic_path = "/GlossaryTermConcept/TranslatedTermDefinition/Dictionary"
        aud_path = "/GlossaryTermConcept/TermDefinition/Audience"

        cursor = db.connect(user="CdrGuest", tier=self.TIER,
                                             timeout=300).cursor()

        query = db.Query("query_term qt", "gl.doc_id", "s.value", 
                                          "qt.value").order("s.value")
        query.outer("query_term aud", 
                    "aud.doc_id = qt.doc_id",
                    "aud.path like '/GlossaryTermConcept/%/Audience'",
                    "LEFT(aud.node_loc, 4) = LEFT(qt.node_loc, 4)")
        query.join( "query_term gl", 
                    "gl.int_val = aud.doc_id",
                    "gl.path = '/GlossaryTermName/GlossaryTermConcept/@cdr:ref'")
        query.join( "pub_proc_cg cg", "cg.id = gl.doc_id")
        query.join( "query_term t", 
                    "t.doc_id = cg.id",
                    "t.path = '/GlossaryTermName/TermName/TermNameString'")
        query.outer("query_term s", 
                    "s.doc_id = cg.id",
                    "s.path = '/GlossaryTermName/TranslatedName/TermNameString'")
        query.where(query.Condition("aud.value", self.audience))
        query.where("s.value IS NOT NULL")
        if self.audience == 'Patient':
            query.where(query.Condition("qt.path", dic_path))
        else:
            query.where(query.Condition("qt.path", aud_path))

        if self.max_docs is not None:
           query.limit(self.max_docs)

        if self.log_level == 'debug':
            self.log.debug("    " + 40*"=")
            self.log.debug(query)
            self.log.debug("    " + 40*"=")

        self.log.info(f"    Creating {self.audience} audience from {self.tier.upper()}")
        rows = query.execute(cursor).fetchall()
        self.log.info(f"    {len(rows):d} Spanish glossary terms selected")

        # Create file with the Spanish terms one per line
        # -----------------------------------------------
        dict_name = "patient" if self.audience == "Patient" else "hp"
        stamp = datetime.datetime.now().strftime("-%Y%m%d%H%M%S")
        pathname = Path('/tmp', f'dict_{dict_name}_{self.tier}{stamp}.dic')
        self.log.info(f"    creating {pathname}")

        with open(pathname, mode='w+', encoding='utf-8') as d:
            d.write(str(len(rows)) + '\n')
            for row in rows:
                d.write(row[1] + '\n')

        return(pathname)


def main():
    """
    Handling command line arguments
    """
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--audience", required=True,
                        help="hp or patient")
    parser.add_argument("--max-docs", type=int,
                        help="max number of terms to retrieve")
    parser.add_argument("--tier", 
                        help="dev, qa, stage, or prod")
    parser.add_argument("--log-level", 
                        help="info, debug, error")
    opts = vars(parser.parse_args())  # vars() returns the audience
    CreateDictionary(opts).run()


if __name__ == "__main__":
    main()
