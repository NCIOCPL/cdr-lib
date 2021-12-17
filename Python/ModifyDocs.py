"""
Harness for batch modification of CDR documents

Jobs can be run in test mode, in which case output is written to the
file system for review, or in live mode, with the transformed documents
stored in the repository's database.
"""

from copy import deepcopy
from datetime import datetime
from getpass import getpass
from os import makedirs
from random import random

import cdr
from cdrapi import db


class Job:
    """
    Top-level control for batch modification job

    Create derived classes implementing the `select()` and
    `transform()` methods appropriate to each job, and providing
    a meaningful `COMMENT` string to be stored with the modified
    documents.
    """

    LOGNAME = "ModifyDocs"
    COMMENT = "Batch transformation of CDR documents"

    def __init__(self, **opts):
        """
        Capture control settings for job

        Keyword arguments:
          user - CDR user ID of operator; required if no session
          session - name of active CDR session; required if no user
          mode - "test" (the default) or "live"
          tier - PROD|STAGE|QA|DEV (optional override)
          versions - if `False`, only create new working documents
          max_docs - optional throttle (e.g., for debugging)
          max_errors - error threshold before aborting (default is 0)
          validate - if True (the default), check for invalidating changes
          console - set to `False` to suppress console logging
          force - if `False`, don't save versions which were invalidated
                  by the change (ignored if validate is `False`); default
                  is `True`
        """

        self.__opts = opts

    @property
    def comment(self):
        """
        String explaining why we're transforming docs for this job

        Override the class-level COMMENT value to provide a custom
        string appropriate to your specific job. Or, if for some
        reason comments need to be dynamically generated (based,
        for example, on self.doc) override this @property method
        instead.
        """

        return self.COMMENT

    @property
    def creating_versions(self):
        """
        Boolean controlling whether we create new versions

        If True (the default) we create new last versions (and new
        last publishable versions if any publishable version exists).
        Otherwise, we only update the current working copy of the
        documents. In either case we create a new version to preserve
        current working document's XML if it is unversioned and
        would be lost.
        """

        if not hasattr(self, "_creating_versions"):
            self._creating_versions = self.__opts.get("versions", True)
        return self._creating_versions

    @property
    def cursor(self):
        """
        Reference to read-only CDR database cursor
        """

        if not hasattr(self, "_cursor"):
            conn = db.connect(user="CdrGuest", tier=self.tier)
            self._cursor = conn.cursor()
        return self._cursor

    @property
    def forcing(self):
        """
        Do we save documents even if the change invalidates them?

        Return:
          `True` (the default) or `False`
        """

        if not hasattr(self, "_forcing"):
            self._forcing = self.__opts.get("force", True)
        return self._forcing

    @property
    def logger(self):
        """
        Object for recording what we do

        Override the class-level value `LOGNAME` for a custom
        location for logging output. Logging output is echoed
        to the console, unless the `console` option is passed
        to the constructor with the value `False`.
        """

        if not hasattr(self, "_logger"):
            opts = dict(console=self.__opts.get("console", True))
            self._logger = cdr.Logging.get_logger(self.LOGNAME, **opts)
        return self._logger

    @property
    def max_docs(self):
        """
        Optional cap on the number of documents to process
        """

        if not hasattr(self, "_max_docs"):
            self._max_docs = self.__opts.get("max_docs")
        return self._max_docs or 9999999

    @property
    def max_errors(self):
        """
        Optional cap on the number of failures before aborting

        These are processing failures, not validation errors.
        See the `forcing` property for controlling whether validation
        errors prevent creation of new versions.
        """

        if not hasattr(self, "_max_errors"):
            self._max_errors = self.__opts.get("max_errors", 0)
        return self._max_errors

    @property
    def output_directory(self):
        """
        Directory in which to store test output

        The directory name is in the form YYYY-MM-DD_HH-MM-SS
        """

        if not hasattr(self, "_output_directory"):
            now = datetime.now()
            stamp = now.strftime("%Y-%m-%d_%H-%M-%S")
            path = "{}/GlobalChange/{}".format(cdr.BASEDIR, stamp)
            try:
                makedirs(path)
            except:
                self.logger.exception("Creating %s", path)
                raise
            self._output_directory = path
        return self._output_directory

    @property
    def session(self):
        """
        CDR session under which we save document versions

        We use an existing session or create a new one,
        depending on what has been passed to the constructor.

        Return:
          string for name of CDR session
        """

        if not hasattr(self, "_session"):
            self._session = self.__opts.get("session")
            if not self._session:
                user = self.__opts.get("user")
                if not user:
                    raise ValueError("must supply session or user name")
                password = getpass()
                self._session = str(cdr.login(user, password, tier=self.tier))
        return self._session

    @property
    def testing(self):
        """
        Flag indicating whether we are running in test mode

        Return:
          `True` (the default) or `False`
        """

        if not hasattr(self, "_testing"):
            self._testing = self.__opts.get("mode", "test").lower() != "live"
        return self._testing

    @property
    def tier(self):
        """
        CBIIT tier on which we are running (if not on localhost)

        Return:
          string naming specific tier (e.g., "PROD") or `None`
        """

        if not hasattr(self, "_tier"):
            self._tier = self.__opts.get("tier")
        return self._tier

    @property
    def validating(self):
        """
        Flag indicating whether we are validating the documents

        Return:
          `True` or `False`
        """

        if not hasattr(self, "_validating"):
            self._validating = self.__opts.get("validate", True)
        return self._validating

    def get_blob(self, doc_id):
        """Override this if you have blobs which need to be saved.

        Pass:
            doc_id - integer for CDR document ID

        Return:
            bytes or None (this stub version always returns None)
        """

        return None

    def run(self):
        """
        Transform a batch of CDR documents

        The overridden `select()` and `transform()` methods are
        invoked by this base class method.
        """

        self.failures = {}
        self.successes = set()
        self.unavailable = set()
        if not self.testing:
            self.logger.info("Running in real mode, updating the database")
        else:
            self.logger.info("Saving test output to %r", self.output_directory)
        if self.tier:
            self.logger.info("Running on {}".format(self.tier))
        start = datetime.now()
        doc_ids = self.select()
        counts = dict(
            processed=0,
            saved=0,
            versions=0,
            locked=0,
            errors=0
        )
        types = {}
        self.logger.info("%d documents selected", len(doc_ids))
        self.logger.info("Purpose: %r", self.comment)
        for doc_id in doc_ids:
            self.doc = None
            needs_unlock = True
            try:
                self.doc = self.Doc(self, doc_id)
                self.logger.info("Processing %s", self.doc)
                self.doc.save_changes()
                self.successes.add(doc_id)
                if self.doc.saved:
                    counts["saved"] += 1
                if "cwd" in self.doc.changed:
                    counts["versions"] += 1
                if "lastv" in self.doc.changed:
                    counts["versions"] += 1
                if "lastp" in self.doc.changed:
                    counts["versions"] += 1
                for version_type in self.doc.saved:
                    types[version_type] = types.get(version_type, 0) + 1
            except Job.DocumentLocked as info:
                self.unavailable.add(doc_id)
                needs_unlock = False
                counts["locked"] += 1
                self.logger.warning(str(info))
            except Exception as e:
                self.failures[doc_id] = str(e)
                self.logger.exception("Document %d", doc_id)
                counts["errors"] += 1
                if counts["errors"] > self.max_errors:
                    message = "Stopping after %d errors"
                    self.logger.error(message, counts["errors"])
                    break
            if needs_unlock:
                cdr.unlock(self.session, doc_id, tier=self.tier)
            counts["processed"] += 1
            if counts["processed"] >= self.max_docs:
                message = "Stopping after processing %d documents"
                self.logger.info(message, counts["processed"])
                break
        details = []
        if types:
            details = ["Specific versions saved:"]
            for key in sorted(types):
                details.append("  {} = {:d}".format(key, types[key]))
            details.append("")
        elapsed = datetime.now() - start
        self.logger.info("""\
Run completed.
   Docs examined    = {processed:d}
   Docs changed     = {saved:d}
   Versions changed = {versions:d}
   Could not lock   = {locked:d}
   Errors           = {errors:d}
   Time             = {time}
{details}""".format(details="\n".join(details), time=elapsed, **counts))
        if not self.__opts.get("session"):
            cdr.logout(self.session, tier=self.tier)

    def save_pair(self, doc_id, before, after, pair_type, errors=None):
        """
        Write before and after XML to the file system

        Used in test mode to save everything to the file system
        for review, instead of writing to the repository.

        Pass:
          doc_id - integer identifying the document being processed
          before - serialized XML for the document before transformation
          after - serialzed XML for the document after transformation
          pair_type - string identifying what was transformed (cwd|pub|lastv)
          errors - optional sequence of validation error messages
        """

        args = self.output_directory, cdr.normalize(doc_id), pair_type
        old_path = "{}/{}.{}old.xml".format(*args)
        new_path = "{}/{}.{}new.xml".format(*args)
        diff_path = "{}/{}.{}.diff".format(*args)
        errors_path = "{}/{}.{}.NEW_ERRORS.txt".format(*args)
        try:
            with open(old_path, "wb") as fp:
                fp.write(before)
            with open(new_path, "wb") as fp:
                fp.write(after)
            diff = cdr.diffXmlDocs(before, after) or b"-- No differences --"
            with open(diff_path, "wb") as fp:
                fp.write(diff)
            if errors:
                with open(errors_path, "wb") as fp:
                    for error in errors:
                        fp.write(error.encode("utf-8") + b"\n")
        except:
            self.logger.exception("Failure writing XML pair")
            raise

    def select(self):
        """
        Determine which documents are to be modified

        Invoked once for each job run. Must be implemented by
        the job's derived class.

        Return:
           sequence of CDR document ID integers
        """

        raise NotImplementedError("must override select() method")

    def transform(self, doc):
        """
        Modify a single CDR document version

        Must be implemented by the job's derived class.

        Pass:
          doc - reference to `cdr.Doc` object for document to be modified

        Return:
          serialized transformed document XML, encoded as UTF-8
        """

        raise NotImplementedError("must override transform() method")


    class DocumentLocked(Exception):
        """
        Custom exception indicating that we can't check out a document
        """


    class Doc(object):
        """
        Single CDR document to be transformed
        """

        def __init__(self, job, doc_id):

            self.job = job
            self.id = doc_id
            self.cdr_id = cdr.normalize(doc_id)
            self.versions = cdr.lastVersions("guest", doc_id, tier=job.tier)
            self.status = cdr.getDocStatus("guest", doc_id, tier=job.tier)
            self.saved = set()
            self.doc_objects = {}
            self.transformed_xml = {}
            self.errors = {}
            self.changed = set()
            self.load_versions()

        def load_versions(self):
            """
            Check out and transform the XML for this document

            We get the current working document (CWD), the last version
            (if any), and the last publishable version (if any).
            """

            session = self.job.session
            checkout = "N" if self.job.testing else "Y"
            opts = dict(checkout=checkout, getObject=True, tier=self.job.tier)
            try:
                cwd = cdr.getDoc(session, self.id, **opts)
                self.doc_objects["cwd"] = cwd
            except Exception as e:
                message = "Unable to check out {}: {}".format(self.cdr_id, e)
                raise Job.DocumentLocked(message)
            last_version, last_publishable_version, changed = self.versions
            errors = self.preserve = None
            if changed == "Y" or last_version < 1:
                self.preserve = deepcopy(cwd)
            new_xml = self.transformed_xml["cwd"] = self.job.transform(cwd)
            if self.job.validating:
                args = session, cwd.type, cwd.xml, new_xml
                errors = cdr.valPair(*args, tier=self.job.tier)
                self.errors["cwd"] = errors
            if self.job.creating_versions:
                if last_version > 0:
                    if changed == "Y":
                        opts["version"] = last_version
                        try:
                            lastv = cdr.getDoc(session, self.id, **opts)
                            self.doc_objects["lastv"] = lastv
                        except Exception as e:
                            msg = "Failure retrieving lastv ({:d}) for {}: {}"
                            args = last_version, self.cdr_id, e
                            raise Exception(msg.format(*args))
                        new_xml = self.job.transform(lastv)
                        self.transformed_xml["lastv"] = new_xml
                        if self.job.validating:
                            args = session, lastv.type, lastv.xml, new_xml
                            errors = cdr.valPair(*args, tier=self.job.tier)
                            self.errors["lastv"] = errors
                    else:
                        lastv = self.doc_objects["lastv"] = cwd
                        self.transformed_xml["lastv"] = new_xml
                        self.errors["lastv"] = errors
                if last_publishable_version > 0:
                    if last_publishable_version != last_version:
                        opts["version"] = last_publishable_version
                        try:
                            lastp = cdr.getDoc(session, self.id, **opts)
                            self.doc_objects["lastp"] = lastp
                        except Exception as e:
                            msg = "Failure retrieving lastp ({:d}) for {}: {}"
                            args = last_publishable_version, self.cdr_id, e
                            raise Exception(msg.format(*args))
                        new_xml = self.job.transform(lastp)
                        self.transformed_xml["lastp"] = new_xml
                        if self.job.validating:
                            args = session, lastp.type, lastp.xml, new_xml
                            errors = cdr.valPair(*args, tier=self.job.tier)
                            self.errors["lastp"] = errors
                    else:
                        self.doc_objects["lastp"] = lastv
                        self.transformed_xml["lastp"] = new_xml
                        self.errors["lastp"] = errors

        def save_changes(self):
            """
            Save modified XML to the repository or the file system

            In live mode, saves all versions of a document needing to be saved.
            In test mode, writes to output files, leaving the database alone.

            Uses the following logic:

            Let PV     = publishable version
            Let LPV    = latest publishable version
            Let LPV(t) = transformed copy of latest publishable version
            Let NPV    = non-publishable version
            Let CWD    = copy of current working document when job begins
            Let CWD(t) = transformed copy of CWD
            Let LV     = latest version (regardless of whether publishable)
            Let LV(t)  = transformed copy of LV
            Let LS     = last saved copy of document (versioned or not)

            If CWD <> LV:
                Create new NPV from unmodified CWD
                Preserves the original CWD which otherwise would be lost.
            If LPV(t) <> LPV:
                Create new PV using LPV(t)
            If LV(t) <> LV:
                Create new NPV from LV(t)
            If CWD(t) <> LS:
                Create new CWD using CWD(t)

            Often, one or more of the following is true:
                CWD==LV, CWD==LPV, LV==LPV

            NOTE: If versions are equivalent, references to doc objects are
            manipulated in tricky ways to ensure that the right thing is done.
            """

            logger = self.job.logger
            last_saved_xml = errors_to_log = None
            val_warning = "%s for %s made invalid by change not stored"
            cwd = self.doc_objects.get("cwd")
            lastp = self.doc_objects.get("lastp")
            lastv = self.doc_objects.get("lastv")
            if self.job.testing:
                new_xml = self.transformed_xml.get("cwd")
                errors = self.errors.get("cwd")
                args = self.id, cwd.xml, new_xml, "cwd", errors
                self.job.save_pair(*args)
            new_xml = self.transformed_xml.get("lastp")
            if lastp and self.compare(lastp.xml, new_xml):
                errors = errors_to_log = self.errors.get("lastp")
                if self.job.testing:
                    args = self.id, lastp.xml, new_xml, "pub", errors
                    self.job.save_pair(*args)
                else:
                    lastp.xml = new_xml
                    if not errors or self.job.forcing:
                        self.save("new pub", str(lastp), "Y", "Y", "Y")
                        last_saved_xml = new_xml
                    else:
                        logger.warning(val_warning, "new pub", self.cdr_id)
                self.changed.add("lastp")
                self.changed.add("cwd")

            new_xml = self.transformed_xml.get("lastv")
            if lastv and self.compare(lastv.xml, new_xml):
                errors = self.errors.get("lastv")
                errors_to_log = errors_to_log or errors
                if self.job.testing:
                    args = self.id, lastv.xml, new_xml, "lastv", errors
                    self.job.save_pair(*args)
                lastv.xml = new_xml
                if not self.job.testing:
                    if self.job.forcing or not errors:
                        val = "Y" if self.ever_validated else "N"
                        self.save("new ver", str(lastv), "Y", "N", val)
                        last_saved_xml = new_xml
                    else:
                        logger.warning(val_warning, "new ver", self.cdr_id)
                self.changed.add("lastv")
                self.changed.add("cwd")
            cwd_save_needed = False
            new_xml = self.transformed_xml.get("cwd")
            if last_saved_xml:
                if self.compare(last_saved_xml, new_xml):
                    cwd_save_needed = True
            elif self.compare(cwd.xml, new_xml):
                cwd_save_needed = True
            if cwd_save_needed:
                self.changed.add("cwd")
                errors = self.errors.get("cwd")
                errors_to_log = errors_to_log or errors
                if not self.job.testing:
                    cwd.xml = new_xml
                    if self.job.forcing or not errors:
                        val = "Y" if self.ever_validated else "N"
                        self.save("new cwd", str(cwd), "N", "N", val)
                    else:
                        logger.warning(val_warning, "new cwd", self.cdr_id)
            if errors_to_log:
                for error in errors_to_log:
                    logger.warning("%s: %r", self.cdr_id, error.encode("utf-8"))

        def save(self, label, doc_str, ver, pub, val):
            """
            Invoke the CdrRepDoc command (or its local equivalent)

            If what we're about to do would overwrite unversioned XML,
            we recurse to create a numbered version to capture the current
            working document before continuing with the requested save.
            Keep track of which versions we have saved in `self.saved`.

            Pass:
              label - string identifying what we're saving (e.g., "new cwd")
              doc_str - serialized CDR document object
              ver - "Y" or "N" (should we create a version?)
              pub - "Y" or "N" (should the version be publishable?)
              val - "Y" or "N" (should we validate the document?)
            """

            if self.preserve:
                preserve = str(self.preserve)
                self.preserve = None
                self.save("old cwd", preserve, "Y", "N", "N")
            args = self.id, ver, pub, val, label
            self.job.logger.info("save(%d, ver=%r pub=%r val=%r %s)", *args)
            opts = dict(
                doc=doc_str,
                ver=ver,
                val=val,
                publishable=pub,
                reason=self.job.comment,
                comment=self.job.comment,
                show_warnings=True,
                tier=self.job.tier,
                blob=self.blob,
            )
            doc_id, errors = cdr.repDoc(self.job.session, **opts)
            if not doc_id:
                args = self.job, doc_str, ver, pub, val, errors
                self.capture_transaction(*args)
                args = self.cdr_id, errors
                message = "Failure saving changes for {}: {}".format(*args)
                raise Exception(message)
            if errors:
                opts = dict(asSequence=True, errorsExpected=False)
                warnings = cdr.getErrors(errors, **opts)
                for warning in warnings:
                    self.job.logger.warning("%s: %s" % (self.cdr_id, warning))
            self.saved.add(label)

        @property
        def blob(self):
            """Binary object to save with the document, if any."""

            if not hasattr(self, "_blob"):
                self._blob = self.job.get_blob(self.id)
            return self._blob

        @property
        def ever_validated(self):
            """
            Boolean flag indicating whether this doc has ever been validated

            It is important to avoid running validation on a document
            for the first time unless a publishable version is being
            created (in which case validation is mandatory), because
            the validation wipes out the XMetaL prompt PIs.
            """

            if not hasattr(self, "_ever_validated"):
                if "lastp" in self.doc_objects:
                    self._ever_validated = True
            if not hasattr(self, "_ever_validated"):
                query = db.Query("document", "val_status")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.job.cursor).fetchone()
                if not row:
                    message = "Failure retrieving val_status for {}"
                    raise Exception(message.format(self.cdr_id))
                if row.val_status in ("I", "V"):
                    self._ever_validated = True
            if not hasattr(self, "_ever_validated"):
                query = db.Query("doc_version", "COUNT(*) AS n")
                query.where(query.Condition("id", self.id))
                query.where("val_status IN ('I', 'V')")
                row = query.execute(self.job.cursor).fetchone()
                self._ever_validated = row.n > 0
            return self._ever_validated

        def __str__(self):
            """
            String representation of document for logging
            """

            last_version, last_publishable_version, changed = self.versions
            cwd_version = "unversioned" if changed == "Y" else last_version
            segments = [self.cdr_id]
            if self.status != "A":
                segments.append(" (BLOCKED)")
            segments.append(" [")
            if last_publishable_version > 0:
                segments.append("pub:{:d}".format(last_publishable_version))
            segments.append("/")
            if last_version > 0:
                segments.append("last:{:d}".format(last_version))
            segments.append("/cwd:{}]".format(cwd_version))
            return "".join(segments)

        @staticmethod
        def compare(old_xml, new_xml):
            """
            Compare normalized document XML

            Pass:
              old_xml - original serialized XML for document versions
              new_xml - transformed XML

            Return:
              `True` if they differ, otherwise `False`
            """

            return cdr.compareXmlDocs(old_xml, new_xml)

        @staticmethod
        def capture_transaction(job, doc, ver, pub, val, error):
            """
            Create a repro case for a failed document save

            The document save will not have failed because the document was
            checked out by another user, as that condition is detected and
            handled elsewhere. This function creates a script in the standard
            CDR log directory which can be used to recreate the failure.

            To run it, you must do the following on the correct tier:
                1. Create a CDR session
                2. Use it to check out the document
                3. Pass your session ID to this script on the command line

            Required positional arguments:
              job - reference to the `ModifyDocs.Job` object
              doc - serialized `cdr.Doc` object
              ver - "Y" or "N" (should we create a version?)
              pub - "Y" or "N" (should the version be publishable?)
              val - "Y" or "N" (should we validate the document?)
              error - string describing the failure
            """

            stamp = str(datetime.now())
            for c in "-: ":
                stamp = stamp.replace(c, "")
            stamp += "-" + str(random())
            args = cdr.DEFAULT_LOGDIR, stamp
            path = "{}/ModifyDocsFailure-{}.py".format(*args)
            log = "ModifyDocsTest-{}.out".format(stamp)
            assertion = "session is required"
            warning = "make sure the doc is checked out before running this"
            with open(path, "w") as fp:
                fp.write("import sys\nimport cdr\n")
                fp.write("assert len(sys.argv) > 1, {!r}\n".format(assertion))
                fp.write("print {!r}\n".format(warning))
                fp.write("# error: {!r}\n".format(error))
                fp.write("response = cdr.repDoc(\n")
                fp.write("    sys.argv[1],\n")
                fp.write("    doc={!r},\n".format(doc))
                fp.write("    ver={!r},\n".format(ver))
                fp.write("    val={!r},\n".format(val))
                fp.write("    publishable={!r},\n".format(pub))
                fp.write("    reason={!r},\n".format(job.comment))
                fp.write("    comment={!r},\n".format(job.comment))
                if job.tier:
                    fp.write("    tier={!r},\n".format(job.tier))
                fp.write("    show_warnings=True)\n")
                fp.write("print response[0] and 'Success' or 'Failure'\n")
                fp.write("with open({!r}, 'w') as fp:\n".format(log))
                fp.write("    fp.write(repr(response))\n")
                fp.write("print 'response in {}'\n".format(log))
