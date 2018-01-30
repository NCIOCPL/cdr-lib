"""
Process a queued publishing job
"""

import argparse
import base64
import csv
import datetime
import gc
import glob
import os
import pdb
import re
import threading
import time
from lxml import etree
import objgraph
import cdr
import cdr2gk
from cdrapi import db as cdrdb
from cdrapi.docs import Doc
from cdrapi.publishing import Job
from cdrapi.settings import Tier
from cdrapi.users import Session
from AssignGroupNums import GroupNums

try:
    basestring
    base64encode = base64.encodestring
    base64decode = base64.decodestring
except:
    base64encode = base64.encodebytes
    base64decode = base64.decodebytes
    basestring = str, bytes
    unicode = str


class Control:
    """
    Top-level object for CDR publishing job processing
    """

    MANIFEST_LOCK = threading.Lock()
    DIRECTORY_LOCK = threading.Lock()
    FAILURE_LOCK = threading.Lock()
    PROCESSED_LOCK = threading.Lock()
    SENDER = "NCIPDQOperator@mail.nih.gov"
    PUSH = "Push_Documents_To_Cancer.Gov"
    PUSH_STAGE = "pub_proc_cg_work"
    RUN = "In process"
    SUCCESS = "Success"
    FAILURE = "Failure"
    VERIFYING = "Verifying"
    WAIT = "Waiting user approval"
    COMPLETED_STATUSES = SUCCESS, FAILURE, VERIFYING
    XMLDECL = re.compile(r"<\?xml[^?]+\?>\s*")
    DOCTYPE = re.compile(r"<!DOCTYPE[^>]*>\s*")
    NORMALIZE_SPACE = re.compile(r"\s+")
    EXCLUDED = ["Country"]
    FAILURE_SLEEP = 5
    GK_TYPES = dict(
        GlossaryTermName="GlossaryTerm",
        Person="GeneticsProfessional"
    )
    MEDIA_TYPES = dict(
        jpg="image/jpeg",
        gif="image/gif",
        mp3="audio/mpeg"
    )

    def __init__(self, job_id, **opts):
        """
        Stash away what we'll need for running a publishing job
        """

        self.__job_id = job_id
        self.__opts = opts

    def publish(self):
        """
        Run the publishing job

        Processing is passed off to a separate private method, which
        we wrap in a `try` block to catch all exceptions.
        """

        try:
            self.__publish()
        except Exception as e:
            self.logger.exception("Job %d failure", self.job.id)
            self.update_status(self.FAILURE, str(e))
            if self.work_dir and os.path.isdir(self.work_dir):
                os.rename(self.work_dir, self.failure_dir)
            self.notify("Job failed: {}".format(e))
        #Doc.memory_log.close()
        roots = objgraph.get_leaking_objects()
        objgraph.show_refs(roots[:10], refcounts=True)
        #, filename="c:/tmp/root.png")

    def __publish(self):
        """
        Run the publishing job

        The processing work is extracted out to this private method
        so we can trap all exceptions.

        There are basically three flavors of job:
          - scripted (the script embodies all of the logic for the job)
          - export (write filtered documents to the file syatem)
          - push (push the results of an export job to cancer.gov's GateKeeper
        """

        self.update_status(self.RUN, "Job started")
        if self.job.subsystem.script:
            self.run_script()
        else:
            start = datetime.datetime.now()
            self.logger.debug("Job %d parms=%s", self.job.id, self.job.parms)
            args = self.job.id, self.job.subsystem.options
            self.logger.debug("Job %d opts=%s", *args)
            self.processed = set()
            if "SubSetName" in self.job.parms:
                verb = "Pushed"
                self.push_docs()
            else:
                verb = "Exported"
                self.export_docs()
            if self.job_failed:
                self.update_status(self.FAILURE)
                message = "Job failed"
            else:
                elapsed = (datetime.datetime.now() - start).total_seconds()
                args = verb, len(self.processed), elapsed
                message = "{} {:d} documents in {:f} seconds".format(*args)
                if self.job.parms.get("ReportOnly") == "Yes":
                    message += " (status set to failure for 'ReportOnly' job)"
                    self.update_status(self.FAILURE, message)
                elif not self.status == self.VERIFYING:
                    self.update_status(self.SUCCESS, message)
                else:
                    self.post_message(message)
            self.notify(message, with_link=True)

    def avoid_collisions(self):
        """
        Rename directories which would conflict with this job's output

        If we're running on a lower tier which has been refreshed from
        the production database, it's possible that the current job
        number was already used. If so, move any directories left over
        from such jobs out of the way. Let exceptions bubble up.
        """

        output_dir = self.output_dir
        if "SubSetName" not in self.job.parms and output_dir:
            for path in glob.glob(output_dir + "*"):
                if os.path.isdir(path) and "-" not in os.path.basename(path):
                    stat = os.stat(path)
                    localtime = time.localtime(stat.st_mtime)
                    stamp = time.strftime("%Y%m%d%H%M%S", localtime)
                    new = "{}-{}".format(path, stamp)
                    self.logger.warning("Renaming {} to {}".format(path, new))
                    os.rename(path, new)

    def create_push_job(self):
        """
        Queue up a job to push the documents we just exported
        """

        default_desc = "{} push job".format(self.job.subsystem.name)
        desc = self.job.parms.get("GKPushJobDescription") or default_desc
        parms = dict(
            GKServer=self.job.parms["GKServer"],
            GKPubTarget=self.job.parms["GKPubTarget"],
            GKPushJobDescription=desc,
            InteractiveMode=self.job.parms.get("InteractiveMode", "No")
        )
        opts = dict(
            system="Primary",
            subsystem="{}_{}".format(self.PUSH, self.job.subsystem.name),
            parms=parms,
            email=self.job.email,
            no_output=True
        )
        job = Job(self.session, **opts)
        job.create()
        self.logger.info("Job %d: created push job %d", self.job.id, job.id)

    def export_doc(self, doc, spec):
        """
        Filter and store a CDR document in the file system

        Pass:
          doc - reference to `Doc` object
          spec - reference to `Job.Subsystem.Specification` object
                 which controls how we prepare the document
        """

        doctype = doc.doctype.name
        try:
            self.__export_doc(doc, spec)
        except Exception as e:
            self.logger.exception("Failure exporting %s", doc.cdr_id)
            doc.add_error(str(e), level=Doc.LEVEL_ERROR, type="exporting")
            self.record_failure(doc)

    def __export_doc(self, doc, spec):
        """
        Do the actual work of filtering and storing a CDR document

        This helper method is separated out to facilitate wrapping
        the work in a `try` block for capturing any exceptions which
        are raised.

        Pass:
          doc - reference to `Doc` object
          spec - reference to `Job.Subsystem.Specification` object
        """

        topstart = datetime.datetime.now()
        with self.PROCESSED_LOCK:
            if doc.id in self.processed:
                args = self.job.id, doc.cdr_id
                self.logger.warning("Job %d: %s already processed", *args)
                return
            self.processed.add(doc.id)
        filename = doc.export_filename
        directory = self.work_dir
        if spec.subdirectory:
            directory += "/{}".format(spec.subdirectory)
        self.logger.info("Exporting %s", filename)
        if doc.doctype.name == "Media":
            self.write_doc(doc.blob, directory, filename)
            blob_date = str(doc.blob_date)[:10]
            title = doc.title.replace("\r", "").replace("\n", " ")
            values = doc.export_filename, blob_date, title.strip()
            with self.MANIFEST_LOCK:
                self.manifest.append(values)
        else:
            start = datetime.datetime.now()
            result = self.filter_doc(doc, spec)
            elapsed = (datetime.datetime.now() - start).total_seconds()
            self.logger.debug("Filtered %s in %f seconds", doc.cdr_id, elapsed)
            if self.job.parms.get("ValidateDocs") == "Yes":
                start = datetime.datetime.now()
                dtd_name = self.job.parms["DTDFileName"]
                dtd_path = os.path.join(cdr.PDQDTDPATH, dtd_name)
                errors = self.validate_doc(result.result_tree, dtd_path)
                elapsed = (datetime.datetime.now() - start).total_seconds()
                args = doc.cdr_id, elapsed
                self.logger.debug("Validated %s in %f seconds", *args)
                if errors:
                    args = self.job.id, doc.cdr_id
                    self.logger.warning("Job %d: %s invalid", *args)
                    directory = self.work_dir + "/InvalidDocs"
                    for error in errors:
                        opts = dict(level=error.level_name.lower())
                        doc.add_error(error.message, **opts)
                        args = args[0], args[1], opts["level"], error.message
                        self.logger.debug("Job %d: %s: [%s] %s", *args)
                    self.record_failure(doc)
            xml = etree.tostring(result.result_tree, encoding="utf-8")
            xml = xml.replace(b"\r", b"").strip() + b"\n"
            self.write_doc(xml, directory, filename)
        elapsed = (datetime.datetime.now() - topstart).total_seconds()
        self.logger.debug("Exported %s in %f seconds", doc.cdr_id, elapsed)

    def export_docs(self):
        """
        Filter and store the job's documents in the file system

        There are two path for identifying documents to be exported.
        One is by manually identifying each document by its unique ID.
        The other is to run queries stored in the publishing control
        document. In theory both could be used for the same job, but
        in practice it's one or the other.
        """

        self.failed = {}
        self.manifest = []
        self.avoid_collisions()
        self.post_message("Start filtering/validating")
        self.publish_user_selected_documents()
        self.publish_query_selected_documents()
        self.write_media_manifest()
        output_dir, work_dir = self.output_dir, self.work_dir
        name = self.failure_dir if self.job_failed else output_dir
        if os.path.isdir(work_dir):
            os.rename(work_dir, output_dir)
        if not self.job_failed and not self.job.no_output:
            if self.job.parms.get("ReportOnly") != "Yes":
                failed = sum([len(f) for f in self.failed.values()])
                succeeded = len(self.processed) - failed
                if succeeded > 0:
                    self.create_push_job()

    def filter_doc(self, doc, spec):
        """
        Transform a CDR document to its exportable structure

        Pass:
          doc - reference to `Doc` object to be transformed
          spec - reference to `Job.Subsystem.Specification` object
                 which controls how we prepare the document

        Return:
          `Doc.FilterResult` object, with the following attributes:
              `result_tree` - `_XSLTResultTree` object
              `messages` - optional sequence of string messages emitted
                           by the XSL/T engine
        """

        if not spec.filters:
            raise Exception("Specification has no filters")
        root = None
        messages = []
        first_pub = doc.first_pub
        if not first_pub and doc.first_pub_knowable:
            first_pub = self.job.started
        for filter_set in spec.filters:
            parms = dict(filter_set.parameters)
            if "DateFirstPub" in parms and first_pub:
                parms["DateFirstPub"] = str(first_pub)[:10]
                parms["pubProcDate"] = str(self.job.started)[:10]
            opts = dict(parms=parms, doc=root)
            result = doc.filter(*filter_set.filters, **opts)
            if result.messages:
                messages += result.messages
            root = result.result_tree
        result.messages = messages
        return result

    def normalize(self, xml):
        """
        Prepare document for comparison

        Used to determine whether we should send a fresh copy of a
        document to the GateKeeper.

        Pass:
          xml - string for serialized version of filtered CDR document

        Return:
          version of `xml` argument with whitespace normalized
        """

        return self.NORMALIZE_SPACE.sub(" ", xml)

    def notify(self, message, with_link=False):
        """
        Tell the operator what we've done

        Pass:
          message - string for the body of the email message
          with_link - if True, include a link to the job's status page
        """

        email = self.job.parms.get("email") or "bkline@rksystems.com"
        if email and "@" in email:
            recips = email.replace(";", ",").split(",")
            args = self.tier.name, self.job.id
            subject = "[{}] CDR Publishing Job {:d}".format(*args)
            if with_link:
                cgi_base = "https://{}/cgi-bin/cdr".format(cdr.APPC)
                args = cgi_base, self.job.id
                link = "{}/PubStatus.py?id={:d}".format(*args)
                body = "{}\n\n{}".format(message, link)
            else:
                body = message
            cdr.sendMailMime(self.SENDER, recips, subject, body)
        else:
            args = self.job.id, message
            self.logger.warning("Job %d: no recips for notification %r", *args)
        if self.__opts.get("level").upper() == "DEBUG":
            print("Job {:d}: {}".format(self.job.id, message))

    def post_message(self, message):
        """
        Store a progress message in the `pub_proc` table

        Pass:
          message - string to be stored in the database
        """

        self.logger.info("Job %d: %s", self.job.id, message)
        messages = "[{}] {}".format(datetime.datetime.now(), message)
        update = "UPDATE pub_proc SET messages = ? WHERE id = ?"
        self.cursor.execute(update, (messages, self.job.id))
        self.conn.commit()

    def prep_push(self):
        """
        Find the corresponding export job and queue its docs for pushing to GK
        """

        # Output directory inappropriate for push jobs.
        update = "UPDATE pub_proc SET output_dir = '' WHERE id = ?"
        self.cursor.execute(update, (self.job.id,))
        self.conn.commit()

        # Find the export job we need to push.
        export_job = self.ExportJob(self)

        # If this is a (drastic and VERY rare) full load, clear the decks.
        pub_type = self.job.parms.get("PubType")
        if pub_type == "Full Load":
            self.cursor.execute("DELETE pub_proc_cg")
            self.conn.commit()

        # Prepare the working table, unless we're trying again with prev job.
        if not self.job.parms.get("RerunFailedPush") == "Yes":
            self.stage_push_job(export_job)

        # Some push jobs require explicit release by the operator.
        if self.job.parms.get("InteractiveMode") == "Yes":
            self.wait_for_approval()

    def publish_query_selected_documents(self):
        """
        Export documents not explicitly selected by the user for this job

        Use multi-threading for performance.
        """

        self.post_message("selecting documents")
        for spec in self.job.subsystem.specifications:
            label = spec.subdirectory
            if label:
                self.post_message("selecting {} documents".format(label))
            start = datetime.datetime.now()
            self.Thread.DOCS = spec.select_documents(self)
            ndocs = len(self.Thread.DOCS)
            what = "{} documents".format(label) if label else "documents"
            message = "{:d} {} selected".format(ndocs, what)
            self.post_message(message)
            if ndocs:
                num_threads = min(self.num_threads, ndocs)
                args = self.job.id, num_threads
                self.logger.debug("Job %d launching %d threads", *args)
                self.Thread.NEXT = 0
                threads = []
                for _ in range(num_threads):
                    session = self.session.duplicate()
                    threads.append(self.Thread(self, session, spec))
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                if label:
                    delta = (datetime.datetime.now() - start).total_seconds()
                    args = ndocs, what, delta
                    message = "{:d} {} exported in {:f} seconds".format(*args)
                    self.post_message(message)

    def publish_user_selected_documents(self):
        """
        Export documents manually selected for this job
        """

        self.logger.info("Processing user-selected documents")
        for spec in self.job.subsystem.specifications:
            for doc in self.job.docs:
                if doc.id in self.processed:
                    continue
                if doc.doctype.name not in spec.user_select_doctypes:
                    continue
                self.export_doc(doc, spec)
                self.processed.add(doc.id)
        update = "UPDATE pub_proc_doc SET {} WHERE pub_proc = ? and doc_id = ?"
        for doc in self.job.docs:
            if doc.id not in self.processed:
                args = doc.doctype.name, doc.cdr_id
                message = "{} doc {} not allowed by job".format(*args)
                self.logger.error(message)
                doc.add_error(message, Doc.LEVEL_ERROR, type="publishing")
                self.record_failure(doc)
            if doc.errors:
                if doc.id in self.failed[doc.doctype.name]:
                    columns = "messages = ?, failure = 'Y'"
                else:
                    columns = "messages = ?"
                messages = "; ".join([str(error) for error in doc.errors])
                args = messages, self.job.id, doc.id
                self.cursor.execute(update.format(columns), args)
                self.conn.commit()

    def push_docs(self):
        """
        Send the most recent export job's document to the GateKeeper
        """

        self.prep_push()
        self.send_docs()
        self.record_pushed_docs()
        self.update_status(self.VERIFYING)

    def record_failure(self, doc):
        """
        Record a document as having failed

        We add the document ID to a dictionary of failed documents for
        its document type. Then we make sure we haven't exceeded the
        threshold for the allowable number of failures for the document
        type, as well as the global threshold for errors of any document
        type.

        Pass:
          doc - reference to `Doc` object
        """

        doctype = doc.doctype.name
        if doctype not in self.failed:
            self.failed[doctype] = set()
        self.failed[doctype].add(doc.id)
        threshold = self.job.subsystem.threshold
        if threshold is not None:
            if sum([len(f) for f in self.failed.values()]) > threshold:
                message = "Error threshold ({:d}) exceeded"
                raise Exception(message.format(threshold))
        threshold = self.job.subsystem.thresholds.get(doctype)
        if threshold is not None:
            if len(self.failed[doctype]) > threshold:
                message = "{} error threshold (({:d}) exceeded"
                raise Exception(message.format(doctype, threshold))

    def record_pushed_docs(self):
        """
        Update the `pub_proc_cg` and `pub_proc_doc` tables

        Use the information stored in the `pub_proc_cg_work` table.
        All of the work in this method is wrapped in a transaction
        so that everything succeeds or nothing is updated.
        """

        # Handle removed documents
        self.cursor.execute("""\
            DELETE FROM pub_proc_cg
                  WHERE id IN (SELECT id
                                 FROM pub_proc_cg_work
                                WHERE xml IS NULL)""")
        self.cursor.execute("""\
            INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc, removed)
                 SELECT id, num, cg_job, 'Y'
                   FROM pub_proc_cg_work
                  WHERE xml IS NULL""")

        # Handle changed documents
        self.cursor.execute("""\
            UPDATE pub_proc_cg
               SET xml = w.xml,
                   pub_proc = w.cg_job,
                   force_push = 'N',
                   cg_new = 'N'
              FROM pub_proc_cg c
              JOIN pub_proc_cg_work w
                ON c.id = w.id""")
        self.cursor.execute("""\
            INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                 SELECT w.id, w.num, w.cg_job
                   FROM pub_proc_cg_work w
                   JOIN pub_proc_cg c
                     ON c.id = w.id""")

        # Handle new documents (order of INSERTs is important!)
        self.cursor.execute("""\
            INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                 SELECT w.id, w.num, w.cg_job
                   FROM pub_proc_cg_work w
        LEFT OUTER JOIN pub_proc_cg c
                     ON c.id = w.id
                  WHERE w.xml IS NOT NULL
                    AND c.id IS NULL""")
        self.cursor.execute("""\
            INSERT INTO pub_proc_cg (id, pub_proc, xml)
                 SELECT w.id, w.cg_job, w.xml
                   FROM pub_proc_cg_work w
        LEFT OUTER JOIN pub_proc_cg c
                     ON c.id = w.id
                  WHERE w.xml IS NOT NULL
                    AND c.id IS NULL""")

        # Seal the deal.
        self.conn.commit()

    def run_script(self):
        """
        Run an external script to handle this job

        Typically used for mailer jobs
        """

        script = self.job.subsystem.script
        if not os.path.isabs(script):
            script = "{}:/cdr/{}".format(self.tier.drive, script)
        if not os.path.isfile(script):
            message = "Processing script {!r} not found".format(script)
            raise Exception(message)
        command = "{} {:d}".format(script, self.job.id)
        os.system(cmd)

    def send_docs(self):
        """
        Send the documents for the push job to the GateKeeper
        """

        query = cdrdb.Query("pub_proc_cg_work", "COUNT(*) AS n")
        num_docs = query.execute(self.cursor).fetchone().n
        if not num_docs:
            self.update_status(self.SUCCESS, "Nothing to push")
            return
        opts = dict(host=self.job.parms.get("GKServer"))
        target = self.job.parms["GKPubTarget"]
        pub_type = self.job.parms["PubType"]
        if pub_type.startswith("Hotfix"):
            pub_type = "Hotfix"
        response = cdr2gk.initiateRequest(pub_type, target, **opts)
        if response.type != "OK":
            args = response.type, response.message
            raise Exception("GateKeeper: {} ({})".format(*args))
        last_push = self.last_push
        if last_push != response.details.lastJobId:
            if self.tier.name == "PROD":
                if self.job.parms.get("IgnoreGKJobIDMismatch") != "Yes":
                    raise Exception("Aborting on job ID mismatch")
            self.logger.warning("Last job ID override")
            last_push = response.details.lastJobId
        args = self.push_desc, self.job.id, pub_type, target, last_push
        response = cdr2gk.sendDataProlog(*args, **opts)
        if response.type != "OK":
            args = response.type, response.message
            raise Exception("GateKeeper: {} ({})".format(*args))
        query = cdrdb.Query("pub_proc_cg_work", "id", "num", "doc_type", "xml")
        query.where("xml IS NOT NULL")
        query.where(query.Condition("doc_type", self.EXCLUDED, "NOT IN"))
        query.execute(self.cursor)
        group_nums = GroupNums(self.job.id)
        row = self.cursor.fetchone()
        counter = 1
        while row:
            doc_type = self.GK_TYPES.get(row.doc_type, row.doc_type)
            xml = self.XMLDECL.sub("", self.DOCTYPE.sub("", row.xml))
            group_num = group_nums.getDocGroupNum(row.id)
            args = (self.job.id, counter, "Export", doc_type, row.id,
                    row.num, group_num, xml.encode("utf-8"))
            response = cdr2gk.sendDocument(*args, **opts)
            if response.type != "OK":
                args = response.type, response.message
                raise Exception("GateKeeper: {} ({})".format(*args))
            counter += 1
            row = self.cursor.fetchone()
        query = cdrdb.Query("pub_proc_cg_work", "id", "num", "doc_type")
        query.where("xml IS NULL")
        query.where(query.Condition("doc_type", self.EXCLUDED, "NOT IN"))
        rows = query.execute(self.cursor).fetchall()
        for row in rows:
            doc_type = self.GK_TYPES.get(row.doc_type, row.doc_type)
            args = (self.job.id, counter, "Remove", doc_type, row.id,
                    row.num, group_nums.genNewUniqueNum())
            response = cdr2gk.sendDocument(*args, **opts)
            if response.type != "OK":
                args = response.type, response.message
                raise Exception("GateKeeper: {} ({})".format(*args))
        args = self.job.id, pub_type, num_docs, "complete"
        response = cdr2gk.sendJobComplete(*args, **opts)
        if response.type != "OK":
            args = response.type, response.message
            raise Exception("GateKeeper: {} ({})".format(*args))

    def stage_push_job(self, export_job):
        """
        Populate the `pub_proc_cg_work` table with documents to be pushed

        Pass:
          export_job - reference to `Control.ExportJob` object
        """

        self.cursor.execute("DELETE {}".format(self.PUSH_STAGE))
        self.conn.commit()
        push_id = str(self.job.id)

        # For 'Hotfix (Remove)' jobs all docs in pub_proc_doc are removals.
        if self.job.parms["PubType"] == "Hotfix (Remove)":
            cols = "d.id", "p.doc_version", "p.pub_proc", push_id, "t.name"
            query = cdrdb.Query("pub_proc_doc p", *cols)
            query.join("document d", "d.id = p.doc_id")
            query.join("doc_type t", "t.id = d.doc_type")
            query.where("p.pub_proc = {:d}".format(export_job.job_id))
            cols = "id", "num", "vendor_job", "cg_job", "doc_type"
            args = self.PUSH_STAGE, ", ".join(cols), query
            insert = "INSERT INTO {} ({}) {}".format(*args)
            self.cursor.execute(insert)
            self.conn.commit()
            return

        # Fetch the documents which need to be modified.
        push_all = self.job.parms.get("PushAllDocs") == "Yes"
        doc_type = "t.name AS doc_type"
        cols = "c.id", doc_type, "d.subdir", "d.doc_version", "c.force_push"
        query = cdrdb.Query("pub_proc_cg c", *cols)
        query.join("pub_proc_doc d", "d.doc_id = c.id")
        query.join("doc_version v", "v.id = c.id", "v.num = d.doc_version")
        query.join("doc_type t", "t.id = v.doc_type")
        query.where(query.Condition("d.pub_proc", export_job.job_id))
        query.where(query.Condition("t.name", self.EXCLUDED, "NOT IN"))
        query.where("d.failure IS NULL")
        rows = query.execute(self.cursor).fetchall()
        fields = dict(
            vendor_job=export_job.job_id,
            cg_job=self.job.id,
            id=None,
            doc_type=None,
            xml=None,
            num=None
        )
        names = sorted(fields)
        placeholders = ", ".join(["?"] * len(names))
        args = self.PUSH_STAGE, ", ".join(names), placeholders
        for row in rows:
            if row.id in self.processed:
                continue
            self.processed.add(row.id)
            directory = export_job.directory
            subdir = (row.subdir or "").strip()
            if subdir:
                directory = "{}/{}".format(export_job.directory, subdir)
            if row.doc_type == "Media":
                exported = self.wrap_media_file(directory, row.id)
            else:
                path = "{}/CDR{:d}.xml".format(directory, row.id)
                with open(path, "rb") as fp:
                    exported = fp.read().decode("utf-8")
            needs_push = push_all or row.force_push == "Y"
            if not needs_push:
                query = cdrdb.Query("pub_proc_cg", "xml")
                query.where(query.Condition("id", row.id))
                pushed = query.execute(self.cursor).fetchone().xml
                if self.normalize(pushed) != self.normalize(exported):
                    needs_push = True
            if needs_push:
                fields["id"] = row.id
                fields["doc_type"] = row.doc_type
                fields["xml"] = exported
                fields["num"] = row.doc_version
                values = [fields[name] for name in names]
                insert = "INSERT INTO {} ({}) VALUES ({})".format(*args)
                self.logger.info("Queueing mod doc CDR%d for push", row.id)
                try:
                    self.cursor.execute(insert, values)
                except:
                    self.logger.exception("First insert failed; trying again")
                    time.sleep(self.FAILURE_SLEEP)
                    self.cursor.execute(insert, values)
                self.conn.commit()

        # Queue up documents which the GateKeeper doesn't already have.
        cols = "v.id", doc_type, "d.subdir", "d.doc_version"
        query = cdrdb.Query("pub_proc_doc d", *cols)
        query.join("doc_version v", "v.id = d.doc_id", "v.num = d.doc_version")
        query.join("doc_type t", "t.id = v.doc_type")
        query.outer("pub_proc_cg c", "c.id = v.id")
        query.where("d.pub_proc = {:d}".format(export_job.job_id))
        query.where("d.failure IS NULL")
        query.where("c.id IS NULL")
        rows = query.execute(self.cursor).fetchall()
        for row in rows:
            if row.id in self.processed:
                continue
            self.processed.add(row.id)
            directory = export_job.directory
            subdir = (row.subdir or "").strip()
            if subdir:
                directory = "{}/{}".format(export_job.directory, subdir)
            if row.doc_type == "Media":
                exported = self.wrap_media_file(directory, row.id)
            else:
                path = "{}/CDR{:d}.xml".format(directory, row.id)
                with open(path, "rb") as fp:
                    exported = fp.read().decode("utf-8")
            fields["id"] = row.id
            fields["doc_type"] = row.doc_type
            fields["xml"] = exported
            fields["num"] = row.doc_version
            values = [fields[name] for name in names]
            insert = "INSERT INTO {} ({}) VALUES ({})".format(*args)
            self.logger.info("Queueing new doc CDR%d for push", row.id)
            try:
                self.cursor.execute(insert, values)
            except:
                self.logger.exception("First insert failed; trying again")
                time.sleep(self.FAILURE_SLEEP)
                self.cursor.execute(insert, values)
            self.conn.commit()

        # Don't prune documents not included in a hotfix job.
        if self.job.parms["PubType"].startswith("Hotfix"):
            return

        # Don't prune documents if the number of documents was restricted.
        if self.job.parms.get("NumDocs"):
            return

        # Handle documents which have been dropped for doctypes published
        # XXX TODO WHAT HAPPENS IF NUMDOCS IS OVERRIDDEN???????????
        query = cdrdb.Query("pub_proc_doc d", "v.doc_type").unique()
        query.join("doc_version v", "v.id = d.doc_id", "v.num = d.doc_version")
        query.where(query.Condition("d.pub_proc", export_job.job_id))
        types = [row.doc_type for row in query.execute(self.cursor).fetchall()]
        if not types:
            return
        types = ", ".join([str(t) for t in types])
        export_id = str(export_job.job_id)
        cols = "v.id", "v.num", export_id, push_id, "t.name"
        query = cdrdb.Query("pub_proc_doc d", *cols).unique()
        query.join("doc_version v", "v.id = d.doc_id", "v.num = d.doc_version")
        query.join("all_docs a", "a.id = v.id")
        query.join("pub_proc_cg c", "c.id = v.id", "c.pub_proc = d.pub_proc")
        query.join("doc_type t", "t.id = v.doc_type")
        query.outer("pub_proc_cg_work w", "w.id = c.id")
        query.where("w.id IS NULL")
        query.where("a.active_status <> 'A'")
        query.where("t.name <> 'Media'")
        query.where("v.doc_type IN ({})".format(types))
        cols = "id", "num", "vendor_job", "cg_job", "doc_type"
        args = self.PUSH_STAGE, ", ".join(cols), query
        insert = "INSERT INTO {} ({})\n{}".format(*args)
        self.logger.info("Queueing dropped documents")
        try:
            self.cursor.execute(insert)
        except:
            self.logger.exception("First insert failed; trying again")
            time.sleep(self.FAILURE_SLEEP)
            self.cursor.execute(insert)
        self.conn.commit()

    def update_status(self, status, message=None):
        """
        Set the job's current status in the database

        Also record message if one is passed.

        Pass:
          status - string for value to store in the `pub_proc.status` column
          message - optional string for message to store in the DB
        """

        date = "GETDATE()" if status in self.COMPLETED_STATUSES else "NULL"
        update = """\
            UPDATE pub_proc
               SET status = ?,
                   completed = {}
             WHERE id = ?
               AND status != 'Success'""".format(date)
        self.cursor.execute(update, (status, self.job.id))
        self.conn.commit
        if message:
            self.post_message(message)
        else:
            self.logger.info("Job %d: set status to %s", self.job.id, status)

    def wait_for_approval(self):
        """
        Allow the operator to review the queued push job before releasing it
        """

        self.update_status(self.WAIT, "Waiting for push job release")
        query = cdrdb.Query("pub_proc", "status")
        query.where(query.Condition("id", self.job.id))
        body = "Push job {:d} is waiting for approval.".format(self.job.id)
        self.notify(body)
        while True:
            status = query.execute(self.cursor).fetchone().status
            if status == self.RUN:
                self.post_message("Job resumed by user")
                break
            if status == self.FAILURE:
                raise Exception("Job killed by user")
            if status == self.WAIT:
                time.sleep(10)
            else:
                message = "Unexpected status {} for job {}"
                raise Exception(message.format(status, self.job.id))

    def wrap_media_file(self, directory, doc_id):
        """
        Wrap the Media document's encoded blob in an XML document

        Pass:
          directory - string for location path for stored binary file
          doc_id - integer for the CDR document's unique ID

        Return:
          serialized Media XML document
        """

        paths = glob.glob("{}/CDR{:010d}.*".format(directory, doc_id))
        if not paths:
            raise Exception("Media file for CDR{} not found".format(doc_id))
        path = paths[0].replace("\\", "/")
        base, extension = os.path.splitext(path)
        extension = extension.replace(".", "")
        if extension not in self.MEDIA_TYPES:
            raise Exception("Media type not supported for {}".format(path))
        media_type = self.MEDIA_TYPES[extension]
        with open(path, "rb") as fp:
            media_bytes = fp.read()
        encoded = base64encode(media_bytes)
        template = "<Media Type='{}' Size='{:d}' Encoding='base64'>{}</Media>"
        template += "<!-- {} -->".format(datetime.datetime.now())
        return template.format(media_type, len(media_bytes), encoded)

    def write_doc(self, doc_bytes, directory, filename):
        """
        Store an exported CDR document in the file system

        Pass:
          doc_bytes - document representation to be stored
          directory - path to location of file
          filename - string for name of file to be created
        """

        if self.job.no_output:
            return
        if not os.path.isdir(directory):
            with self.DIRECTORY_LOCK:
                if not os.path.isdir(directory) and not self.job_failed:
                    os.makedirs(directory)
        if not self.job_failed:
            with open(os.path.join(directory, filename), "wb") as fp:
                fp.write(doc_bytes)

    def write_media_manifest(self):
        """
        Store information about each Media document exported by this job

        The information is stored using comma-separated value format
        """

        if self.manifest and os.path.isdir(self.work_dir):
            path = os.path.join(self.work_dir, "media_catalog.txt")
            with open(path, "w") as fp:
                writer = csv.writer(fp)
                writer.writerows(sorted(self.manifest))

    # ------------------------------------------------------------------
    # PROPERTIES START HERE.
    # ------------------------------------------------------------------

    @property
    def conn(self):
        """
        Connection to the CDR database
        """

        if not hasattr(self, "_conn"):
            opts = dict(user="CdrPublishing")
            try:
                self._conn = cdrdb.connect(**opts)
            except Exception as e:
                self.logger.exception("unable to connect to database")
                raise Exception("Database connection failure: {}".format(e))
        return self._conn

    @property
    def cursor(self):
        """
        Database `Cursor` object
        """

        if not hasattr(self, "_cursor"):
            self._cursor = self.conn.cursor()
        return self._cursor

    @property
    def failure_dir(self):
        """
        String for path to which output directory is renamed on failure
        """

        output_dir = self.output_dir
        if output_dir:
            return output_dir + ".FAILURE"
        return None

    @property
    def job(self):
        """
        Reference to `publishing.Job` object for this run
        """

        if not hasattr(self, "_job"):
            self._job = Job(self.session, id=self.__job_id)
        return self._job

    @property
    def job_failed(self):
        """
        Flag indicating whether the current job has failed
        """

        with Control.FAILURE_LOCK:
            if not hasattr(self, "_failed"):
                self._failed = False
            return self._failed

    @job_failed.setter
    def job_failed(self, value):
        """
        Mark the current job as having failed
        """

        with Control.FAILURE_LOCK:
            self._failed = value

    @property
    def last_push(self):
        """
        Primary key for the most recent successful push job

        Used to sychronize with the GateKeeper.
        """

        if not hasattr(self, "_last_push"):
            push = "{}%".format(self.PUSH)
            query = cdrdb.Query("pub_proc p", "MAX(p.id) AS id")
            query.join("pub_proc_doc d", "d.pub_proc = p.id")
            query.where(query.Condition("p.status", self.SUCCESS))
            query.where(query.Condition("p.pub_subset", push, "LIKE"))
            query.where(query.Condition("p.pub_system", self.job.system.id))
            self._last_push = query.execute(self.cursor).fetchone().id
        return self._last_push

    @property
    def logger(self):
        """
        Standard library `Logger` object
        """

        if not hasattr(self, "_logger"):
            opts = dict(level=self.__opts.get("level") or "INFO")
            self._logger = self.tier.get_logger("cdrpub", **opts)
        return self._logger

    @property
    def num_threads(self):
        """
        Integer for the number of threads used for exporting documents
        """

        if not hasattr(self, "_num_threads"):
            self._num_threads = self.__opts.get("threads")
            if not self._num_threads:
                query = cdrdb.Query("ctl", "val")
                query.where("grp = 'Publishing'")
                query.where("name = 'ThreadCount'")
                query.where("inactivated IS NULL")
                row = query.execute(self.cursor).fetchone()
                self._num_threads = int(row.val) if row else 4
        return self._num_threads

    @property
    def output_dir(self):
        """
        Final path name for exported documents' location
        """

        return self.__opts.get("output-dir") or self.job.output_dir

    @property
    def push_desc(self):
        """
        Description sent to the GateKeeper for the push job
        """

        return self.job.parms.get("GKPushJobDescription")

    @property
    def session(self):
        """
        `Session` object representing a CDR login
        """

        if not hasattr(self, "_session"):
            query = cdrdb.Query("usr u", "u.name")
            query.join("pub_proc p", "p.usr = u.id")
            query.where(query.Condition("p.id", self.__job_id))
            row = query.execute(self.cursor).fetchone()
            if not row:
                raise Exception("Job {} not found".format(self.__job_id))
            user = row.name
            password = self.tier.passwords.get(user.lower()) or ""
            self._session = Session.create_session(user, password=password)
        return self._session

    @property
    def status(self):
        """
        Current status of the publishing job
        """

        query = cdrdb.Query("pub_proc", "status")
        query.where(query.Condition("id", self.job.id))
        status = query.execute(self.cursor).fetchone().status
        return status

    @property
    def tier(self):
        """
        Identification of which CDR server is running the publishing job
        """

        if not hasattr(self, "_tier"):
            self._tier = Tier()
        return self._tier

    @property
    def work_dir(self):
        """
        Temporary name for job output while we are exporting
        """

        output_dir = self.output_dir
        if output_dir:
            return output_dir + ".InProcess"
        return None

    @classmethod
    def validate_doc(cls, doc, dtd_path):
        """
        Validate a filtered CDR document against its DTD

        Pass:
          doc - top-level node for filtered document
          dtd_path - location of DTD file

        Return:
          result of validation operation
        """

        with open(dtd_path) as fp:
            dtd = etree.DTD(fp)
        if isinstance(doc, basestring):
            if isinstance(doc, unicode):
                doc = doc.encode("utf-8")
            doc = etree.fromstring(doc)
        dtd.validate(doc)
        return dtd.error_log.filter_from_errors()


    # ------------------------------------------------------------------
    # NESTED CLASSES START HERE.
    # ------------------------------------------------------------------


    class Thread(threading.Thread):
        """
        Object for exporting jobs in parallel
        """

        QUEUE_LOCK = threading.Lock()
        DOCS_LOCK = threading.Lock()
        DOCS = []
        NEXT = 0
        TABLE = "pub_proc_doc"
        FLDS = "pub_proc, doc_id, doc_version, subdir"
        INSERT = "INSERT INTO {} ({}) VALUES (?, ?, ?, ?)".format(TABLE, FLDS)
        WHERE = "pub_proc = ? AND doc_id = ?"
        SET = "messages = ?, failure = 'Y'"
        UPDATE = "UPDATE {} SET {} WHERE {}".format(TABLE, SET, WHERE)

        def __init__(self, control, session, spec):
            """
            Capture the passed arguments and ensure thread safety

            We don't use things like thresholds or output_dir directly
            from this object, but we save them as attributes during the
            constructor's operation (which is single-threaded) so that
            those properties will have been calculated and cached once
            we get into the hairier threaded code.

            Pass:
              control - reference to `Control` object running this job
              session - reference to distinct `Session` object for thread
              spec - reference to `Job.Subsystem.Specification` object
            """

            threading.Thread.__init__(self)
            self.control = control
            self.session = session
            self.spec = spec
            self.job_start = str(control.job.started)[:19]
            self.output_dir = control.output_dir
            self.subdir = spec.subdirectory or ""
            self.filters = [f.filters for f in spec.filters]
            self.parms = [f.parameters for f in spec.filters]
            self.threshold = self.control.job.subsystem.threshold
            self.thresholds = self.control.job.subsystem.thresholds

        def run(self):
            """
            Keep publishing documents from the queue until it's exhausted
            """

            while True:
                with Control.Thread.QUEUE_LOCK:
                    if Control.Thread.NEXT >= len(Control.Thread.DOCS):
                        break
                    d = Control.Thread.DOCS[Control.Thread.NEXT]
                    Control.Thread.NEXT += 1
                opts = dict(id=d.id, version=d.version)
                if d.version == "lastp":
                    opts["before"] = self.job_started
                doc = Doc(self.session, **opts)
                values = self.control.job.id, doc.id, doc.version, self.subdir
                self.session.cursor.execute(self.INSERT, values)
                self.session.conn.commit()
                try:
                    self.control.export_doc(doc, self.spec)
                except Exception as e:
                    messages = "; ".join([e.message for e in doc.errors])
                    values = messages or str(e), self.control.job.id, doc.id
                    self.session.cursor.execute(self.UPDATE, values)
                    self.session.conn.commit()
                    raise
                failed_docs = self.control.failed.get(doc.doctype.name)
                if failed_docs and doc.id in failed_docs:
                    messages = "; ".join([e.message for e in doc.errors])
                    messages = messages or "Export failed"
                    values = messages, self.control.job.id, doc.id
                    self.session.cursor.execute(self.UPDATE, values)
                    self.session.conn.commit()

                # Desperate (but futile?) attempt to free memory.
                doc = messages = values = failed_docs = opts = d = None
                gc.collect()

    class ExportJob:
        """
        Export job whose needs to be pushed.

        Attributes:
          job_id - integer primary key into the `pub_proc` table
          directory - base path where the documents were written
          doc_count - number of documents exported
        """

        def __init__(self, control):
            """
            Find the properties of the most recent successful export job

            Pass:
              control - reference to `Control` object running this job
            """

            cursor = control.cursor
            subset_name = control.job.parms["SubSetName"]
            query = cdrdb.Query("pub_proc", "id", "output_dir").limit(1)
            query.where(query.Condition("pub_system", control.job.system.id))
            query.where(query.Condition("pub_subset", subset_name))
            query.where(query.Condition("status", control.SUCCESS))
            row = query.order("id DESC").execute(cursor).fetchone()
            if not row:
                raise Exception("{} job not found".format(subset_name))
            self.job_id, directory = row
            self.directory = directory.strip().rstrip("/")
            push_job = self.__push_job(control, cursor, subset_name)
            if push_job is not None and push_job > self.job_id:
                message = "Export job {} has already been push by job {}"
                raise Exception(message.format(self.job_id, push_job))
            query = cdrdb.Query("pub_proc_doc", "COUNT(*) AS n")
            query.where(query.Condition("pub_proc", self.job_id))
            self.doc_count = query.execute(cursor).fetchone().n
            assert self.doc_count, "No documents to push"

        def __push_job(self, control, cursor, subset_name):
            """
            Find out if this job has already been successfully pushed to CG

            We have to use the LIKE operator because of a bug in SQL Server,
            which is unable to use the = operator with NTEXT values.

            Pass:
              control - reference to `Control` object running this job
              cursor - database query object
              subset_name - string for the export job's type
            """

            query = cdrdb.Query("pub_proc j", "MAX(j.id) AS job_id")
            push_name = "{}_{}".format(control.PUSH, subset_name)
            query.join("pub_proc_parm p", "p.pub_proc = j.id")
            query.where(query.Condition("j.id", self.job_id, ">"))
            query.where(query.Condition("j.pub_system", control.job.system.id))
            query.where(query.Condition("j.pub_subset", push_name))
            query.where(query.Condition("j.status", control.SUCCESS))
            query.where("p.parm_name = 'SubSetName'")
            query.where(query.Condition("p.parm_value", subset_name, "LIKE"))
            row = query.execute(cursor).fetchone()
            return row.job_id if row else None


def main():
    """
    Test driver
    """

    opts = dict(level="INFO")
    parser = argparse.ArgumentParser()
    parser.add_argument("job_id", metavar="job-id")
    parser.add_argument("--debug", "-d", action="store_true")
    parser.add_argument("--threads", "-t", type=int)
    parser.add_argument("--output", "-o")
    args = parser.parse_args()
    if args.debug:
        cdr2gk.DEBUGLEVEL = 1
        opts["level"] = "DEBUG"
    if args.threads:
        opts["threads"] = args.threads
    if args.output:
        opts["output-dir"] = args.output
    Control(args.job_id, **opts).publish()

if __name__ == "__main__":
    """
    Let this be loaded as a module without doing anything

    """
    main()
