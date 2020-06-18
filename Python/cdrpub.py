"""
Process a queued publishing job

The top-level entry point for this module is `Control.publish()`, which
calls `Control.__publish()`, wrapped in a try block to facilitate handling
of all failures in a central place. The latter method decides which of the
three basic publishing job types is being run (scripted, export, or push)
and handles the job appropriately. For scripted jobs the work is simply
handed off to the specified script by launching a separate process. For
an overview of the logic for the other two job types, see the methods
`Control.export_docs()` and `Control.push_docs()`.
"""

import argparse
import base64
import csv
import datetime
import glob
import hashlib
import io
import json
import os
import re
import subprocess
import shutil
import threading
import time
from lxml import etree, html
from PIL import Image
import cdr
import cdr2gk
from cdrapi import db
from cdrapi.docs import Doc
from cdrapi.publishing import Job, DrupalClient
from cdrapi.settings import Tier
from cdrapi.users import Session
from AssignGroupNums import GroupNums


class Control:
    """
    Top-level object for CDR publishing job processing
    """

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
    PUB = "Publishing"
    DEFAULT_BATCHSIZE = cdr.getControlValue(PUB, "batchsize", default=25)
    DEFAULT_NUMPROCS = cdr.getControlValue(PUB, "numprocs", default=8)
    GK_TYPES = dict(
        GlossaryTermName="GlossaryTerm",
        Person="GeneticsProfessional"
    )
    MEDIA_TYPES = dict(
        jpg="image/jpeg",
        gif="image/gif",
        mp3="audio/mpeg"
    )
    SHORT_TITLE_MAX = 100
    DESCRIPTION_MAX = 600

    def __init__(self, job_id, **opts):
        """
        Stash away what we'll need for running a publishing job
        """

        self.__job_id = job_id
        self.__opts = opts
        self.__gk_prolog_sent = False

    # ------------------------------------------------------------------
    # TOP-LEVEL PROCESSING METHODS.
    # ------------------------------------------------------------------

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
            if self.__gk_prolog_sent:
                args = self.job.id, "Export", 0, "abort"
                opts = dict(host=self.job.parms.get("GKServer"))
                response = cdr2gk.sendJobComplete(*args, **opts)
                if response.type != "OK":
                    args = response.type, response.message
                    self.logger.warning("GK abort response: %s (%s)", *args)
            self.notify("Job failed: {}".format(e))

    def __publish(self):
        """
        Run the publishing job

        The heavy lifting is extracted out to this private method so we
        can trap all exceptions.

        There are basically three kinds of publishing jobs:
          1. scripted (the script embodies all of the logic for the job)
          2. export (write filtered documents to the file syatem)
          3. push (push results of an export job to cancer.gov's GateKeeper)

        Push jobs have a `SubSetName` parameter to identify the type of
        the corresponding export job. We use the presence of this parameter
        to recognize push jobs.
        """

        # Announce ourselves.
        self.update_status(self.RUN, "Job started")

        # 1. External script jobs completely handed off here.
        if self.job.subsystem.script:
            script = self.job.subsystem.script
            if not os.path.isabs(script):
                script = "{}:/cdr/{}".format(self.tier.drive, script)
            if not os.path.isfile(script):
                message = "Processing script {!r} not found".format(script)
                raise Exception(message)
            command = "{} {:d}".format(script, self.job.id)
            if script.endswith(".py") and "python" not in command.lower():
                command = " ".join([cdr.PYTHON, command])
            self.logger.info("Launching %s", command)
            os.system(command)

        # The job is an export job or a push job.
        else:
            start = datetime.datetime.now()
            self.logger.debug("Job %d parms=%s", self.job.id, self.job.parms)
            args = self.job.id, self.job.subsystem.options
            self.logger.debug("Job %d opts=%s", *args)
            self.processed = set()

            # 2. Export jobs don't have a `SubSetName` parameter.
            if "SubSetName" not in self.job.parms:
                self.export_docs()
                verb = "Exported"
                count = len(self.processed)

            # 3. Otherwise, this is a push job.
            else:
                self.push_docs()
                verb = "Pushed"
                count = self.__num_pushed

            # Report the job's completion.
            elapsed = (datetime.datetime.now() - start).total_seconds()
            args = verb, count, elapsed
            message = "{} {:d} documents in {:.2f} seconds".format(*args)
            if self.job.parms.get("ReportOnly") == "Yes":
                message += " (status set to failure for 'ReportOnly' job)"
                self.update_status(self.FAILURE, message)
            elif self.status != self.VERIFYING:
                self.update_status(self.SUCCESS, message)
            else:
                self.post_message(message)
            self.notify(message, with_link=True)

            # Record documents published for the first time
            self.record_first_pub()

    # ------------------------------------------------------------------
    # METHODS FOR EXPORT JOBS START HERE.
    # ------------------------------------------------------------------

    def export_docs(self):
        """
        Filter and store the job's documents in the file system

        There are two path for identifying documents to be exported.
        One is by manually identifying each document by its unique ID.
        The other is to run queries stored in the publishing control
        document. In theory both could be used for the same job, but
        in practice it's one or the other.

        After exporting the documents, we create the corresponding
        push job (assuming at least one document was successfully
        exported, and we haven't been told to skip the push job).

        Processing steps:
          0. Housekeeping preparation
          1. Export any manually selected documents
          2. Export any query-selected documents
          3. Write the media manifest if appropriate
          4. Check error thresholds
          5. Rename the output directory
          6. Create push job if appropriate
        """

        # 0. Housekeeping preparation
        self.post_message("Start filtering/validating")
        self.prep_export()

        # 1. Export any manually selected documents
        self.publish_user_selected_documents()

        # 2. Export any query-selected document
        self.publish_query_selected_documents()

        # 3. Make sure we haven't blown any error threshold limits.
        self.check_error_thresholds()

        # 4. Write the media manifest if appropriate
        self.write_media_manifest()

        # 5. Rename the output directory
        if os.path.isdir(self.work_dir):
            os.rename(self.work_dir, self.output_dir)

        # 6. Create the push job if appropriate
        if not self.job.no_output:
            if self.job.parms.get("ReportOnly") != "Yes":
                self.create_push_job()

    def record_first_pub(self):
        """
        Populate the `document.first_pub` column where appropriate

        Avoid populating the column for documents which pre-date
        the CDR, because we have no way of knowing when those
        were first published, as the legacy Oracle system did not
        capture that information.
        """

        self.cursor.execute("""\
            UPDATE document
               SET document.first_pub = pub_proc.started
              FROM pub_proc
              JOIN pub_proc_doc
                ON pub_proc_doc.pub_proc = pub_proc.id
              JOIN document
                ON document.id = pub_proc_doc.doc_id
             WHERE pub_proc.id = ?
               AND pub_proc.status = ?
               AND pub_proc_doc.removed != 'Y'
               AND pub_proc_doc.failure IS NULL
               AND document.first_pub IS NULL
               AND document.first_pub_knowable = 'Y'""",
                            (self.job.id, self.SUCCESS))
        count = self.cursor.rowcount
        if count:
            self.logger.info("Set first_pub for %d document(s)", count)
            self.conn.commit()

    def prep_export(self):
        """
        Clean up from any previous jobs with this name/ID

        If we're running on a lower tier which has been refreshed from
        the production database, it's possible that the current job
        number was already used. If so, move any directories left over
        from such jobs out of the way. Let exceptions bubble up.
        Similarly, we need to clear out the `export_spec` table
        in case a previous attempt to run this job left rows in it.
        Finally, create a way to remember which rows we have already
        added to this table in this run, and a lock for controlling
        access to things that can be changed by multiple threads.
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
        delete = "DELETE FROM export_spec WHERE job_id = ?"
        self.cursor.execute(delete, (self.job.id,))
        self.conn.commit()
        self.spec_ids = set()
        self.export_failed = False
        self.lock = threading.Lock()

    def publish_user_selected_documents(self):
        """
        Export documents manually selected for this job
        """

        self.logger.info("Processing user-selected documents")
        for i, spec in enumerate(self.job.subsystem.specifications):
            self.spec_id = i + 1
            self.docs = []
            for doc in self.job.docs:
                if doc.id in self.processed:
                    continue
                if spec.user_select_doctypes:
                    if doc.doctype.name not in spec.user_select_doctypes:
                        continue
                self.docs.append("{}/{}".format(doc.id, doc.version))
                self.processed.add(doc.id)
            if self.docs:
                self.launch_exporters(spec)

        # Mark any documents left behind as failed.
        for doc in self.job.docs:
            if doc.id not in self.processed:
                args = doc.doctype.name, doc.cdr_id
                message = "{} doc {} not allowed by this job".format(*args)
                self.logger.error(message)
                self.cursor.execute("""\
                    UPDATE pub_proc_doc
                       SET failure = 'Y', messages = ?
                     WHERE pub_proc = ?
                       AND doc_id = ?""", (message, self.job.id, doc.id))
                self.conn.commit()

    def publish_query_selected_documents(self):
        """
        Export documents not explicitly selected by the user for this job

        Use multi-processing for performance.
        """

        self.post_message("selecting documents")
        for i, spec in enumerate(self.job.subsystem.specifications):
            if spec.query is not None:
                self.spec_id = i + 1
                self.post_message("selecting {} documents".format(spec.name))
                start = datetime.datetime.now()
                docs = spec.select_documents(self)
                elapsed = (datetime.datetime.now() - start).total_seconds()
                name = "{} ".format(spec.name) if spec.name else ""
                args = len(docs), name, elapsed
                msg = "{:d} {}docs selected in {:.2f} seconds".format(*args)
                self.post_message(msg)
                self.docs = []
                for doc in docs:
                    if doc.id not in self.processed:
                        self.docs.append("{:d}/{}".format(doc.id, doc.version))
                        self.processed.add(doc.id)
                if self.docs:
                    self.launch_exporters(spec)

    def launch_exporters(self, spec):
        """
        Pass off the export work to separate processes

        Pass:
          spec - reference to `Job.Subsystem.Specification` object
                 which controls how we prepare the document
        """

        # Communicate spec settings to processes via database tables
        if self.spec_id not in self.spec_ids:
            filters = [(f.filters, f.parameters) for f in spec.filters]
            values = [self.job.id, self.spec_id, repr(filters)]
            cols = ["job_id", "spec_id", "filters"]
            if spec.subdirectory:
                values.append(spec.subdirectory)
                cols.append("subdir")
            args = ", ".join(cols), ", ".join(["?"] * len(cols))
            insert = "INSERT INTO export_spec ({}) VALUES ({})".format(*args)
            self.cursor.execute(insert, tuple(values))
            self.conn.commit()
            self.spec_ids.add(self.spec_id)

        # Determine how many processes to launch and batch size for each
        self.next = 0
        name = "{}-batchsize".format(spec.name)
        default = self.DEFAULT_BATCHSIZE
        batchsize = cdr.getControlValue(self.PUB, name, default=default)
        self.batchsize = int(batchsize)
        name = "{}-numprocs".format(spec.name)
        default = self.__opts.get("numprocs") or self.DEFAULT_NUMPROCS
        numprocs = cdr.getControlValue(self.PUB, name, default=default)
        numprocs = min(int(numprocs), len(self.docs))
        if self.batchsize * numprocs > len(self.docs):
            self.batchsize = len(self.docs) // numprocs
        if "batchsize" in self.__opts:
            self.batchsize = self.__opts["batchsize"]
        if "numprocs" in self.__opts:
            numprocs = self.__opts["numprocs"]

        # Create a separate thread to manage each external process
        self.logger.info("Using %d parallel processes", numprocs)
        threads = []
        start = datetime.datetime.now()
        for _ in range(numprocs):
            threads.append(self.Thread(self))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = (datetime.datetime.now() - start).total_seconds()
        args = len(self.docs), spec.name, elapsed
        self.logger.info("exported %d %s docs in %.2f seconds", *args)

    def check_error_thresholds(self):
        """
        Make sure we haven't exceeded error thresholds
        """

        if self.export_failed:
            raise Exception("Export multiprocessing failure")
        query = db.Query("pub_proc_doc d", "t.name", "COUNT(*) AS errors")
        query.join("doc_version v", "v.id = d.doc_id", "v.num = d.doc_version")
        query.join("doc_type t", "t.id = v.doc_type")
        query.where("d.failure = 'Y'")
        query.where(query.Condition("d.pub_proc", self.job.id))
        query.group("t.name")
        rows = query.execute(self.cursor).fetchall()
        total_errors = 0
        errors = dict([tuple(row) for row in rows])
        for doctype in errors:
            total_errors += errors[doctype]
            name = "Max{}Errors".format(doctype)
            threshold = self.job.parms.get(name)
            if threshold is not None and int(threshold) < errors[doctype]:
                args = threshold, doctype, errors[doctype]
                message = "{} {} errors allowed; {:d} found".format(*args)
                raise Exception(message)
        threshold = self.job.subsystem.threshold
        if threshold is not None and threshold < total_errors:
            args = threshold, total_errors
            message = "{:d} total errors allowed; {:d} found".format(*args)
            raise Exception(message)
        if len(self.processed) - total_errors < 1:
            raise Exception("All documents failed export")

    def write_media_manifest(self):
        """
        Store information about each Media document exported by this job

        The information is stored using comma-separated value format
        """

        query = db.Query("media_manifest", "filename", "blob_date", "title")
        query.where(query.Condition("job_id", self.job.id))
        query.order("doc_id")
        rows = query.execute(self.cursor).fetchall()
        if rows and os.path.isdir(self.work_dir):
            values = [(row[0], str(row[1])[:10], row[2]) for row in rows]
            path = os.path.join(self.work_dir, "media_catalog.txt")
            with open(path, "w", newline="", encoding="utf-8") as fp:
                opts = dict(delimiter=",", quotechar='"')
                writer = csv.writer(fp, **opts)
                writer.writerows(values)

    def create_push_job(self):
        """
        Queue up a job to push the documents we just exported
        """

        # First make sure there's something to push.
        query = db.Query("pub_proc_doc", "COUNT(*) AS exported")
        query.where(query.Condition("pub_proc", self.job.id))
        query.where("failure IS NULL")
        if query.execute(self.cursor).fetchone().exported:
            default_desc = "{} push job".format(self.job.subsystem.name)
            desc = self.job.parms.get("GKPushJobDescription") or default_desc
            parms = dict(
                DrupalServer=self.job.parms.get("DrupalServer"),
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
            self.logger.info("Job %d created push job %d", self.job.id, job.id)
        else:
            self.logger.warning("Job %d has nothing to push", self.job.id)

    # ------------------------------------------------------------------
    # METHODS FOR PUSH JOBS START HERE.
    # ------------------------------------------------------------------

    def push_docs(self):
        """
        Send the most recent export job's document to the GateKeeper

        Processing steps:
          1. Find the matching export job and queue its docs for pushing
          2. Send the queued documents to the cancer.gov GateKeeper
          3. Update the `pub_proc_cg` and `pub_proc_doc` tables
          4. Record the job's status as waiting for GK to confirm the push
        """

        # 1. Find the matching export job and queue its docs for pushing
        self.prep_push()

        # 2. Send the queued documents to the cancer.gov GateKeeper
        self.send_docs()

        # 3. Update the `pub_proc_cg` and `pub_proc_doc` tables
        self.record_pushed_docs()

        # 4. Record the job's status as waiting for GK to confirm the push
        if self.__num_pushed:
            self.update_status(self.VERIFYING)

    def prep_push(self):
        """
        Find the corresponding export job and queue its docs for pushing to GK
        """

        # Find the export job we need to push.
        export_job = self.ExportJob(self)

        # If this is a (drastic and VERY rare) full load, clear the decks.
        # By 'rare' I mean there have only been three in the past couple of
        # decades, and the last one was in 2007.
        if self.job.parms.get("PubType") == "Full Load":
            self.cursor.execute("DELETE pub_proc_cg")
            self.conn.commit()

        # Prepare the working table, unless we're trying again for this job.
        if self.job.parms.get("RerunFailedPush") == "Yes":
            update = "UPDATE pub_proc_cg_work SET cg_job = ?"
            job_id = self.job.id
            self.cursor.execute(update, (job_id,))
            self.conn.commit()
            self.logger.info("Job %d reprocessing existing work queue", job_id)
        else:
            self.stage_push_job(export_job)

        # Some push jobs require explicit release by the operator.
        if self.job.parms.get("InteractiveMode") == "Yes":
            self.wait_for_approval()

    def stage_push_job(self, export_job):
        """
        Populate the `pub_proc_cg_work` table with documents to be pushed

        Pass:
          export_job - reference to `Control.ExportJob` object
        """

        # Use a separate connection with a long timeout.
        conn = db.connect(timeout=1000)
        cursor = conn.cursor()

        self.logger.info("Job %d clearing %s", self.job.id, self.PUSH_STAGE)
        cursor.execute(f"DELETE FROM {self.PUSH_STAGE}")
        conn.commit()
        push_id = str(self.job.id)

        # For 'Hotfix (Remove)' jobs all docs in pub_proc_doc are removals.
        # Leaving the `xml` column NULL is what flags these as removals.
        if self.job.parms["PubType"] == "Hotfix (Remove)":
            args = self.job.id, self.PUSH_STAGE
            self.logger.info("Job %d populating %s for Hotfix (Remove)", *args)
            cols = "d.id", "p.doc_version", "p.pub_proc", push_id, "t.name"
            query = db.Query("pub_proc_doc p", *cols)
            query.join("document d", "d.id = p.doc_id")
            query.join("doc_type t", "t.id = d.doc_type")
            query.where("p.pub_proc = {:d}".format(export_job.job_id))
            cols = "id", "num", "vendor_job", "cg_job", "doc_type"
            args = self.PUSH_STAGE, ", ".join(cols), query
            insert = "INSERT INTO {} ({}) {}".format(*args)
            self.cursor.execute(insert)
            self.conn.commit()
            return

        # Fetch the documents which need to be replaced on cancer.gov.
        # Compare what we sent last time with what we've got now for each doc.
        doc_type = "t.name AS doc_type"
        cols = "c.id", doc_type, "d.subdir", "d.doc_version", "c.force_push"
        query = db.Query("pub_proc_cg c", *cols)
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
        insert = "INSERT INTO {} ({}) VALUES ({})".format(*args)
        push_all = self.job.parms.get("PushAllDocs") == "Yes"
        self.logger.info("Queuing changed documents for push")
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
                with open(path, encoding="utf-8") as fp:
                    exported = fp.read()
            needs_push = push_all or row.force_push == "Y"
            if not needs_push:
                query = db.Query("pub_proc_cg", "xml")
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
                self.logger.info("Queueing changed doc CDR%d for push", row.id)
                try:
                    self.cursor.execute(insert, values)
                except:
                    self.logger.exception("First insert failed; trying again")
                    time.sleep(self.FAILURE_SLEEP)
                    self.cursor.execute(insert, values)
                self.conn.commit()

        # Queue up documents which the GateKeeper doesn't already have.
        self.logger.info("Queuing new documents for push")
        cols = "v.id", doc_type, "d.subdir", "d.doc_version"
        query = db.Query("pub_proc_doc d", *cols)
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
                with open(path, encoding="utf-8") as fp:
                    exported = fp.read()
            fields["id"] = row.id
            fields["doc_type"] = row.doc_type
            fields["xml"] = exported
            fields["num"] = row.doc_version
            values = [fields[name] for name in names]
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
        query = db.Query("pub_proc_doc d", "v.doc_type").unique()
        query.join("doc_version v", "v.id = d.doc_id", "v.num = d.doc_version")
        query.where(query.Condition("d.pub_proc", export_job.job_id))
        types = [row.doc_type for row in query.execute(self.cursor).fetchall()]
        if not types:
            return
        types = ", ".join([str(t) for t in types])
        export_id = str(export_job.job_id)
        cols = "v.id", "v.num", export_id, push_id, "t.name"
        query = db.Query("pub_proc_doc d", *cols).unique()
        query.join("doc_version v", "v.id = d.doc_id", "v.num = d.doc_version")
        query.join("all_docs a", "a.id = v.id")
        query.join("pub_proc_cg c", "c.id = v.id", "c.pub_proc = d.pub_proc")
        query.join("doc_type t", "t.id = v.doc_type")
        query.outer("pub_proc_cg_work w", "w.id = c.id")
        query.where("w.id IS NULL")
        query.where("a.active_status <> 'A'")
        query.where("v.doc_type IN ({})".format(types))
        cols = "id", "num", "vendor_job", "cg_job", "doc_type"
        args = self.PUSH_STAGE, ", ".join(cols), query
        insert = "INSERT INTO {} ({})\n{}".format(*args)
        self.logger.info("Queueing dropped documents")
        try:
            self.cursor.execute(insert)
            count = self.cursor.rowcount
            if count:
                self.logger.info("Queued %d dropped documents", count)
        except:
            self.logger.exception("First insert failed; trying again")
            time.sleep(self.FAILURE_SLEEP)
            self.cursor.execute(insert)
        self.conn.commit()

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
        encoded = base64.encodebytes(media_bytes).decode("ascii")
        template = "<Media Type='{}' Size='{:d}' Encoding='base64'>{}</Media>"
        return template.format(media_type, len(media_bytes), encoded)

    def wait_for_approval(self):
        """
        Allow the operator to review the queued push job before releasing it
        """

        self.update_status(self.WAIT, "Waiting for push job release")
        query = db.Query("pub_proc", "status")
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

    def send_docs(self):
        """
        Send the documents for the push job to the GateKeeper
        """

        # Make sure we've got something to push.
        self.__num_pushed = 0
        query = db.Query("pub_proc_cg_work", "COUNT(*) AS num_docs")
        num_docs = query.execute(self.cursor).fetchone().num_docs
        if not num_docs:
            self.update_status(self.SUCCESS, "Nothing to push")
            return

        # Make sure the GateKeeper is awake and open for business.
        gkopts = dict(host=self.job.parms.get("GKServer"))
        target = self.job.parms["GKPubTarget"]
        pub_type = self.job.parms["PubType"]
        if pub_type.startswith("Hotfix"):
            pub_type = "Hotfix"
        response = cdr2gk.initiateRequest(pub_type, target, **gkopts)
        if response.type != "OK":
            args = response.type, response.message
            raise Exception("GateKeeper: {} ({})".format(*args))
        self.logger.info("GateKeeper is awake")

        # Make any necessary tweaks to the last push ID if on a lower tier.
        last_push = self.last_push
        if last_push != response.details.lastJobId:
            if self.tier.name == "PROD":
                if self.job.parms.get("IgnoreGKJobIDMismatch") != "Yes":
                    raise Exception("Aborting on job ID mismatch")
            self.logger.warning("Last job ID override")
            last_push = response.details.lastJobId

        # Give the GateKeeper an idea of what we're about to send.
        args = self.push_desc, self.job.id, pub_type, target, last_push
        response = cdr2gk.sendDataProlog(*args, **gkopts)
        if response.type != "OK":
            args = response.type, response.message
            raise Exception("GateKeeper: {} ({})".format(*args))
        self.logger.info("Prolog sent to GateKeeper")
        self.__gk_prolog_sent = True

        # Send the GateKeeper each of the exported documents.
        send_to_cms = dict()
        media = dict()
        start = datetime.datetime.now()
        query = db.Query("pub_proc_cg_work", "id")
        query.where("xml IS NOT NULL")
        query.where(query.Condition("doc_type", self.EXCLUDED, "NOT IN"))
        ids = [row.id for row in query.execute(self.cursor).fetchall()]
        elapsed = (datetime.datetime.now() - start).total_seconds()
        args = len(ids), elapsed
        self.logger.info("Selected %d documents in %.2f seconds", *args)
        start = datetime.datetime.now()
        group_nums = GroupNums(self.job.id)
        elapsed = (datetime.datetime.now() - start).total_seconds()
        args = group_nums.getDocCount(), elapsed
        self.logger.info("Grouped %d documents in %.2f seconds", *args)
        counter = 0
        for doc_id in ids:
            query = db.Query("pub_proc_cg_work", "num", "doc_type", "xml")
            query.where(query.Condition("id", doc_id))
            row = query.execute(self.cursor).fetchone()
            doc_type = self.GK_TYPES.get(row.doc_type, row.doc_type)
            if doc_type == "Media":
                media[doc_id] = row.num
            xml = self.XMLDECL.sub("", self.DOCTYPE.sub("", row.xml))
            group_num = group_nums.getDocGroupNum(doc_id)
            counter += 1
            args = (self.job.id, counter, "Export", doc_type, doc_id,
                    row.num, group_num, xml.encode("utf-8"))
            self.logger.info("Job %d pushing CDR%d", self.job.id, doc_id)
            response = cdr2gk.sendDocument(*args, **gkopts)
            if response.type != "OK":
                args = response.type, response.message
                raise Exception("GateKeeper: {} ({})".format(*args))
            if row.doc_type in DrupalClient.TYPES:
                send_to_cms[doc_id] = row.doc_type

        # Tell the GateKeeper about the documents being removed.
        remove_from_cms = dict()
        query = db.Query("pub_proc_cg_work", "id", "num", "doc_type")
        query.where("xml IS NULL")
        query.where(query.Condition("doc_type", self.EXCLUDED, "NOT IN"))
        rows = query.execute(self.cursor).fetchall()
        for row in rows:
            if row.doc_type == "Media":
                media[row.id] = None
            args = self.job.id, row.id
            self.logger.info("Job %d removing blocked document CDR%d", *args)
            doc_type = self.GK_TYPES.get(row.doc_type, row.doc_type)
            counter += 1
            args = (self.job.id, counter, "Remove", doc_type, row.id,
                    row.num, group_nums.genNewUniqueNum())
            response = cdr2gk.sendDocument(*args, **gkopts)
            if response.type != "OK":
                args = response.type, response.message
                raise Exception("GateKeeper: {} ({})".format(*args))
            if row.doc_type in DrupalClient.TYPES:
                remove_from_cms[row.id] = row.doc_type

        # Tell the GateKeeper we're all done.
        self.__num_pushed = counter
        args = self.job.id, pub_type, counter, "complete"
        response = cdr2gk.sendJobComplete(*args, **gkopts)
        if response.type != "OK":
            args = response.type, response.message
            raise Exception("GateKeeper: {} ({})".format(*args))

        # Send the PDQ summaries to the Drupal CMS.
        source = "pub_proc_cg_work"
        server = self.job.parms.get("DrupalServer")
        base = "https://{}".format(server) if server else None
        opts = dict(
            send=send_to_cms,
            remove=remove_from_cms,
            table=source,
            logger=self.logger,
            base=base,
        )
        self.update_cms(self.session, **opts)

        # Make sure Akamai has any changes to the media files.
        if media:
            self.Media.sync(self.session, self.logger, media)

    @classmethod
    def update_cms(cls, session, **opts):
        """
        Send new/modified summaries to Drupal and remove dropped content

        As with the push to GateKeeper, failure of any of these documents
        will cause the entire job to be marked as a failure (and almost
        always leave the content pushed to Drupal in a `draft` state).
        I say "almost" because the edge case is that the `publish()`
        call might fail between batches. Nothing we can do about that
        very unlikely problem.

        Implemented as a class method so that we can invoke this
        functionality without creating a real publishing job.

        Required positional argument:
          session - object to be used in database queries, logging, etc.

        Optional keyword arguments
          send - dictionary of cdr_id -> document type for summaries to send
          remove - similar dictionary for summaries being dropped
          table - where to get the exported XML (default is pub_proc_cg)
          logger - overide session.logger for recording activity
          base - front portion of PDQ API URL
          auth - optional credentials for Drupal client (name, pw tuple)
          dumpfile - optional path for file in which to store docs

        Raise:
          `Exception` if unable to perform complete update successfully
        """

        # Record what we're about to do.
        dumpfile = opts.get("dumpfile")
        logger = opts.get("logger")
        base = opts.get("base")
        auth = opts.get("auth")
        client_opts = dict(logger=logger, base=base, auth=auth)
        client = DrupalClient(session, **client_opts)
        send = opts.get("send") or dict()
        remove = opts.get("remove") or dict()
        args = len(send), len(remove)
        client.logger.info("Sending %d documents and removing %d", *args)
        start = datetime.datetime.now()

        # Compile the XSL/T filters we'll need.
        filters= dict()
        for name in ("Cancer", "Drug"):
            title = "{} Information Summary for Drupal CMS".format(name)
            key = "DrugInformationSummary" if name == "Drug" else "Summary"
            filters[key] = Doc.load_single_filter(session, title)

        # Defer the Spanish content to a second pass.
        spanish = set()
        pushed = []
        table = opts.get("table", "pub_proc_cg")
        query = db.Query("query_term_pub", "value")
        query.where("path = '/Summary/SummaryMetaData/SummaryLanguage'")
        query.where(query.Condition("doc_id", 0))
        query = str(query)
        for doc_id in sorted(send):
            doctype = send[doc_id]
            xsl = filters[doctype]
            root = cls.fetch_exported_doc(session, doc_id, table)
            args = session, doc_id, xsl, root
            if doctype == "Summary":
                session.cursor.execute(query, (doc_id,))
                language = session.cursor.fetchone().value
                if language.lower() != "english":
                    spanish.add(doc_id)
                    continue
                values = cls.assemble_values_for_cis(*args)
            else:
                values = cls.assemble_values_for_dis(*args)
            if dumpfile:
                with open(dumpfile, "a") as fp:
                    fp.write("{}\n".format(json.dumps(values)))
            nid = client.push(values)
            pushed.append((doc_id, nid, "en"))

        # Do a second pass for the translated content.
        xsl = filters["Summary"]
        for doc_id in sorted(spanish):
            root = cls.fetch_exported_doc(session, doc_id, table)
            args = session, doc_id, xsl, root
            values = cls.assemble_values_for_cis(*args)
            if dumpfile:
                with open(dumpfile, "a") as fp:
                    fp.write("{}\n".format(json.dumps(values)))
            nid = client.push(values)
            pushed.append((doc_id, nid, "es"))

        # Drop the documents being removed.
        for doc_id in remove:
            client.remove(doc_id)

        # Only after all the other steps are done, set pushed docs to published.
        client.publish(pushed)

        # Record how long it took.
        elapsed = (datetime.datetime.now() - start).total_seconds()
        args = len(send), len(remove), elapsed
        client.logger.info("Sent %d and removed %d in %f seconds", *args)

    @classmethod
    def assemble_values_for_cis(cls, session, doc_id, xsl, root):
        """
        Get the pieces of the summary needed by the Drupal CMS

        Pass:
          session - object to be used in database queries, logging, etc.
          doc_id - CDR ID for the PDQ summary
          xsl - compiled filter for generating HTML for the summary
          root - parsed xml for the exported document

        Return:
          dictionary of values suitable for shipping to Drupal API
        """

        # Tease out pieces which need a little bit of logic.
        meta = root.find("SummaryMetaData")
        node = meta.find("SummaryURL")
        if node is None:
            raise Exception("CDR{:d} has no SummaryURL".format(doc_id))
        url = node.get("xref").replace("https://www.cancer.gov", "")
        if url.startswith("/espanol"):
            url = url[len("/espanol"):]
        short_title = None
        for node in root.findall("AltTitle"):
            if node.get("TitleType") == "Short":
                short_title = Doc.get_text(node)
        translation_of = None
        node = root.find("TranslationOf")
        if node is not None:
            translation_of = Doc.extract_id(node.get("ref"))

        # Munging of image URLs based on instructions from Blair in
        # Slack message to Volker and me 2019-02-28 11:20.
        # Possibly a temporary solution?
        # Now that QA is broken (indefinitely?) this advice no longer
        # seems very attractive. Using what's in the host configuration
        # file instead.
        #tier_extras = dict(DEV="-blue-dev", PROD="")
        #suffix = tier_extras.get(session.tier.name, "-qa")
        #replacement = f"https://www{suffix}.cancer.gov/images/cdr/live"
        #host = session.tier.hosts["CG"]
        #replacement = f"https://{host}/images/cdr/live"
        #target = "/images/cdr/live"
        target = "@@MEDIA-TIER@@"
        tier = session.tier.name.lower()
        replacement = f"-{tier}" if tier != "prod" else ""

        # Pull out the summary sections into sequence of separate dictionaries.
        transformed = xsl(root)
        cls.consolidate_citation_references(transformed)
        xpath = "body/div/article/div[@class='pdq-sections']"
        sections = []
        for node in transformed.xpath(xpath):
            h2 = node.find("h2")
            if h2 is None:
                raise Exception("CDR{:d} missing section title".format(doc_id))
            section_title = Doc.get_text(h2)
            node.remove(h2)
            section_id = node.get("id")
            if section_id.startswith("_section"):
                section_id = section_id[len("_section"):]
            body = html.tostring(node).decode("utf-8")
            body = body.replace(target, replacement)
            sections.append(dict(
                title=section_title,
                id=section_id,
                html=body
            ))

        # Pull everything together.
        langs = dict(English="en", Spanish="es")
        audience = Doc.get_text(meta.find("SummaryAudience"))
        description = Doc.get_text(meta.find("SummaryDescription"))
        if len(description) > cls.DESCRIPTION_MAX:
            session.logger.warning("Truncating description %r", description)
            description = description[:cls.DESCRIPTION_MAX]
        if len(short_title) > cls.SHORT_TITLE_MAX:
            session.logger.warning("Truncating short title %r", short_title)
            short_title = short_title[:cls.SHORT_TITLE_MAX]
        return dict(
            cdr_id=doc_id,
            url=url,
            short_title=short_title,
            translation_of=translation_of,
            sections=sections,
            title=Doc.get_text(root.find("SummaryTitle")),
            description=description,
            summary_type=Doc.get_text(meta.find("SummaryType")),
            audience=audience.replace(" prof", " Prof"),
            language=langs[Doc.get_text(meta.find("SummaryLanguage"))],
            posted_date=Doc.get_text(root.find("DateFirstPublished")),
            updated_date=Doc.get_text(root.find("DateLastModified")),
            type="pdq_cancer_information_summary",
        )

    @classmethod
    def assemble_values_for_dis(cls, session, doc_id, xsl, root):
        """
        Get the pieces of the drug info summary needed by the Drupal CMS

        Pass:
          session - object to be used in database queries, logging, etc.
          doc_id - CDR ID for the PDQ summary
          xsl - compiled filter for generating HTML for the summary
          root - parsed xml for the exported document

        Return:
          dictionary of values suitable for shipping to Drupal API
        """

        # Tease out the pronunciation fields. Strange that we have one pro-
        # nunciation key, but multiple audio pronunciation clips.
        meta = root.find("DrugInfoMetaData")
        audio_id = None
        pron = meta.find("PronunciationInfo")
        if pron is not None:
            for node in pron.findall("MediaLink"):
                if node.get("language") == "en":
                    ref = node.get("ref")
                    if ref:
                        try:
                            audio_id = int(Doc.extract_id(ref))
                            break
                        except Exception as e:
                            args = doc_id, ref
                            msg = "CDR{}: invalid audio ID {!r}".format(*args)
                            raise Exception(msg)
            pron = Doc.get_text(pron.find("TermPronunciation"))

        # Pull everything together.
        prefix = "https://www.cancer.gov"
        description = Doc.get_text(meta.find("DrugInfoDescription"))
        if len(description) > cls.DESCRIPTION_MAX:
            session.logger.warning("Truncating description %r", description)
            description = description[:cls.DESCRIPTION_MAX]
        return dict(
            cdr_id=doc_id,
            title=Doc.get_text(root.find("DrugInfoTitle")),
            description=description,
            url=meta.find("DrugInfoURL").get("xref").replace(prefix, ""),
            posted_date=Doc.get_text(root.find("DateFirstPublished")),
            updated_date=Doc.get_text(root.find("DateLastModified")),
            pron=pron,
            audio_id=audio_id,
            body=html.tostring(xsl(root)).decode("utf-8"),
            type="pdq_drug_information_summary",
        )

    @classmethod
    def consolidate_citation_references(cls, root):
        """
        Combine adjacent citation reference links

        Ranges of three or more sequential reference numbers should be
        collapsed as FIRST-LAST. A sequence of adjacent refs (ignoring
        interventing whitespace) should be surrounded by a pair of
        square brackets. Both ranges and individual refs should be
        separated by commas. The substring "cit/section" should be
        replaced in the result by "section" (stripping "cit/"). For
        example, with input of ...

          <a href="#cit/section_1.1">1</a>
          <a href="#cit/section_1.2">2</a>
          <a href="#cit/section_1.3">3</a>
          <a href="#cit/section_1.5">5</a>
          <a href="#cit/section_1.6">6</a>

        ... we should end up with ...

          [<a href="section_1.1"
           >1</a>-<a href="section_1.3"
           >3</a>,<a href="section_1.5"
           >5</a>,<a href="section_1.6"
           >6</a>]

        2019-03-13: Bryan P. decided to override Frank's request to
        have "cit/" stripped from the linking URLs.

        Pass:
          root - reference to parsed XML document for the PDQ summary

        Return:
          None (parsed tree is altered as a side effect)
        """

        # Collect all of the citation links, stripping "cit/" from the url.
        # 2019-03-13 (per BP): don't strip "cit/".
        links = []
        for link in root.iter("a"):
            href = link.get("href")
            if href is not None and href.startswith("#cit/section"):
                links.append(link)

        # Collect links which are only separated by optional whitespace.
        adjacent = []
        for link in links:

            # First time through the loop? Start a new list.
            if not adjacent:
                adjacent = [link]
                prev = link

            # Otherwise, find out if this element belongs in the list.
            else:
                if prev.getnext() is link:

                    # Whitespace in between is ignored.
                    if prev.tail is None or not prev.tail.strip():
                        adjacent.append(link)
                        prev = link
                        continue

                # Consolidate the previous list and start a new one.
                cls.rewrite_adjacent_citation_refs(adjacent)
                adjacent = [link]
                prev = link

        # Deal with the final list of adjacent elements, if any.
        if adjacent:
            cls.rewrite_adjacent_citation_refs(adjacent)

    @classmethod
    def rewrite_adjacent_citation_refs(cls, links):
        """
        Add punctuation to citation reference links and collapse ranges

        For details, see `consolidate_citation_references()` above.

        Pass:
          nodes - list of adjacent reference link elements

        Return:
          None (the parsed tree is modified in place)
        """

        # Find out where to hang the left square bracket.
        prev = links[0].getprevious()
        parent = links[0].getparent()
        if prev is not None:
            if prev.tail is not None:
                prev.tail += "["
            else:
                prev.tail = "["
        elif parent.text is not None:
            parent.text += "["
        else:
            parent.text = "["

        # Pull out the integers for the reference lines.
        refs = [int(link.text) for link in links]

        # Find ranges of unbroken integer sequences.
        i = 0
        while i < len(refs):

            # Identify the next range.
            range_len = 1
            while i + range_len < len(refs):
                if refs[i+range_len-1] + 1 != refs[i+range_len]:
                    break
                range_len += 1

            # If range is three or more integers, collapse it.
            if range_len > 2:
                if i > 0:
                    links[i-1].tail = ","
                links[i].tail = "-"
                j = 1
                while j < range_len - 1:
                    parent.remove(links[i+j])
                    j += 1
                i += range_len

            # For shorter ranges, separate each from its left neighbor.
            else:
                while range_len > 0:
                    if i > 0:
                        links[i-1].tail = ","
                    i += 1
                    range_len -= 1

        # Add closing bracket, preserving the last node's tail text.
        tail = links[-1].tail
        if tail is None:
            links[-1].tail = "]"
        else:
            links[-1].tail = f"]{tail}"

    @classmethod
    def fetch_exported_doc(cls, session, doc_id, table):
        """
        Pull the exported XML from the appropriate cancer.gov table

        Pass:
          session - used for database query
          doc_id - which document to fetch
          table - where to fetch it from

        Return:
          parsed XML document
        """

        query = db.Query(table, "xml")
        query.where(query.Condition("id", doc_id))
        xml = query.execute(session.cursor).fetchone().xml
        return etree.fromstring(xml.encode("utf-8"))

    def record_pushed_docs(self):
        """
        Update the `pub_proc_cg` and `pub_proc_doc` tables

        Use the information stored in the `pub_proc_cg_work` table.
        All of the work in this method is wrapped in a transaction
        so that everything succeeds or nothing is updated.
        """

        # Use a separate connection with a long timeout.
        conn = db.connect(timeout=1000)
        cursor = conn.cursor()

        # Handle removed documents
        cursor.execute("""\
            DELETE FROM pub_proc_cg
                  WHERE id IN (SELECT id
                                 FROM pub_proc_cg_work
                                WHERE xml IS NULL)""")
        cursor.execute("""\
            INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc, removed)
                 SELECT id, num, cg_job, 'Y'
                   FROM pub_proc_cg_work
                  WHERE xml IS NULL""")

        # Handle changed documents
        cursor.execute("""\
            UPDATE pub_proc_cg
               SET xml = w.xml,
                   pub_proc = w.cg_job,
                   force_push = 'N',
                   cg_new = 'N'
              FROM pub_proc_cg c
              JOIN pub_proc_cg_work w
                ON c.id = w.id""")
        cursor.execute("""\
            INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                 SELECT w.id, w.num, w.cg_job
                   FROM pub_proc_cg_work w
                   JOIN pub_proc_cg c
                     ON c.id = w.id""")

        # Handle new documents (order of INSERTs is important!)
        cursor.execute("""\
            INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                 SELECT w.id, w.num, w.cg_job
                   FROM pub_proc_cg_work w
        LEFT OUTER JOIN pub_proc_cg c
                     ON c.id = w.id
                  WHERE w.xml IS NOT NULL
                    AND c.id IS NULL""")
        cursor.execute("""\
            INSERT INTO pub_proc_cg (id, pub_proc, xml)
                 SELECT w.id, w.cg_job, w.xml
                   FROM pub_proc_cg_work w
        LEFT OUTER JOIN pub_proc_cg c
                     ON c.id = w.id
                  WHERE w.xml IS NOT NULL
                    AND c.id IS NULL""")

        # Seal the deal.
        conn.commit()
        conn.close()

    def normalize(self, xml):
        """
        Prepare document for comparison

        Used to determine whether we should send a fresh copy of a
        document to the GateKeeper.

        Pass:
          xml - string for serialized version of filtered CDR document

        Return:
          version of `xml` argument with irrelevant differences suppressed
        """

        xml = self.NORMALIZE_SPACE.sub(" ", xml).strip() + "\n"
        if "<Media" in xml:
            xml = xml.replace("Encoding='base64'> ", "Encoding='base64'>")
            xml = xml.replace(" </Media>", "</Media")
        return self.XMLDECL.sub("", self.DOCTYPE.sub("", xml))

    # ------------------------------------------------------------------
    # GENERAL SUPPORT METHODS START HERE.
    # ------------------------------------------------------------------

    def notify(self, message, with_link=False):
        """
        Tell the operator what we've done

        Pass:
          message - string for the body of the email message
          with_link - if True, include a link to the job's status page
        """

        default = cdr.getEmailList("Operator Publishing Notification")
        email = self.job.email or ",".join(default)
        if "@" in email:
            recips = email.replace(";", ",").split(",")
            subject = f"[{self.tier}] CDR Publishing Job {self.job.id:d}"
            if with_link:
                cgi_base = f"https://{cdr.APPC}/cgi-bin/cdr"
                link = f"{cgi_base}/PubStatus.py?id={self.job.id:d}"
                body = f"{message}\n\n{link}"
            else:
                body = message
            opts = dict(subject=subject, body=body)
            message = cdr.EmailMessage(self.SENDER, recips, **opts)
            message.send()
        else:
            args = self.job.id, message
            self.logger.warning("Job %d: no recips for notification %r", *args)
        if self.__opts.get("level", "INFO").upper() == "DEBUG":
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

    @classmethod
    def wrap_for_cms(cls, doc, cdr_id):
        """
        Create a dictionary of values to be sent to the Drupal PDQ API
        """

    # ------------------------------------------------------------------
    # PROPERTIES START HERE.
    # ------------------------------------------------------------------

    @property
    def conn(self):
        """
        Connection to the CDR database
        """

        if not hasattr(self, "_conn"):
            opts = dict(user="CdrPublishing", timeout=600)
            try:
                self._conn = db.connect(**opts)
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
    def last_push(self):
        """
        Primary key for the most recent successful push job

        Used to sychronize with the GateKeeper.
        """

        if not hasattr(self, "_last_push"):
            push = "{}%".format(self.PUSH)
            query = db.Query("pub_proc p", "MAX(p.id) AS id")
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
            query = db.Query("usr u", "u.name")
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

        query = db.Query("pub_proc", "status")
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
        if isinstance(doc, (str, bytes)):
            doc = etree.fromstring(doc)
        dtd.validate(doc)
        return dtd.error_log.filter_from_errors()


    # ------------------------------------------------------------------
    # NESTED CLASSES START HERE.
    # ------------------------------------------------------------------


    class Thread(threading.Thread):
        """
        Object for exporting documents in parallel
        """

        SCRIPT = cdr.BASEDIR + "/Publishing/export-docs.py"
        OPTS = dict(
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        def __init__(self, control):
            """
            Capture the passed arguments and invoke the base class constructor

            Pass:
              control - reference to `Control` object running this job
            """

            threading.Thread.__init__(self)
            self.control = control
            self.args = [
                cdr.PYTHON,
                self.SCRIPT,
                control.session.name,
                str(control.job.id),
                str(control.spec_id)
            ]

        def run(self):
            """
            Launch separate processes to export documents concurrently
            """

            # Keep going until the queue is exhausted.
            logger = self.control.logger
            bailing = "thread %05d bailing because job aborted"
            while True:

                # Take responsibility for a slice of the queue.
                with self.control.lock:
                    if self.control.export_failed:
                        logger.warning(bailing, self.ident)
                        return
                    remaining = len(self.control.docs) - self.control.next
                    if remaining < 1:
                        break
                    start = self.control.next
                    end = self.control.next + self.control.batchsize
                    docs = self.control.docs[start:end]
                    self.control.next = end

                # Handle failures robustly, trying more than once if needed.
                tries = 5
                pause = 5
                retrying = False
                while not self.launch(docs, remaining, retrying):
                    tries -= 1
                    if tries > 0:
                        with self.control.lock:
                            if self.control.export_failed:
                                logger.warning(bailing, self.ident)
                                return
                        retrying = True
                        args = self.ident, tries
                        pattern = "thread %05d has %d tries left"
                        logger.warning(pattern, *args)
                        time.sleep(pause)
                        pause += 5
                    else:
                        with self.control.lock:
                            self.control.export_failed = True
                        message = "thread {:05d} giving up".format(self.ident)
                        logger.error(message)
                        raise Exception(message)
            logger.info("thread %05d finished", self.ident)

        def launch(self, docs, remaining, retrying):
            args = self.ident, len(docs), remaining
            pattern = "thread %05d exporting %d of %d remaining documents"
            if retrying:
                pattern = "thread %05d retrying %d documents"
                args = self.ident, len(docs)
            self.control.logger.info(pattern, *args)
            stream = subprocess.Popen(self.args + docs, **self.OPTS)
            stdout, stderr = stream.communicate()
            if stream.returncode:
                args = self.ident, stream.returncode
                self.control.logger.error("thread %05d got return %d", *args)
            if stdout:
                args = self.ident, stdout
                self.control.logger.warning("thread %05d: %s", *args)
            if stderr:
                args = self.ident, stderr
                self.control.logger.warning("thread %05d: %s", *args)
            return stream.returncode == 0


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
            query = db.Query("pub_proc", "id", "output_dir").limit(1)
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
                message = "Export job {} has already been pushed by job {}"
                raise Exception(message.format(self.job_id, push_job))
            query = db.Query("pub_proc_doc", "COUNT(*) AS n")
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

            query = db.Query("pub_proc j", "MAX(j.id) AS job_id")
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


    class Media:
        """Common functionality for publishing audio/video/image documents."""

        BLOCKSIZE = 4096
        AKAMAI = f"{cdr.BASEDIR}/akamai"
        MEDIA = f"{AKAMAI}/media"
        LOCK = f"{MEDIA}.locked"
        OLD = f"{MEDIA}.old"
        JPEG_QUALITY = 80
        IMAGE_WIDTHS = 571, 750
        TYPES = dict(jpg="image/jpeg", gif="image/gif", mp3="audio/mpeg")
        STAMP = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        SSH = (
            f"{cdr.WORK_DRIVE}:\\cygwin\\bin\\ssh",
            f"-i {cdr.WORK_DRIVE}:/etc/akamai-pdq-{{}}",
            "-oHostKeyAlgorithms=+ssh-dss",
            "-oStrictHostKeyChecking=no",
        )
        SSH = " ".join(SSH)
        FLAGS = "nrave" # for dry run
        FLAGS = "rave"
        SSH_HOST = Tier().hosts["AKAMAI"]
        RSYNC = (
            f"{cdr.WORK_DRIVE}:\\cygwin\\bin\\rsync",
            "--delete",
            f'-{FLAGS} "{SSH}"',
            "./",
            f"sshacs@{SSH_HOST}:media",
        )
        RSYNC = " ".join(RSYNC)

        @classmethod
        def clone(cls):
            """Copy the media files to a new working directory.

            Return:
                string for the path to the copy
            """

            if not os.access(cls.LOCK, os.F_OK):
                raise Exception("Locked media not found")
            path = f"{cls.MEDIA}-{cls.STAMP}"
            shutil.copytree(cls.LOCK, path)
            command = f"{cdr.BASEDIR}/Bin/fix-permissions.cmd {path}"
            opts = dict(merge_output=True)
            process = cdr.run_command(command.replace("/", "\\"), **opts)
            if process.returncode:
                raise Exception(f"{command}: {process.stdout}")
            return path

        @classmethod
        def get_files(cls, doc):
            """Create an array of file objects for a media document.

            Pass:
                doc - `Doc` object for the media document

            Return:
                sequence of `Media.File` objects
            """

            if doc.export_filename.endswith(".mp3"):
                path = f"audio/{doc.id:d}.mp3"
                return [cls.File(path, doc.blob)]
            image_bytes = io.BytesIO(doc.blob)
            image = Image.open(image_bytes)
            if image.mode == "P":
                image = image.convert("RGB")
            path = f"images/{doc.id:d}.jpg"
            opts = dict(quality=cls.JPEG_QUALITY)
            with io.BytesIO() as fp:
                image.save(fp, "JPEG", **opts)
                compressed_original = fp.getvalue()
            files = [cls.File(path, compressed_original)]
            for width in cls.IMAGE_WIDTHS:
                path = f"images/{doc.id:d}-{width:d}.jpg"
                if width < image.width:
                    ratio = image.height / image.width
                    height = int(round(width * ratio))
                    size = width, height
                    scaled_image = image.resize(size, Image.LANCZOS)
                    with io.BytesIO() as fp:
                        scaled_image.save(fp, "JPEG", **opts)
                        image_bytes = fp.getvalue()
                else:
                    image_bytes = compressed_original
                files.append(cls.File(path, image_bytes))
            return files

        @classmethod
        def lock(cls):
            """Rename the media directory to lock out other jobs."""

            if os.access(cls.LOCK, os.F_OK):
                raise Exception("The media directory is already locked")
            if not os.access(cls.MEDIA, os.F_OK):
                raise Exception("The media directory does not exist")
            try:
                os.rename(cls.MEDIA, cls.LOCK)
            except Exception as e:
                raise Exception(f"Unable to rename {cls.MEDIA}: {e}")

        @classmethod
        def promote(cls, directory):
            """Move the new media set to the current published position.

            Steps:
              1. If media.old exists, remove it
              2. Rename media.lock to media.old
              3. Rename media-timestamp to media

            Pass:
                directory - path for the current job's working media directory
            """

            if not os.access(cls.LOCK, os.F_OK):
                raise Exception("The media lock directory does not exist")
            if os.access(cls.OLD, os.F_OK):
                try:
                    shutil.rmtree(cls.OLD)
                except Exception as e:
                    raise Exception(f"Unable to remove {cls.OLD}: {e}")
            try:
                os.rename(cls.LOCK, cls.OLD)
            except Exception as e:
                raise Exception(f"Unable to move {cls.LOCK} to {cls.OLD}: {e}")
            try:
                os.rename(directory, cls.MEDIA)
            except Exception as e:
                message = f"Unable to move {directory} to {cls.MEDIA}: {e}"
                raise Exception(message)
            command = f"{cdr.BASEDIR}/Bin/fix-permissions.cmd {cls.MEDIA}"
            opts = dict(merge_output=True)
            process = cdr.run_command(command.replace("/", "\\"), **opts)
            if process.returncode:
                raise Exception(f"{command}: {process.stdout}")

        @classmethod
        def remove(cls, doc_id, directory):
            """Remove media files for a CDR document.

            Pass:
                doc_id - integer for the document's primary key
                directory - string for the path to the working directory
            """

            for path in glob.glob(f"{directory}/images/{doc_id}.jpg"):
                os.remove(path)
            for path in glob.glob(f"{directory}/images/{doc_id}-*.jpg"):
                os.remove(path)
            for path in glob.glob(f"{directory}/audio/{doc_id}.mp3"):
                os.remove(path)

        @classmethod
        def rsync(cls, tier, logger, directory):
            """Update the media files on the Akamai server.

            Pass:
                tier - name of the tier on which we're running
                directory - location of the working set of media files
            """

            command = cls.RSYNC.format(tier.lower())
            logger.info("Running %s", command)
            old = os.getcwd()
            os.chdir(directory)
            process = cdr.run_command(command, merge_output=True)
            os.chdir(old)
            if process.returncode:
                raise Exception("rsync failure: %s", process.stdout)
            logger.info("rsync output: %s", process.stdout)

        @classmethod
        def save(cls, doc, directory):
            """Write the file(s) for the media doc to the working directory.

            Pass:
                doc - `Doc` object for the media document
                directory - string for the path to the working directory
            """

            for f in cls.get_files(doc):
                with open(f"{directory}/{f.path}", "wb") as fp:
                    fp.write(f.bytes)

        @classmethod
        def sync(cls, session, logger, media):
            """Refresh the set of media files and sync with Akamai.

            Pass:
                session - needed for Doc object creation
                logger - capture what we're doing
                media - dictionary of media document versions, index by CDR ID
                        (version is None for media being removed)
            """

            # Make sure we have something to do.
            if not media:
                logger.warning("No media to sync: skipping this step")
                return

            # Make sure no other jobs interfere with us.
            logger.info("Starting Media.sync()")
            cls.lock()

            # Create a staging area for the changes.
            directory = cls.clone()
            logger.info("Staging media in %s", directory)

            # Make the required changes to the set of media files.
            for doc_id in sorted(media):
                version = media[doc_id]
                if version:
                    try:
                        doc = Doc(session, id=doc_id, version=version)
                        cls.save(doc, directory)
                    except Exception:
                        arg = f"version {version} of {doc.cdr_id}"
                        logger.exception("Failure saving media for %s", arg)
                        raise Exception(f"Failure saving media for {arg}")
                else:
                    try:
                        cls.remove(doc_id, directory)
                    except Exception:
                        arg = f"CDR{doc_id}"
                        logger.exception("Failure removing media for %s", arg)
                        raise Exception(f"Failure removing media for {arg}")
            cls.rsync(session.tier.name, logger, directory)
            cls.promote(directory)

        @classmethod
        def unlock(cls):
            """Restore the media directory by renaming it from media.locked."""

            if os.access(cls.MEDIA, os.F_OK):
                raise Exception("The media directory is already unlocked")
            if not os.access(cls.LOCK, os.F_OK):
                raise Exception("The media lock directory does not exist")
            try:
                os.rename(cls.LOCK, cls.MEDIA)
            except Exception as e:
                raise Exception(f"Unable to rename {cls.LOCK}: {e}")


        class File:
            """Wrapper for the path and bytes of a media file."""
            def __init__(self, path, bytes):
                self.path = path
                self.bytes = bytes


def main():
    """
    Test driver
    """

    opts = dict(level="INFO")
    parser = argparse.ArgumentParser()
    parser.add_argument("job_id", metavar="job-id")
    parser.add_argument("--debug", "-d", action="store_true")
    parser.add_argument("--batchsize", "-b", type=int)
    parser.add_argument("--numprocs", "-n", type=int)
    parser.add_argument("--output", "-o")
    args = parser.parse_args()
    if args.debug:
        cdr2gk.DEBUGLEVEL = 1
        opts["level"] = "DEBUG"
    if args.numprocs:
        opts["numprocs"] = args.numprocs
    if args.batchsize:
        opts["batchsize"] = args.batchsize
    if args.output:
        opts["output-dir"] = args.output
    Control(args.job_id, **opts).publish()

if __name__ == "__main__":
    """
    Let this be loaded as a module without doing anything

    """
    main()
