"""
Process a queued publishing job
"""

import argparse
import base64
import datetime
import glob
import os
import re
import subprocess
import threading
import time
from lxml import etree
import unicodecsv as csv
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
except:
    base64encode = base64.encodebytes
    basestring = str, bytes
    unicode = str


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
            self.update_status(self.FAILURE, unicode(e))
            if self.work_dir and os.path.isdir(self.work_dir):
                os.rename(self.work_dir, self.failure_dir)
            if self.__gk_prolog_sent:
                args = self.job.id, "Export", 0, "abort"
                opts = dict(host=self.job.parms.get("GKServer"))
                response = sendJobComplete(*args, **opts)
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
                args = len(docs), spec.name, elapsed
                msg = "{:d} {} docs selected in {:.2f} seconds".format(*args)
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
            self.batchsize = len(self.docs) / numprocs
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
        query = cdrdb.Query("pub_proc_doc d", "t.name", "COUNT(*) AS errors")
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
            threshold = self.job.subsystem.thresholds.get(doctype)
            if threshold is not None and threshold < errors[doctype]:
                args = threshold, doctype, errors[doctype]
                message = "{:d} {} errors allowed; {:d} found".format(*args)
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

        query = cdrdb.Query("media_manifest", "filename", "blob_date", "title")
        query.where(query.Condition("job_id", self.job.id))
        query.order("doc_id")
        rows = query.execute(self.cursor).fetchall()
        if rows and os.path.isdir(self.work_dir):
            values = [(row[0], str(row[1])[:10], row[2]) for row in rows]
            path = os.path.join(self.work_dir, "media_catalog.txt")
            with open(path, "wb") as fp:
                opts = dict(encoding="utf-8", delimiter=",", quotechar='"')
                writer = csv.writer(fp, **opts)
                writer.writerows(values)

    def create_push_job(self):
        """
        Queue up a job to push the documents we just exported
        """

        # First make sure there's something to push.
        query = cdrdb.Query("pub_proc_doc", "COUNT(*) AS exported")
        query.where(query.Condition("pub_proc", self.job.id))
        query.where("failure IS NULL")
        if query.execute(self.cursor).fetchone().exported:
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

        self.logger.info("Job %d clearing %s", self.job.id, self.PUSH_STAGE)
        self.cursor.execute("DELETE {}".format(self.PUSH_STAGE))
        self.conn.commit()
        push_id = str(self.job.id)

        # For 'Hotfix (Remove)' jobs all docs in pub_proc_doc are removals.
        # Leaving the `xml` column NULL is what flags these as removals.
        if self.job.parms["PubType"] == "Hotfix (Remove)":
            args = self.job.id, self.PUSH_STAGE
            self.logger.info("Job %d populating %s for Hotfix (Remove)", *args)
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

        # Fetch the documents which need to be replaced on cancer.gov.
        # Compare what we sent last time with what we've got now for each doc.
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
        insert = "INSERT INTO {} ({}) VALUES ({})".format(*args)
        push_all = self.job.parms.get("PushAllDocs") == "Yes"
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
                self.logger.info("Queueing changed doc CDR%d for push", row.id)
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
            count = self.cursor.get_rowcount()
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
        encoded = base64encode(media_bytes)
        template = "<Media Type='{}' Size='{:d}' Encoding='base64'>{}</Media>"
        return template.format(media_type, len(media_bytes), encoded)

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

    def send_docs(self):
        """
        Send the documents for the push job to the GateKeeper
        """

        # Make sure we've got something to push.
        self.__num_pushed = 0
        query = cdrdb.Query("pub_proc_cg_work", "COUNT(*) AS num_docs")
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
        start = datetime.datetime.now()
        query = cdrdb.Query("pub_proc_cg_work", "id")
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
            query = cdrdb.Query("pub_proc_cg_work", "num", "doc_type", "xml")
            query.where(query.Condition("id", doc_id))
            row = query.execute(self.cursor).fetchone()
            doc_type = self.GK_TYPES.get(row.doc_type, row.doc_type)
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
            row = self.cursor.fetchone()

        # Tell the GateKeeper about the documents being removed.
        query = cdrdb.Query("pub_proc_cg_work", "id", "num", "doc_type")
        query.where("xml IS NULL")
        query.where(query.Condition("doc_type", self.EXCLUDED, "NOT IN"))
        rows = query.execute(self.cursor).fetchall()
        for row in rows:
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

        # Tell the GateKeeper we're all done.
        self.__num_pushed = counter
        args = self.job.id, pub_type, counter, "complete"
        response = cdr2gk.sendJobComplete(*args, **gkopts)
        if response.type != "OK":
            args = response.type, response.message
            raise Exception("GateKeeper: {} ({})".format(*args))

    def record_pushed_docs(self):
        """
        Update the `pub_proc_cg` and `pub_proc_doc` tables

        Use the information stored in the `pub_proc_cg_work` table.
        All of the work in this method is wrapped in a transaction
        so that everything succeeds or nothing is updated.
        """

        # Use a separate connection with a long timeout.
        conn = cdrdb.connect(timeout=1000)
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
