"""
Manage CDR publishing jobs and provide acceess to the Drupal CMS
"""

import datetime
import json
import logging
import time
import threading
from six import iteritems
import dateutil.parser
import requests
from cdrapi.db import Query
from cdrapi.docs import Doc

# TODO: Get Acquia to fix their broken certificates.
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class Job:
    """
    Request to publish a set of CDR documents for outside consumption

    Properties:
      session - reference to object representing current login
      id - primary key into the `pub_proc` database table
      system - reference to `Doc` object for job's publishing control document
      subsystem - reference to the `Job.Subsystem` object controlling this job
      started - When the job was created (not the best name)
      completed - date/time when the job finished (if applicable)
      cursor - reference to the `Session`'s cursor object
      parms - name/value pairs overriding the control doc's parameters
      docs - list of documents requested or published for this job
      email - string for the email address to which processing notifications
              are sent
      force - If true, this is a hotfix/remove, so bypass 'Active' checks
      permissive - flag to suppress the requirement for publishable doc
                   versions
      no_output - Flag indicating that document output won't be written to disk
      output_dir - destination location for the exported documents
    """

    def __init__(self, session, **opts):
        """
        Capture job attributes.

        Required positional argument:
          session - information about the account in control

        Optional keyword arguments:
          id - unique identifier for an existing job
          system - name of the publishing system for a new job
          subsystem - string for the publishing subset for a new job
          parms - name/value pairs overriding the control doc's parameters
          docs - list of documents to be published for a new job
          email - address to which reports should be sent for new job
          no_output - if True, job won't write published docs to disk
          permissive - if True, don't require documents to be "publishable"
          force - if True, this is a hotfix-remove job; doc needn't be active
        """

        self.__session = session
        self.__opts = opts

    @property
    def completed(self):
        """
        Date/time when the job finished
        """

        if not hasattr(self, "_completed"):
            if self.id:
                query = Query("pub_proc", "completed")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    raise Exception("Job {} not found".format(self.id))
                self._completed = row.completed
            else:
                self._completed = None
            if isinstance(self._completed, datetime.datetime):
                self._completed = self._completed.replace(microsecond=0)
        return self._completed

    @property
    def cursor(self):
        """
        Use the `Session` object's database cursor
        """

        return self.__session.cursor

    @property
    def docs(self):
        """
        List of documents requested or published for this job

        For a job which is being created, this will be populated from the
        `docs` argument passed into the constructor. For a job which is
        already in the database and which has been published, the list
        will be pulled from the `pub_proc_doc` table. For a job which is
        in the database, queued but not yet run, the list will have
        the documents explicitly requested at job creation time, but will
        not yet have the documents which will be picked up by the control
        document's query logic for this job type.

        It might seem redundant to create new Doc objects for an unsaved
        job's documents, but in most cases the Doc objects passed to the
        constructor will only have document IDs, and no version information.
        We can't really get the date/time cutoff used to pick the right
        version without creating a new object.
        """

        if not hasattr(self, "_docs"):
            docs = []
            if self.id:
                query = Query("pub_proc_doc", "doc_id", "doc_version")
                query.where(query.Condition("pub_proc", self.id))
                for doc_id, version in query.execute(self.cursor).fetchall():
                    docs.append(Doc(self.session, id=doc_id, version=version))
            else:
                cutoff = self.parms.get("MaxDocUpdatedDate")
                if not cutoff or cutoff == "JobStartDateTime":
                    cutoff = self.started
                opts = dict(before=cutoff)
                for requested in self.__opts.get("docs", []):
                    opts["id"] = requested.id
                    if self.force:
                        opts["version"] = "last"
                    elif requested.version:
                        if not (self.permissive or requested.publishable):
                            args = requested.cdr_id, requested.version
                            message = "{}V{} is not publishable".format(*args)
                            raise Exception(message)
                        opts["version"] = requested.version
                    else:
                        opts["version"] = "lastp"
                    try:
                        doc = Doc(self.session, **opts)
                        if not self.force and doc.active_status != "A":
                            raise Exception("{} is blocked".format(doc.id))
                        docs.append(doc)
                    except Exception as e:
                        raise Exception("{}: {}".format(requested.cdr_id, e))
            self._docs = docs
        return self._docs

    @property
    def email(self):
        """
        String for the email address to which processing notifications go
        """

        if not hasattr(self, "_email"):
            if "email" in self.__opts:
                self._email = self.__opts["email"]
            elif self.id:
                query = Query("pub_proc", "email")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    raise Exception("Job {} not found".format(self.id))
                self._email = row.email
            else:
                self._email = None
        return self._email

    @property
    def force(self):
        """
        If true, this is a hotfix/remove, so bypass version checks
        """

        if not hasattr(self, "_force"):
            self._force = self.__opts.get("force", False)
        return self._force

    @property
    def id(self):
        """
        Primary key into the `pub_proc` database table
        """

        if not hasattr(self, "_id"):
            self._id = self.__opts.get("id")
            if self._id:
                self._id = int(self._id)
        return self._id

    @property
    def no_output(self):
        """
        Flag indicating that document output won't be written to disk
        """

        if not hasattr(self, "_no_output"):
            if "no_output" in self.__opts:
                self._no_output = self.__opts["no_output"]
            elif self.id:
                query = Query("pub_proc", "no_output")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    raise Exception("Job {} not found".format(self.id))
                self._no_output = row.no_output == "Y"
            else:
                self._no_output = False
        return self._no_output

    @property
    def output_dir(self):
        """
        Destination location for the exported documents
        """

        if not hasattr(self, "_output_dir"):
            if self.id:
                query = Query("pub_proc", "output_dir")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    raise Exception("Job {} not found".format(self.id))
                self._output_dir = row.output_dir.replace("\\", "/")
            else:
                self._output_dir = None
        return self._output_dir

    @property
    def parms(self):
        """
        Dictionary of parameters for the job

        Default values from the control document. Some may be overridden
        for this job.

        Have to strip unwanted space from the values; see
        https://sourceforge.net/p/adodbapi/bugs/27/
        """

        if not hasattr(self, "_parms"):
            if self.id:
                query = Query("pub_proc_parm", "parm_name", "parm_value")
                query.where(query.Condition("pub_proc", self.id))
                self._parms = {}
                for name, value in query.execute(self.cursor).fetchall():
                    if value is not None:
                        value = value.strip() # bug in adodbapi
                    self._parms[name] = value
            else:
                if not self.subsystem:
                    raise Exception("Missing publishing subsystem")
                requested = self.__opts.get("parms") or {}
                defined = self.subsystem.parms.copy()
                undefined = set(requested) - set(defined)
                if undefined:
                    messages = "Paramater(s) {} undefined"
                    #raise Exception(messages.format(", ".join(undefined)))
                    raise Exception(messages.format(undefined))
                defined.update(requested)
                self.session.logger.info("job parms: %r", defined)
                self._parms = defined
        return self._parms

    @property
    def permissive(self):
        """
        Flag to suppress the requirement for publishable doc versions
        """

        if not hasattr(self, "_permissive"):
            self._permissive = self.__opts.get("permissive", False)
        return self._permissive

    @property
    def session(self):
        """
        Reference to object representing the current login
        """

        return self.__session

    @property
    def started(self):
        """
        When the job was created (so a bit of a misnomer)
        """

        if not hasattr(self, "_started"):
            if self.id:
                query = Query("pub_proc", "started")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    raise Exception("Job {} not found".format(self.id))
                self._started = row.started
            else:
                self._started = datetime.datetime.now()
            if isinstance(self._started, datetime.datetime):
                self._started = self._started.replace(microsecond=0)
        return self._started

    @property
    def subsystem(self):
        """
        Reference to the `Job.Subsystem` object controlling this job
        """

        if not hasattr(self, "_subsystem"):
            self._subsystem = None
            if self.system:
                if self.system.root is None:
                    doc = self.system
                    args = doc.cdr_id, doc.version, doc.title
                    doc = "{}V{} ({})".format(*args)
                    message = "{} can't be parsed".format(doc)
                    raise Exception(message)
                name = self.__opts.get("subsystem")
                if not name and self.id:
                    query = Query("pub_proc", "pub_subset")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.cursor).fetchone()
                    if not row:
                        raise Exception("Job {} not found".format(self.id))
                    name = row.pub_subset
                if name:
                    path = "SystemSubset/SubsetName"
                    for node in self.system.root.findall(path):
                        if Doc.get_text(node) == name:
                            parent = node.getparent()
                            self._subsystem = Job.Subsystem(parent, name)
                            break
        return self._subsystem

    @property
    def system(self):
        """
        Reference to `Doc` object for job's publishing control document
        """

        if not hasattr(self, "_system"):
            opts = dict(id=None, before=self.started)
            if self.id:
                query = Query("pub_proc", "pub_system")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    raise Exception("Job {} not found".format(self.id))
                opts["id"] = row.pub_system
            else:
                name = self.__opts.get("system")
                if name:
                    query = Query("document d", "d.id")
                    query.join("doc_type t", "t.id = d.doc_type")
                    query.where("t.name = 'PublishingSystem'")
                    query.where(query.Condition("d.title", name))
                    rows = query.execute(self.cursor).fetchall()
                    if len(rows) > 1:
                        raise Exception(f"multiple {name} docs")
                    if not rows:
                        message = "Publishing system {!r} not found"
                        raise Exception(message.format(name))
                    row = rows[0]
                    opts["id"] = row.id
                    opts["title"] = name
            if opts["id"]:
                self._system = Doc(self.session, **opts)
            else:
                self._system = None
        return self._system


    # ------------------------------------------------------------------
    # PUBLIC METHODS START HERE.
    # ------------------------------------------------------------------

    def create(self):
        """
        Queue up a CDR publishing job

        Called by:
          cdr.publish()
          client XML wrapper command CdrPublish

        Return:
          None

        Side effects:
          inserted row in `pub_proc` database table, as well as inserted
          related rows in the `pub_proc_doc` and `pub_proc_parm` tables
        """

        self.session.log("Job.create()")
        # Make sure there are no roadblocks preventing the job creation.
        name = self.__opts.get("subsystem")
        if self.subsystem is None:
            if not name:
                raise Exception("Publishing subsystem not specified")
            message = "Publishing subsystem {!r} not found".format(name)
            raise Exception(message)
        action = self.subsystem.action
        if action is not None and not self.session.can_do(action):
            message = "Action {!r} not allowed for requestor".format(action)
            raise Exception(message)
        pending = self.__find_pending_job()
        if pending is not None:
            message = "Job {} of this publication type is still pending"
            raise Exception(message.format(pending))

        # Pass the work on to a helper method to make it easy to roll back.
        try:
            job_id = self.__create()
            self.session.conn.commit()
            self._id = job_id
            return job_id
        except:
            self.session.logger.exception("Job creation failed")
            self.session.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.session.cursor.fetchone().tc:
                self.session.cursor.execute("ROLLBACK TRANSACTION")
            raise


    # ------------------------------------------------------------------
    # PRIVATE METHODS START HERE.
    # ------------------------------------------------------------------

    def __create(self):
        """
        Job creation database writes, separated out for easier error recovery

        Return:
          None

        Side effects:
          inserted row in `pub_proc` database table, as well as inserted
          related rows in the `pub_proc_doc` and `pub_proc_parm` tables
        """

        # Create the `pub_proc` row for the new job.
        fields = dict(
            pub_system=self.system.id,
            pub_subset=self.subsystem.name,
            started=self.started,
            status="Ready",
            usr=self.session.user_id,
            email=str(self.email),
            output_dir="",
            no_output="Y" if self.no_output else "N"
        )
        names = sorted(fields)
        values = [fields[name] for name in names]
        args = ", ".join(names), ", ".join(["?"] * len(names))
        insert = "INSERT INTO pub_proc ({}) VALUES ({})".format(*args)
        self.cursor.execute(insert, values)
        self.cursor.execute("SELECT @@IDENTITY AS id")
        job_id = self.cursor.fetchone().id

        # Update the `output_dir` column now that we have the new job ID.
        base_dir = self.subsystem.options.get("Destination", "").rstrip("\\/")
        if base_dir:
            self._output_dir = "{}/Job{}".format(base_dir, job_id)
            update = "UPDATE pub_proc SET output_dir = ? WHERE id = ?"
            self.cursor.execute(update, (self.output_dir, job_id))
        else: # workaround for https://sourceforge.net/p/adodbapi/bugs/27/
            update = "UPDATE pub_proc SET output_dir = '' WHERE id = ?"
            self.cursor.execute(update, (job_id,))

        # Store the parameters for the job.
        names = "pub_proc", "id", "parm_name", "parm_value"
        args = ", ".join(names), ", ".join(["?"] * len(names))
        insert = "INSERT INTO pub_proc_parm ({}) VALUES ({})".format(*args)
        self.session.logger.debug("storing parms %s", self.parms)
        for i, name in enumerate(self.parms):
            self.session.logger.info("parms[%r] = %r", name, self.parms[name])
            try:
                value = str(self.parms[name])
                self.session.logger.debug("unicode value is %r", value)
            except:
                value = self.parms[name].decode("utf-8")
                self.session.logger.debug("decoded value is %r", value)
            values = job_id, i + 1, name, value
            self.cursor.execute(insert, values)

        # Store the documents requested explicitly by ID for this job.
        names = "pub_proc", "doc_id", "doc_version"
        args = ", ".join(names), ", ".join(["?"] * len(names))
        insert = "INSERT INTO pub_proc_doc ({}) VALUES ({})".format(*args)
        for doc in self.docs:
            version = doc.version or doc.last_version
            if not version:
                raise Exception("{} has no versions".format(doc.cdr_id))
            self.cursor.execute(insert, (job_id, doc.id, version))

        # Commit the new job information and return the job ID.
        self.session.conn.commit()
        self._id = job_id
        return job_id

    def __find_pending_job(self):
        """
        See if we've already got a job of this type in progress

        Used to prevent having two or more of the same publishing job
        type in flight at the same time.

        Return:
          True if there's already another job of this type in progress
        """

        query = Query("pub_proc", "id").limit(1)
        query.where(query.Condition("pub_system", self.system.id))
        query.where(query.Condition("pub_subset", self.subsystem.name))
        query.where("status NOT IN ('Success', 'Failure')")
        row = query.execute(self.cursor).fetchone()
        return row.id if row else None



    # ------------------------------------------------------------------
    # NESTED CLASSES START HERE.
    # ------------------------------------------------------------------

    class Subsystem:
        """
        Control information for a specific job type

        A subsystem has two types of setting values for controlling
        runtime processing logic. The `options` settings are values
        which are set in the control document, and cannot be overridden
        for specific jobs. The `parms` settings specified in the control
        document have default values which can be overridden by the
        request to create a publishing job. An attempt to specify a
        parm setting which is unrecognized for this subsystem will
        cause the job creation request to fail.

        Consult the publishing control document in which this
        subsystem is specified for documentation of the options
        and parms supported for the subsystem.

        Properties:
          name - string for the publishing subsystem's name
          options - dictionary of settings indexed by name
          parms - dictionary of settings which can be overridden for a job
          action - string for permission-controlled action, used to
                   determine whether the current user can create this job
          script - custom script for job
        """

        def __init__(self, node, name):
            """
            Capture the name and control document node for this subsystem

            Pass:
              node - section of the control document defining this subsystem
              name - value stored in the `pub_subset` column of the `pub_proc`
                     table
            """

            self.__node = node
            self.__name = name

        @property
        def action(self):
            """
            Used to determine whether the current user can create this job
            """

            if not hasattr(self, "_action"):
                path = "SubsetActionName"
                self._action = Doc.get_text(self.__node.find(path))
            return self._action

        @property
        def name(self):
            """
            String for the name by which the subsystem is identified
            """

            return self.__name

        @property
        def options(self):
            """
            Dictionary of fixed settings for this job type
            """

            if not hasattr(self, "_options"):
                self._options = dict(
                    AbortOnError="Yes",
                    PublishIfWarnings="No"
                )
                path = "SubsetOptions/SubsetOption"
                for child in self.__node.findall(path):
                    name = Doc.get_text(child.find("OptionName"))
                    value = Doc.get_text(child.find("OptionValue"))
                    self._options[name] = value
            return self._options

        @property
        def parms(self):
            """
            Dictionary of user-controllable settings with defaults
            """

            if not hasattr(self, "_parms"):
                self._parms = {}
                path = "SubsetParameters/SubsetParameter"
                for child in self.__node.findall(path):
                    name = Doc.get_text(child.find("ParmName"))
                    value = Doc.get_text(child.find("ParmValue"))
                    if name in self._parms:
                        raise Exception("Duplicate parm {!r}".format(name))
                    self._parms[name] = value
            return self._parms

        @property
        def script(self):
            """
            Script for custom job processing
            """

            if not hasattr(self, "_script"):
                self._script = Doc.get_text(self.__node.find("ProcessScript"))
            return self._script

        @property
        def specifications(self):
            """
            Instructions for processing this job
            """

            if not hasattr(self, "_specifications"):
                self._specifications = []
                path = "SubsetSpecifications/SubsetSpecification"
                for node in self.__node.findall(path):
                    self._specifications.append(self.Specification(node))
            return self._specifications

        @property
        def threshold(self):
            """
            Global threshold for errors, independent of any document type

            If None, the job runs to completion no matter how many errors
            are encountered.
            """

            if not hasattr(self, "_threshold"):
                abort_on_error = self.options.get("AbortOnError")
                if abort_on_error is None or abort_on_error == "No":
                    self._threshold = None
                elif abort_on_error == "Yes":
                    self._threshold = 0
                else:
                    self._threshold = int(abort_on_error)
            return self._threshold

        class Specification:
            """
            Instructions for processing the publishing job
            """

            def __init__(self, node):
                self.__node = node

            @property
            def name(self):
                if not hasattr(self, "_name"):
                    node = self.__node.find("SpecificationName")
                    self._name = Doc.get_text(node)
                return self._name

            @property
            def user_select_doctypes(self):
                if not hasattr(self, "_doctypes"):
                    self._doctypes = set()
                    path = "SubsetSelection/UserSelect/UserSelectDoctype"
                    for node in self.__node.findall(path):
                        doctype = Doc.get_text(node)
                        if doctype:
                            self._doctypes.add(doctype)
                return self._doctypes

            def select_documents(self, control):
                """
                Select documents selected by the spec's query
                """

                documents = []
                if self.query is not None:
                    sql = self.query
                    for name in control.job.parms:
                        if "?" not in sql:
                            break
                        placeholder = "?{}?".format(name)
                        value = control.job.parms[name]
                        if name == "MaxDocUpdatedDate":
                            if value == "JobStartDateTime":
                                value = str(control.job.started)[:19]
                        elif name == "NumDocs" and not value.strip():
                            value = "999999"
                        elif name == "NumDocsPerDocType" and not value.strip():
                            value = "999999"
                        sql = sql.replace(placeholder, value)
                    control.logger.info("Selecting for query\n%s\n", sql)
                    cursor = control.conn.cursor()
                    cursor.execute(sql)
                    for row in cursor.fetchall():
                        doc_id = row[0]
                        if row[0] not in control.processed:
                            version = row[1] if len(row) > 1 else "lastp"
                            documents.append(self.Document(doc_id, version))
                return documents

            @property
            def query(self):
                if not hasattr(self, "_query"):
                    node = self.__node.find("SubsetSelection/SubsetSQL")
                    self._query = Doc.get_text(node)
                return self._query

            @property
            def subdirectory(self):
                if not hasattr(self, "_subdirectory"):
                    node = self.__node.find("Subdirectory")
                    self._subdirectory = Doc.get_text(node)
                return self._subdirectory

            @property
            def filters(self):
                if not hasattr(self, "_filters"):
                    self._filters = []
                    for node in self.__node.findall("SubsetFilters"):
                        self._filters.append(self.FiltersWithParms(node))
                return self._filters

            class Document:
                def __init__(self, doc_id, doc_version):
                    self.id = doc_id
                    self.version = doc_version

            class FiltersWithParms:
                def __init__(self, node):
                    self.__node = node

                @property
                def filters(self):
                    if not hasattr(self, "_filters"):
                        self._filters = []
                        for node in self.__node.findall("SubsetFilter/*"):
                            text = Doc.get_text(node)
                            if text:
                                if node.tag == "SubsetFilterName":
                                    if not text.startswith("set:"):
                                        text = "name:{}".format(text)
                                elif node.tag != "SubsetFilterId":
                                    err = "Unexpected filter element {}"
                                    raise Exception(err.format(node.tag))
                                self._filters.append(text)
                        if not self._filters:
                            raise Exception("No filters in group")
                    return self._filters

                @property
                def parameters(self):
                    if not hasattr(self, "_parameters"):
                        self._parameters = dict()
                        for node in self.__node.findall("SubsetFilterParm"):
                            name = Doc.get_text(node.find("ParmName"))
                            value = Doc.get_text(node.find("ParmValue"), "")
                            self._parameters[name] = value
                    return self._parameters


class DrupalClient:
    """
    Client end of the PDQ RESTful APIs in the Drupal CMS

    Provides functionality comparable to that provided for communicating
    with the legacy GateKeeper APIs in the `cdr2gk` module.

    Class constants:
        BATCH_SIZE - maximum number of documents we can set to `published`
                     in a single chunk
        URI_PATH - used for routing of PDQ RESTful API requests
        TYPES - names used for the types of PDQ documents we publish
    """

    MAX_RETRIES = 5
    BATCH_SIZE = 1
    URI_PATH = "/pdq/api"
    TYPES = dict(
        Summary=("pdq_cancer_information_summary", "cis"),
        DrugInformationSummary=("pdq_drug_information_summary", "dis"),
    )

    def __init__(self, session, **opts):
        """
        Perform any necessary setup for communicating with the PDQ APIs

        Required positional argument:
          session - information about the account in control

        Optional keyword arguments:
          auth - override for basic authorization credentials pair
          base - e.g., "https://ncigovcddev.prod.acquia-sites.com"
          logger - override for logging object
          batch_size - override for number to mark `published` at once
        """

        self.__session = session
        self.__opts = opts
        self.logger.info("DrupalClient created for %s", self.base)

    @property
    def auth(self):
        """
        Basic authorization credentials pair
        """

        if not hasattr(self, "_auth"):
            self._auth = self.__opts.get("auth")
            if not self._auth:
                password = self.__session.tier.password("PDQ")
                if not password:
                    raise Exception("Unable to find PDQ CMS credentials")
                self._auth = "PDQ", password
        return self._auth

    @property
    def base(self):
        """
        Front portion of the PDQ API URL
        """

        if not hasattr(self, "_base"):
            self._base = self.__opts.get("base")
            if self._base:
                self._base = self._base.strip("/")
                if not self._base.startswith("http"):
                    raise Exception("Valid URL base required")
            else:
                host = self.__session.tier.hosts.get("DRUPAL")
                if not host:
                    raise Exception("Unable to determine CMS host name")
                self._base = "https://{}".format(host)
        return self._base

    @property
    def batch_size(self):
        """
        The number of documents to be marked `published` at once
        """

        if not hasattr(self, "_batch_size"):
            self._batch_size = self.__opts.get("batch_size")
            if self._batch_size:
                self._batch_size = int(self._batch_size)
            else:
                self.__session.cursor.execute("""\
                    SELECT val
                      FROM ctl
                     WHERE grp = 'Publishing'
                       AND name = 'Drupal-PDQ-batchsize'
                       AND inactivated IS NULL""")
                row = self.__session.cursor.fetchone()
                if row:
                    self._batch_size = int(row.val)
            if self._batch_size:
                self.logger.debug("Batch size set to %d", self._batch_size)
            else:
                self._batch_size = self.BATCH_SIZE
        return self._batch_size

    @property
    def logger(self):
        """
        Object for recording what we do
        """

        if not hasattr(self, "_logger"):
            self._logger = self.__opts.get("logger")
            if not self._logger:
                self._logger = self.__session.logger
        return self._logger

    @property
    def session(self):
        """
        Reference to object representing the current login
        """

        return self.__session

    @property
    def types(self):
        """
        Mapping from Drupal class for content to API URL tail
        """

        if not hasattr(self, "_types"):
            self._types = dict(self.TYPES.values())
        return self._types

    def push(self, values):
        """
        Send a PDQ document to the Drupal CMS

        The document will be stored in the `draft` state, and must be
        released to the `published` state at the end of the job in batch
        with the other PDQ documents published by the job (see the
        `publish()` method).

        Pass:
          values - dictionary of field values keyed by field name

        Return:
          integer for the ID of the node in which the document is stored
        """

        # Make sure we use the existing node if already in the CMS.
        self.__check_nid(values)

        # Different types use different API URLs.
        t = values["type"]
        args = self.base, self.URI_PATH, self.types[t]
        url = "{}{}/{}?_format=json".format(*args)
        self.logger.debug("URL for push(): %s", url)

        # Send the values to the CMS and check for success.
        # TODO: Get Acquia to fix their broken certificates.
        opts = dict(json=values, auth=self.auth, verify=False)
        self.logger.debug("auth=%s", self.auth)
        tries = self.MAX_RETRIES
        while tries > 0:
            response = requests.post(url, **opts)
            if response.ok:
                break
            tries -= 1
            if tries <= 0:
                self.logger.error("%r failed: %s", url, response.reason)
                raise Exception(response.reason)
            time.sleep(1)
            args = values["cdr_id"], response.reason
            self.logger.warning("%s: %s (trying again)", *args)

        # Give the caller the node ID where the document was stored.
        parsed = json.loads(response.text)
        nid = int(parsed["nid"])
        args = values["cdr_id"], self.base, nid
        self.logger.info("Pushed CDR%d to %s as node %d", *args)
        return nid

    def publish(self, documents):
        """
        Ask the CMS to set the specified documents to the `published` state.

        We have to break the batch into chunks small enough that memory
        usage will not be an issue.

        Pass:
          documents - sequence of tuples for the PDQ documents which should
                      be switched from `draft` to `published` state, each
                      tuple containing:
                          - integer for the document's unique CDR ID
                          - integer for the Drupal node for the document
                          - language code ('en' or 'es')
                      for example:
                          [
                              (257994, 231, "en"),
                              (257995, 241, "en"),
                              (448617, 226, "es"),
                              (742114, 136, "en"),
                          ]

        Return:
          possibly empty dictionary of error messages, indexed by the
          CDR ID for documents which failed
        """

        url = "{}{}?_format=json".format(self.base, self.URI_PATH)
        self.logger.info("Marking %d documents published", len(documents))
        self.logger.debug("URL for publish(): %s", url)
        offset = 0
        lookup = dict([(doc[1:], doc[0]) for doc in documents])
        errors = dict()
        while offset < len(documents):
            end = offset + self.batch_size
            chunk = [doc[1:] for doc in documents[offset:end]]
            self.logger.info("Marking %d docs as published", len(chunk))
            self.logger.debug("Docs: %r", chunk)
            offset = end
            # TODO: Get Acquia to fix their broken certificates.
            opts = dict(json=chunk, auth=self.auth, verify=False)
            tries = self.MAX_RETRIES
            while tries > 0:
                response = requests.post(url, **opts)
                if not response.ok:
                    tries -= 1
                    if tries <= 0:
                        for key in chunk:
                            cdr_id = lookup[key]
                            errors[cdr_id] = response.reason
                            args = cdr_id, response.reason
                            self.logger.error("CDR%d: %s", *args)
                    else:
                        time.sleep(1)
                        msg = "publish(): %s (trying again)"
                        self.logger.warning(msg, response.reason)
                else:
                    for nid, lang, err in json.loads(response.text)["errors"]:
                        key = nid, lang
                        cdr_id = lookup[(nid, lang)]
                        errors[cdr_id] = err
                        self.logger.error("CDR%d: %s", cdr_id, err)
                    break
        self.logger.info("%d errors found marking docs published", len(errors))
        return errors

    def remove(self, cdr_id):
        """
        Drop a PDQ document from the Drupal CMS

        Pass:
          cdr_id - integer for the PDQ document to be deleted

        Throws:
          `Exception` if delete request failed
        """

        url = "{}{}/{:d}?_format=json".format(self.base, self.URI_PATH, cdr_id)
        self.logger.debug("URL for remove(): %s", url)
        # TODO: Get Acquia to fix their broken certificates.

        tries = self.MAX_RETRIES
        while tries > 0:
            response = requests.delete(url, auth=self.auth, verify=False)
            if response.ok:
                break
            tries -= 1
            if tries <= 0:
                self.logger.error("CDR%d: %s", cdr_id, response.reason)
                self.logger.debug(response.text)
                raise Exception(response.reason)
            time.sleep(1)
            args = cdr_id, response.reason
            self.logger.warning("CDR%d: %s (trying again)", *args)

        if not response.ok:
            raise Exception(response.reason)
        self.logger.info("Removed CDR%d from %s", cdr_id, self.base)

    def list(self):
        """
        Fetch catalog of PDQ content in Drupal CMS

        Return:
          sequence of `CatalogEntry` objects
        """

        url = "{}{}/list?_format=json".format(self.base, self.URI_PATH)
        self.logger.debug("URL for list(): %s", url)
        # TODO: Get Acquia to fix their broken certificates.
        response = requests.get(url, auth=self.auth, verify=False)
        if not response.ok:
            raise Exception(response.reason)
        values = json.loads(response.text)
        catalog = [self.CatalogEntry(v) for v in values]
        args = len(catalog), self.base
        self.logger.info("Found %d PDQ documents on %s", *args)
        return catalog

    def lookup(self, cdr_id):
        """
        Fetch the Drupal ID for document's node (if it exists)

        Pass:
          cdr_id - integer for PDQ document

        Return:
          integer for unique Drupal node ID or None
        """

        url = "{}{}/{}?_format=json".format(self.base, self.URI_PATH, cdr_id)
        self.logger.debug("URL for get_nid(): %s", url)
        # TODO: Get Acquia to fix their broken certificates.
        response = requests.get(url, auth=self.auth, verify=False)
        if response.ok:
            parsed = json.loads(response.text)
            if not parsed:
                raise Exception("CDR ID {} not found".format(cdr_id))
            if len(parsed) > 1:
                raise Exception("Ambiguous CDR ID {}".format(cdr_id))
            return int(parsed[0][0])
        else:
            return None

    def __check_nid(self, values):
        """
        Insert node ID for document already in the Drupal CMS

        Node must already exist when storing the Spanish translation
        of the summary (business rule confirmed by Bryan Pizillo).
        However, this rule does not apply for publish preview
        requests, for which the CDR ID is passed as a negative integer.

        Pass:
          values - dictionary of values for the document being stored
                   (we save the node ID here if appropriate as a side
                   effect)
        """

        cdr_id = int(values["cdr_id"])
        if cdr_id > 0 and not values.get("nid"):
            translation_of = values.get("translation_of")
            if translation_of:
                nid = self.lookup(translation_of)
                if not nid:
                    raise Exception("English summary must be saved first")
            else:
                nid = self.lookup(values["cdr_id"])
            values["nid"] = nid
        if "nid" not in values:
            values["nid"] = None


    class CatalogEntry:
        """
        Information about a PDQ document in the Drupal CMS
        """

        INTEGERS = "cdr_id", "nid", "vid"
        DATETIMES = "created", "changed"

        def __init__(self, values):
            for name in values:
                value = values[name]
                if name in self.INTEGERS:
                    value = int(value)
                elif name in self.DATETIMES:
                    value = dateutil.parser.parse(value)
                setattr(self, name, value)
