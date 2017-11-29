"""
Manage CDR publishing jobs
"""

import datetime
from six import iteritems
from cdrapi.db import Query
from cdrapi.docs import Doc

class Job:
    """
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
          force - if True, this is a hotfix-remove job; any doc version will do
        """

        self.__session = session
        self.__opts = opts

    @property
    def completed(self):
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
        return self.__session.cursor

    @property
    def docs(self):
        if not hasattr(self, "_docs"):
            docs = []
            if self.id:
                query = Query("pub_proc_doc", "doc_id", "doc_version")
                query.where(query.Condition("pub_proc", self.id))
                for doc_id, version in query.execute(self.cursor).fetchall():
                    docs.append(Doc(self.session, doc_id, version))
            else:
                cutoff = self.parms.get("MaxDocUpdatedDate")
                if not cutoff or cutoff == "LastStartDateTime":
                    cutoff = self.started
                opts = dict(before=cutoff)
                for requested in self.__opts.get("docs", []):
                    opts["id"] = requested.id
                    if self.force:
                        opts["version"] = "last"
                    elif requested.version:
                        if requested.active_status != "A":
                            message = "{} is blocked".format(requested.cdr_id)
                            raise Exception(message)
                        if not (self.permissive or requested.publishable):
                            args = requested.cdr_id, requested.version
                            message = "{}V{} is not publishable".format(*args)
                            raise Exception(message)
                        opts["version"] = requested.version
                    else:
                        opts["version"] = "lastp"
                    try:
                        docs.append(Doc(self.session, **opts))
                    except Exception as e:
                        raise Exception("{}: {}".format(requested.cdr_id, e))
            self._docs = docs
        return self._docs

    @property
    def email(self):
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
        if not hasattr(self, "_force"):
            self._force = self.__opts.get("force", False)
        return self._force

    @property
    def id(self):
        if not hasattr(self, "_id"):
            self._id = self.__opts.get("id")
        return self._id

    @property
    def no_output(self):
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
        if not hasattr(self, "_output_dir"):
            if self.id:
                query = Query("pub_proc", "output_dir")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    raise Exception("Job {} not found".format(self.id))
                self._output_dir = row.output_dir
            else:
                self._output_dir = None
        return self._output_dir

    @property
    def parms(self):
        if not hasattr(self, "_parms"):
            if self.id:
                query = Query("pub_proc_parm", "parm_name", "parm_value")
                query.where(query.Condition("id", self.id))
                self._parms = {}
                for name, value in query.execute(self.cursor).fetchall():
                    self._parms[name] = value
            else:
                if not self.subsystem:
                    raise Exception("Missing publishing subsystem")
                requested = self.__opts.get("parms") or {}
                defined = self.subsystem.parms.copy()
                undefined = set(requested) - set(defined)
                if undefined:
                    messages = "Paramater(s) {} undefined"
                    raise Exception(message.format(", ".join(undefined)))
                defined.update(requested)
                self._parms = defined
        return self._parms

    @property
    def permissive(self):
        if not hasattr(self, "_permissive"):
            self._permissive = self.__opts.get("permissive", False)
        return self._permissive

    @property
    def session(self):
        return self.__session

    @property
    def started(self):
        if not hasattr(self, "_started"):
            if self.id:
                query = Query("pub_proc", "started")
                query.where(query.Condition("id", self.id))
                row - query.execute(self.cursor).fetchone()
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
        return self._subsystem

    @property
    def system(self):
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
                    row = query.execute(self.cursor).fetchone()
                    if not row:
                        message = "Publishing system {!r} not found"
                        raise Exception(message.format(self.system))
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
        """

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
        if self.subsystem.options.get("Destination") is None:
            message = "Destination option not specified in {!r} subsystem"
            raise Exception(message.format(self.subsystem.name))

        # Pass the work on to a helper method to make it easy to roll back.
        try:
            job_id = self.__create()
            self.session.conn.commit()
            self._job_id = job_id
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

        # Create the `pub_proc` row for the new job.
        fields = dict(
            pub_system=self.system.id,
            pub_subset=self.subsystem.name,
            started=self.started,
            status="Ready",
            usr=self.session.user_id,
            email=self.email,
            output_dir="",
            messages="just testing",
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
        # Will be blanked out later in the database for "no output" jobs.
        base_dir = self.subsystem.options["Destination"].rstrip("\\/")
        self._output_dir = "{}/Job{}".format(base_dir, job_id)
        update = "UPDATE pub_proc SET output_dir = ? WHERE id = ?"
        self.cursor.execute(update, (self.output_dir, job_id))

        # Store the parameters for the job.
        names = "pub_proc", "id", "parm_name", "parm_value"
        args = ", ".join(names), ", ".join(["?"] * len(names))
        insert = "INSERT INTO pub_proc_parm ({}) VALUES ({})".format(*args)
        for i, name in enumerate(self.parms):
            values = job_id, i + 1, name, self.parms[name]
            self.cursor.execute(insert, values)

        # Store the documents requested explicitly by ID for this job.
        names = "pub_proc", "doc_id", "doc_version"
        args = ", ".join(names), ", ".join(["?"] * len(names))
        insert = "INSERT INTO pub_proc_doc ({}) VALUES ({})".format(*args)
        for doc in self.docs:
            self.cursor.execute(insert, (job_id, doc.id, doc.version))

        # Commit the new job information and return the job ID.
        self.session.conn.commit()
        self._id = job_id
        return job_id

    def __find_pending_job(self):
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
        def __init__(self, node, name):
            self.__node = node
            self.__name = name

        @property
        def name(self):
            return self.__name

        @property
        def options(self):
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
        def action(self):
            if not hasattr(self, "_action"):
                path = "SubsetActionName"
                self._action = Doc.get_text(self.__node.find(path))
            return self._action
