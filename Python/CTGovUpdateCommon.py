#----------------------------------------------------------------------
# Import module to hold common information needed by both
# CTGovUpdateReport.py and CTGovUpdateReportBatch.py.
#----------------------------------------------------------------------
import cdr

JOB_NAME    = "Imported CTGovProtocols vs. CWDs"
REPORT_BASE = cdr.BASEDIR + "/reports/"
REPORT_FILE = "CTGov-CWD-diffs.html"
SCRIPT      = "CTGovUpdateReportBatch.py"
REPORT_URL  = "https://%s/cdrreports/%s" % (cdr.APPC, REPORT_FILE)
LF          = cdr.DEFAULT_LOGFILE
