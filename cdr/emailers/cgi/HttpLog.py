#!/usr/bin/python

#----------------------------------------------------------------------
#
# $Id: HttpLog.py,v 1.1 2004-12-28 14:04:07 bkline Exp $
#
# Module for parsing web logs.
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------
import re, sys, time

class HttpLog:
    
    def __init__(self, file = None, ignoreMalformed = False):
        self.entries = []
        if not file:
            file = sys.stdin
        for line in file:
            if ignoreMalformed:
                try:
                    self.entries.append(HttpLog.Entry(line))
                except:
                    pass
            else:
                self.entries.append(HttpLog.Entry(line))

    class Entry:
        pattern = re.compile("(\\S+) (\\S+) (\\S+) "
                             "\\[(\\d\\d)/(...)/(\\d{4}):"
                             "(\\d\\d):(\\d\\d):(\\d\\d) ([-+]\\d{4})\\] "
                             "\"([^\" ]+) ([^\" ]+)( [^\"]+)?\" (\\d+) (\\S+) "
                             "\"([^\"]*)\" \"(.*)\"")
        def __init__(self, line):
            self.client      = None
            self.clientIdent = None
            self.user        = None
            self.when        = None
            self.method      = None
            self.resource    = None
            self.protVersion = None
            self.status      = None
            self.size        = None
            self.referer     = None
            self.userAgent   = None
            match            = self.pattern.match(line)
            if not match:
                raise Exception("*** NO MATCH FOR %s ***\n" % line)
            (self.client,
             self.clientIdent,
             self.user,
             day, month, year, hour, minute, second, zone,
             self.method,
             self.resource,
             self.protVersion,
             self.status,
             self.size,
             self.referer,
             self.userAgent) = match.groups()
            self.protVersion = (self.protVersion and
                                self.protVersion.strip() or None)
            self.when = self.__parseTime(day, month, year, hour, minute,
                                         second, zone)

        def __parseTime(self, day, month, year, hour, minute, second, zone):
            pos = "JanFebMarAprMayJunJulAugSepOctNovDec".find(month)
            if pos == -1:
                raise Exception("Inavlid month %s" % month)
            try:
                return time.mktime((int(year), pos / 3 + 1, int(day),
                                    int(hour), int(minute), int(second),
                                    0, 0, -1))
            except:
                raise
                str = "%s/%s/%s:%s:%s:%s %s" % (day, month, year,
                                                minute, hour, second,
                                                zone)
                raise Exception("invalid date: [%s]" % str)

#----------------------------------------------------------------------
# Test driver.
#----------------------------------------------------------------------
if __name__ == "__main__":
    httpLog = HttpLog(ignoreMalformed = True)
    cgibin = "/PDQUpdate/cgi-bin/"
    manual = "/PDQUpdate/manual/"
    for entry in httpLog.entries:
        resource = entry.resource
        if resource.startswith(cgibin) or resource.startswith(manual):
            resource = resource[11:]
            questionMark = resource.find('?')
            if questionMark != -1:
                resource = resource[:questionMark]
            print "%s: %-20s: %s" % (time.strftime("%Y-%m-%d %H:%M:%S",
                                                   time.localtime(entry.when)),
                                     entry.client,
                                     resource)
            
    print "%d entries" % len(httpLog.entries)
