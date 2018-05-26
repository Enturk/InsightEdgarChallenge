# SEC EDGAR analysis tool by Nazim Karaca

import os
from datetime import datetime
import re

# verbose UI
DEBUG = True

# FIXME need to define this globally
lastDate 
lastTime

# file holds single value from 1 to 86,400
def get_inactivity_period():
    os.chdir("../input/") # don't know if this is needed or works...
    try:
        with open(inactivity_period.txt) as f:
            if (DEBUG) : print("Opened inactivity period file")
            return [int(x) for x in f]
            # it would be better practice to close the file, but this is more legible
    except:
        print("Couldn't open ineactivity_period file.")
        sys.exit()

# TODO session starts when the IP address first requests a document from the EDGAR
class Session:
    ip
    duration = 0
    requests = 0
    firstReqDate
    firstReqTime
    lastReqDate
    lastReqTime
    live
    
    def __init__(self, ip, date, time):
        if DEBUG:
            print("Session for ip " + ip + " is being created.")
        self.ip = ip
        self.firstReqDate = date
        self.firstReqTime = time
        self.lastReqDate = date
        self.lastReqTime = time
        self.requests = 1
        self.live = True
        if DEBUG:
            print(self.ip + " is alive.")

    def check(date, time):
        if DEBUG:
            print("Checking if " + self.ip + " is alive...")
        
        # this might be simplified to scale it up
        if ((self.lastReqDate - date).total_seconds() + self.lastReqTime.total_seconds() >= timer) and self.live:
            kill_session(self, date, time)
                
        if DEBUG:
            print("The livelyness of session " + self.ip + " is " + self.live)

        return self.live

    def request(date, time):
        self.check(date,time)
        
        if self.live == False:
            # FIXME make new session for ip
            return -1
        
        else:
            self.requests += 1
            self.lastReqDate = date
            self.lastReqTime = time
            return self.requests


# list of live Sessions - should use set to scale up better
sessions = []

def get_session_with_ip(ip):
    return next((s for s in sessions if s.ip == ip), None)

def ip_in_sessions(ip):
    for s in sessions:
        if s.ip == ip:
            return s.ip
    return None

# unused?
def is_alive(ip):
    for s in sessions:
        if s.ip == ip:
            return s.live
    return False


# TODO process the EDGAR weblogs line by line
def process_log():

    os.chdir("../input/") # don't know if this is needed or works...
    csv_file_name = "log.csv"

    try:
        first_line = True
        with open(csv_file_name) as fp:
            if (DEBUG): 
                print("Opened log file")
            for line in fp:
    
                # data listed in chronological order
                lineBuffer = line.split(",")
                
                # grab header positions from first line
                if first_line:
                    headerBuffer = line.split(",")
                    i=0
                    while True:
                        try:
                            if headerBuffer[i] == "ip": ipPos = i # ip making request
                            if headerBuffer[i] == "date": datePos = i # date of the request (yyyy-mm-dd)
                            if headerBuffer[i] == "time": timePos = i # time of the request (hh:mm:ss)
                            if headerBuffer[i] == "cik": cikPos = i # SEC Central Index Key
                            if headerBuffer[i] == "accession": accessionPos = i # SEC document accession number
                            if headerBuffer[i] == "extention": extentionPos = i # Value that helps determine the document being requested
                            i++
                        except:
                            break
                        else:
                            i++
                            continue
                    first_line = False
                    continue
    
                # maybe TODO check if a request was made...
                
                # IP address uniquely identifies a single user
                ip = lineBuffer[ipPos]
                
                # FIXME convert to proper datetime format...
                date = lineBuffer[datePos]
                time = lineBuffer[timePos]
                lastDate = date
                lastTime = time

                
                # need to distinguish first request from last request AND
                if ip_in_sessions(ip): # there's some redundant verification here that should be removed for scaling
                    if DEBUG:
                        print("Session " + ip + " is in the live list and ")
                    ip.request(date, time)
                
                #create new session
                else:
                    s = Session(ip, date, time)
                    sessions.append(s)

                # duration of session in seconds
                # count of webpage requests in session
    
        fp.close()
    except:
        print("Couldn't open inactivity_period file.")
        sys.exit()
        
def kill_session(s, date, time, append)

    if s.live:
        if DEBUG:
            print("Session is still alive, murdering it.")
        s.live = False
        s.duration = (date - s.firstReqDate).totalseconds() + (time - s.firstReqTime).total_seconds()
        s.lastReqDate = date
        s.lastReqTime = time
        if DEBUG:
            print(s.ip + " just ended, and lasted " + s.duration + "seconds.")

    
    # take out of list
    try:
        sessions.remove(session)
    except ValueError:
        print("Couldn't remove session " + session.ip + " from list of sessions due to Value Error.")
        print("Trying to remove from list by position.")
        position = ip_in_sessions(s.ip)
        if position >= 0:
            try:
                sessions.remove(position)
            except:
                print("Still couldn't remove session from list. You're going to run out of memory if this keeps up...")
                return -1
        else:
            print("Session " + s.ip + " is not in list.")
    
    if DEBUG:
        print("I think the session was removed from the list. Now I'll try to write the values to output.")
    
    # TODO write a line to an output file, sessionization.txt
    # should be open in a more global scope, but need to reorganize several things...
    if append:
        x = 'a'
    else:
        x = 'w'
    
    try:
        # FIXME the path!
        o = open("output.txt", x)
        # IP address, duration of the session and number of documents accessed.
        o.write(s.ip + "," s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + s.duration + "," + s.requests)
        o.close()
    except:
        print("Couldn't open output file.")
        sys.exit()

# when user session has ended (by inactivity_period or at end of log.csv)
def cleanup(append):
    if DEBUG:
        print("Producing end of log file output.")

    for s in sessions:
        kill_session(s, lastDate, lastTime, append)

#global variables are almost always bad...
timer = get_inactivity_period()

process_log()
cleanup(DEBUG)
    
