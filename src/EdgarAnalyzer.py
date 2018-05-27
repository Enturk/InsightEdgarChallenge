# SEC EDGAR analysis tool by Nazim Karaca

import os
from datetime import datetime
import sys
import re

# verbose UI
DEBUG = True

PATH = "/home/ubuntu/workspace/"

# yyyy-mm-dd and hh:mm:ss
date_format = "%Y-%m-%d %H:%M:%S"

# file holds single value from 1 to 86,400
def get_inactivity_period():
    os.chdir(PATH + "input/") # don't know if this is needed or works...
    try:
        with open("inactivity_period.txt") as f:
            if (DEBUG) : print("Opened inactivity period file")
            return [int(x) for x in f]
            # it would be better practice to close the file, but this is more legible
    except IOError:
        sys.exit("Couldn't open ineactivity_period file.")

TIMER = get_inactivity_period()

#  session starts when the IP address first requests a document from the EDGAR
class Session:
    
    def __init__(self, ip, date, time):
        if DEBUG:
            print("Session for ip " + ip + " is being created.")
        self.ip = ip
        self.duration = 0
        self.firstReqDate = date
        self.firstReqTime = time
        self.lastReqDate = date
        self.lastReqTime = time
        self.requests = 1
        self.live = True
        if DEBUG:
            print(self.ip + " is alive.")

    def check(self, d, t):
        if DEBUG:
            print("Checking if " + self.ip + " is alive...")
        
        # this might be simplified to scale it up
        priorReq = datetime.strptime(self.lastReqDate + " " + self.lastReqTime, '%Y-%m-%d %H:%M:%S')
        thisReq = datetime.strptime(d + " " + t, '%Y-%m-%d %H:%M:%S')
        
        if ((thisReq - priorReq).total_seconds >= TIMER) and self.live:
            kill_session(self, d, t)
                
        if DEBUG:
            print("The livelyness of session " + self.ip + " is " + str(self.live))

        return self.live

    def request(self, date, time):
        if DEBUG:
            print("Adding a request to session " + self.ip)
        
        self.check(date,time)
        
        if self.live == False:
            # old session timed out
            if DEBUG:
                print("Session "+ self.ip + "has gone stale and has been killed. Time to start a new session.")
            # make new session for ip
            newsesh = Session(self.ip, date, time)
            sessions.append(newsesh)
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

def kill_session(s, date, time):
    if s.live:
        if DEBUG:
            print("Session is still alive, murdering it.")
        s.live = False
        
        lastReq = datetime.strptime(s.lastReqDate + " " + s.lastReqTime, '%Y-%m-%d %H:%M:%S')
        firstReq = datetime.strptime(s.firstReqDate + " " + s.firstReqTime, '%Y-%m-%d %H:%M:%S')

        s.duration = (lastReq - firstReq).total_seconds()
        if DEBUG:
            print(s.ip + " just ended, and lasted " + str(s.duration) + "seconds.")

    
    # take out of list
    try:
        sessions.remove(s)
    except ValueError:
        print("Couldn't remove session " + s.ip + " from list of sessions due to Value Error.")
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
        print("I think the session was removed from the list. Now to write the values to output...")

# TODO process the EDGAR weblogs line by line
def process_log():

    # open output file
    if DEBUG:
        x = 'a'
    else:
        x = 'w'

    os.chdir(PATH + "output/") # don't know if this is needed or works...
    try:
        # FIXME the path!
        o = open("sessionization.txt", x)
        if DEBUG:
            print("Opened output file,")
    except:
        sys.exit("Couldn't open output file.")

    # open log file
    os.chdir(PATH + "input/") # don't know if this is needed or works...
    csv_file_name = "log.csv"

    try:
        first_line = True
        with open(csv_file_name) as fp:
            if (DEBUG): 
                print("Opened log file. Here are the lines as I process them:")
            
            for line in fp:
                
                if (DEBUG):
                    print(line)
                
                # data listed in chronological order
                lineBuffer = line.split(",")
                
                # grab header positions from first line
                if first_line:
                    if DEBUG:
                        print("Processing header line:")
                    
                    i=0
                    while True:
                        try:
                            if lineBuffer[i] == "ip": ipPos = i # ip making request
                            if DEBUG:
                                print("IP position in header is "+ ipPos)
                            if lineBuffer[i] == "date": datePos = i # date of the request (yyyy-mm-dd)
                            if lineBuffer[i] == "time": timePos = i # time of the request (hh:mm:ss)
                            # if lineBuffer[i] == "cik": cikPos = i # SEC Central Index Key
                            # if lineBuffer[i] == "accession": accessionPos = i # SEC document accession number
                            # if lineBuffer[i] == "extention": extentionPos = i # Value that helps determine the document being requested
                            i+=1
                        except:
                            print("Exception in sorting out header buffer. Using testing header positions.")
                            ipPos = 0
                            datePos = 1
                            timePos = 2
                            break
                        else:
                            i+=1
                            continue
                    first_line = False
                    
                    continue
                # maybe TODO check if a request was made...
                
                # IP address uniquely identifies a single user
                ip = lineBuffer[ipPos]
                date = lineBuffer[datePos]
                time = lineBuffer[timePos]
                
                # need to distinguish first request from last request AND
                if ip_in_sessions(ip): # there's some redundant verification here that should be removed for scaling
                    if DEBUG:
                        print("Session " + ip + " is in the live list and ")
                    get_session_with_ip(ip).request(date, time)
                
                #create new session
                else:
                    s = Session(ip, date, time)
                    sessions.append(s)

                # duration of session in seconds
                # count of webpage requests in session
                
                # IP address, duration of the session and number of documents accessed.
                o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests))

        fp.close()

    except IOError:
        sys.exit("Couldn't open log file.")

# when user session has ended (by inactivity_period or at end of log.csv)
    if DEBUG:
        print("Producing end-of-log-file output.")

    for s in sessions:
        kill_session(s, date, time)
        o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests))


    o.close()

process_log()
