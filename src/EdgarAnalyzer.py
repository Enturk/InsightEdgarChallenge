# SEC EDGAR analysis tool by Nazim Karaca

import os
from datetime import datetime
import sys
import re
# import csv # examine as alternative: https://www.e-education.psu.edu/geog485/node/141

# verbose UI
DEBUG = False

PATH = "/home/ubuntu/workspace/"

# yyyy-mm-dd and hh:mm:ss
date_format = "%Y-%m-%d %H:%M:%S"

# file holds single value from 1 to 86,400
def get_inactivity_period():
    os.chdir(PATH + "input/") # don't know if this is needed or works...
    try:
        with open("inactivity_period.txt", "r") as f:
            if DEBUG: print("Opened inactivity period file")
            x = f.readline().rstrip()
            if DEBUG: print("  Timer is " + x)
            try:
                x = float(x)
            except:
                print("  Timer value can't float!")
            return x
            # it would be better practice to close the file, but this is more legible
    except IOError:
        sys.exit("Couldn't open ineactivity_period file.")

TIMER = get_inactivity_period()
if DEBUG:
    print("Timer limit is " + str(TIMER) + " seconds.")

#  session starts when the IP address first requests a document from the EDGAR
class Session:
    
    def __init__(self, ip, date, time):
        if DEBUG:
            print("  Session for ip " + ip + " is being created.")
        self.ip = ip
        self.duration = 0
        self.firstReqDate = date
        self.firstReqTime = time
        self.lastReqDate = date
        self.lastReqTime = time
        self.requests = 1
        self.live = True
        self.dying = False
        if DEBUG:
            print("  " + self.ip + " is born.")

    def check(self, d, t):
        if DEBUG:
            print("  Session " + self.ip + " has been alive for ...")
        
        # this might be simplified to scale it up
        priorReq = datetime.strptime(self.lastReqDate + " " + self.lastReqTime, '%Y-%m-%d %H:%M:%S')
        thisReq = datetime.strptime(d + " " + t, '%Y-%m-%d %H:%M:%S')
        delta = (thisReq - priorReq).total_seconds()
        if DEBUG:
            print("    ... " + str(delta) + " seconds.")
        
        if delta >= TIMER and self.live:
            kill_session(self, d, t)
            self.dying = True
            
        if DEBUG:
            print("     The livelyness of session " + self.ip + " is " + str(self.live) + " and its dyingness is " + str(self.dying))

        return self.live

    def request(self, date, time):
        if DEBUG:
            print("  Time: " + time + " adding a request to session " + self.ip)
        
        self.check(date,time)
        
        if self.live == False:
            # old session timed out
            if DEBUG:
                print("    Session "+ self.ip + "has gone stale and has been killed. Time to start a new session.")
            # make new session for ip
            newsesh = Session(self.ip, date, time)
            sessions.append(newsesh)
            return -1
        
        else:
            self.requests += 1
            self.lastReqDate = date
            self.lastReqTime = time
            if DEBUG:
                print("    Session " + self.ip + " has made " + str(self.requests) + " requests.")
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
            print("  Session is still alive, murdering it.")
        s.live = False
        
        lastReq = datetime.strptime(s.lastReqDate + " " + s.lastReqTime, '%Y-%m-%d %H:%M:%S')
        firstReq = datetime.strptime(s.firstReqDate + " " + s.firstReqTime, '%Y-%m-%d %H:%M:%S')

        s.duration = (lastReq - firstReq).total_seconds()
        if DEBUG:
            print("    "+ s.ip + " just ended, and lasted " + str(s.duration) + " seconds.")

    
    # take out of list
    try:
        sessions.remove(s)
    except ValueError:
        print("    Couldn't remove session " + s.ip + " from list of sessions due to Value Error.")
        print("    Trying to remove from list by position.")
        position = ip_in_sessions(s.ip)
        if position >= 0:
            try:
                sessions.remove(position)
            except:
                print("      Still couldn't remove session from list. You're going to run out of memory if this keeps up...")
                return -1
        else:
            print("    Session " + s.ip + " is not in list.")
    
    if DEBUG:
        print("    I think the session was removed from the list. Now to write the values to output...")

# TODO process the EDGAR weblogs line by line
def process_log():

    # open output file
    if DEBUG:
        x = 'a+'
    else:
        x = 'w+'

    count = 0

    os.chdir(PATH + "output/") # don't know if this is needed or works...
    try:
        # FIXME the path!
        o = open("sessionization.txt", x)
        if DEBUG:
            print("Opened output file,")
            # o.write("Run of: " + str(datetime.datetime.now()) + "\n")
    except:
        sys.exit("  Couldn't open output file.")

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
                        print("  Processing header line:")
                    
                    i=0
                    for header in lineBuffer:
                        if DEBUG:
                            print("  Looking at header: " + header)
                        if header == "ip": 
                            ipPos = i # ip making request
                            if DEBUG:
                                print("  IP position in header is "+ str(ipPos))
                        if header == "date": 
                            datePos = i # date of the request (yyyy-mm-dd)
                            if DEBUG:
                                print("  Date position in header is "+ str(datePos))
                        if header == "time": 
                            timePos = i # time of the request (hh:mm:ss)
                            if DEBUG:
                                print("  Time position in header is "+ str(timePos))
                        i += 1
                        if DEBUG: print("    Counter i = " + str(i))
                    first_line = False
                    continue
                
                # IP address uniquely identifies a single user
                ip = lineBuffer[ipPos]
                date = lineBuffer[datePos]
                time = lineBuffer[timePos]
                
                # need to distinguish first request from last request 
                if ip_in_sessions(ip): # there's some redundant verification here that should be removed for scaling
                    if DEBUG:
                        print("  Session " + ip + " is in the live list.")
                    s = get_session_with_ip(ip)
                    s.check(date, time)
                    
                    # IP address, duration of the session and number of documents accessed.
                    if s.dying:
                        if DEBUG:
                            print("    Session is stale.")
                        o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")
                        s.dying = False
                        
                        # the king is dead, long live the king
                        newsesh = Session(ip, date, time)
                        sessions.append(newsesh)
                        
                    else:
                        if DEBUG:
                            print("    Session is still alive.")
                        s.request(date, time)
                        
                #create new session
                else:
                    s = Session(ip, date, time)
                    sessions.append(s)
            
            # when scaling, use counter to learn when dealing longer files...
            count = count + 1
            if (count % 10000 == 0):
                print("Processed " + str(count) + " lines of input.")
        fp.close()

    except IOError:
        sys.exit("Couldn't open log file.")

# when user session has ended (by inactivity_period or at end of log.csv)
    if DEBUG:
        print("Producing end-of-log-file output.")

    for s in sessions:
        o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")
        kill_session(s, date, time)
        s.dying = False
        if DEBUG:
            print("  Final death for session " + s.ip)
    if DEBUG:
        # extra lines for legibility
        o.write("\n")
    o.close()

process_log()
print("Done. Please check out the output file.")
