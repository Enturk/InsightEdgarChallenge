import os
import sys
from datetime import datetime

from itertools import repeat
import gc

# verbose feedback
DEBUG = True

PATH = "/home/ubuntu/workspace/"

# yyyy-mm-dd and hh:mm:ss
date_format = "%Y-%m-%d %H:%M:%S"

def timestamp_delta(olddate, oldtime, newdate, newtime):
    old_time_stamp = datetime.strptime(olddate + " " + oldtime, '%Y-%m-%d %H:%M:%S')
    new_time_stamp = datetime.strptime(newdate + " " + newtime, '%Y-%m-%d %H:%M:%S')
    return int((new_time_stamp - old_time_stamp).total_seconds())

# file holds single value from 1 to 86,400
def get_inactivity_period():
    os.chdir(PATH + "input/") # don't know if this is needed or works...
    try:
        with open("inactivity_period.txt", "r") as f:
            if DEBUG: print("Opened inactivity period file")
            x = f.readline().rstrip()
            if DEBUG: print("  Inactivity limit is " + x)
            try:
                x = int(x)
            except:
                sys.exit("  File value can't be taken in ass an integer!")
            return x
            # it would be better practice to close the file, but this is more legible
    except IOError:
        sys.exit("Couldn't open ineactivity_period file.")

TIMER = int(get_inactivity_period())
if DEBUG: print("Timer limit is " + str(TIMER) + " seconds.")

class Session:
    
    def __init__(self, ip, date, time, iterator):
        if DEBUG: print("  Session for ip " + ip + " is being created.")
        self.ip = ip
        self.duration = 0
        self.firstReqDate = date
        self.firstReqTime = time
        self.lastReqDate = date
        self.lastReqTime = time
        self.requests = 1
        self.live = True
        self.dying = False
        SESSIONS[iterator].append(self)

        if DEBUG: print("  Session for " + self.ip + " is born.")

    def request(self, ip, date, time, iterator):
        self.requests += 1
        self.lastReqDate = date
        self.lastReqTime = time
        SESSIONS[iterator].append(self)
        
        if DEBUG: print("  Session for " + self.ip + " got a new request.")
        
# list of live Sessions - maybe use set to scale up better?
# position in this list is indicative of last time
SESSIONS = [[] for sec in repeat(None, int(TIMER))]

def get_session_with_ip(ip):
    for sec in SESSIONS:
        for s in sec:
            if s.ip == ip:
                return s
    return None

# on your marks!
count = 0
iterator = -1
date = "-1"
time = "-1"
first_line = True
second_line = True
first_round = True
time_change = False
delta = 0

if DEBUG:
    x = 'a+'
else:
    x = 'w+'

# open output file
os.chdir(PATH + "output/") 
try:
    o = open("sessionization.txt", x)
    if DEBUG: print("Opened output file,")

except:
    sys.exit("  Couldn't open output file.")

# open log file
os.chdir(PATH + "input/") 
csv_file_name = "log.csv"

try:
    with open(csv_file_name) as fp:
        if (DEBUG): 
            print("Opened log file. Here are the lines as I process them:")
            o.write("\n")
            
        for line in fp:
            
            if (DEBUG): print(line)
            
            # data listed in chronological order
            lineBuffer = line.split(",")
            
            # grab header positions from first line
            if first_line:
                if DEBUG:
                    print("  Processing header line:")
                
                i=0
                for header in lineBuffer:
                    if DEBUG: print("  Looking at header: " + header)
                    if header == "ip": 
                        ipPos = i # ip making request
                        if DEBUG: print("  IP position in header is "+ str(ipPos))
                    if header == "date": 
                        datePos = i # date of the request (yyyy-mm-dd)
                        if DEBUG: print("  Date position in header is "+ str(datePos))
                    if header == "time": 
                        timePos = i # time of the request (hh:mm:ss)
                        if DEBUG: print("  Time position in header is "+ str(timePos))
                    i += 1
                    if DEBUG: print("    Counter i = " + str(i))
                first_line = False
                continue
            
            # data
            ip = lineBuffer[ipPos]
            if (time != lineBuffer[timePos]) or (date != lineBuffer[datePos]):
                time_change = True
                if second_line:
                    iterator += 1;
                else:
                    delta = timestamp_delta(date, time, lineBuffer[datePos], lineBuffer[timePos])
                    iterator += delta%TIMER
                if iterator >= TIMER:
                    iterator = 0
                    if first_round: first_round = False
            date = lineBuffer[datePos]
            time = lineBuffer[timePos]
            
            # after second line, monitor for time changes
            if (time_change and not second_line):
                #start the session genocide
                for s in SESSIONS[iterator]:
                    SESSIONS[iterator].remove(s)
                    if timestamp_delta(s.lastReqDate, s.lastReqTime, date, time) >= TIMER:
                        s.live = False
                        s.duration = timestamp_delta(s.firstReqDate, s.firstReqTime, s.lastReqDate, s.lastReqTime)
                        o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")
                        s = None
                #unknown if this helps
                gc.collect()
                time_change = False

            # case in which lotsa time passes between requests
            if delta>TIMER:
                for sec in SESSIONS:
                    for s in sec:
                        s.duration = timestamp_delta(s.firstReqDate, s.firstReqTime, s.lastReqDate, s.lastReqTime)
                        if DEBUG: print("  Session " + s.ip + " lasted " + str(s.duration) + " seconds.")
                        o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")
                        s = None
                gc.collect()
            
            # reset trigger
            delta = 0

            if second_line: second_line = False
            
            #check if session exists
            existing = get_session_with_ip(ip)
            
            # WARNING: assumes that each line makes a request
            #if none, new session
            if existing == None:
                if DEBUG: print("  New session for ip " + ip)
                s = Session(ip, date, time, iterator)
            
            #otherwise, update session
            else:
                if DEBUG: print("  Existing session for ip " + ip + " gets new request at " + str( iterator))
                existing.request(ip, date, time, iterator)
    
    # at end of input file:
    if DEBUG: print("End of input process:")
    for sec in SESSIONS:
        for s in sec:
            s.duration = timestamp_delta(s.firstReqDate, s.firstReqTime, s.lastReqDate, s.lastReqTime)
            if DEBUG: print("  Session " + s.ip + " lasted " + str(s.duration) + " seconds.")
            o.write(s.ip + "," + s.firstReqDate + " " + s.firstReqTime + "," +  s.lastReqDate + " " + s.lastReqTime + "," + str(s.duration) + "," + str(s.requests) + "\n")

except IOError:
    sys.exit("Couldn't open log file.")